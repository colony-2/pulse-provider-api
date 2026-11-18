package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

var fixtureBinary string

func TestMain(m *testing.M) {
	// Adopt orphaned fixture workloads so the harness can reap them.
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36, 1, 0, 0, 0, 0)
	if errno != 0 {
		panic(errno)
	}
	dir, e := os.MkdirTemp("", "plugin-fixture-build-")
	if e != nil {
		panic(e)
	}
	fixtureBinary = filepath.Join(dir, "provider")
	args := []string{"build", "-o", fixtureBinary, "./internal/testplugin"}
	if raceEnabled {
		args = []string{"build", "-race", "-o", fixtureBinary, "./internal/testplugin"}
	}
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, e := cmd.CombinedOutput(); e != nil {
		panic(string(out) + e.Error())
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
func options(t *testing.T, mode string) StartOptions {
	t.Helper()
	dir := t.TempDir()
	return StartOptions{Executable: fixtureBinary, Env: map[string]string{"EVENT_DIR": dir, "MODE": mode}, Init: Initialization{Name: "fixture", BaseDir: dir, ConfigYAML: []byte("mode: fixture\n"), ExecutorEnv: map[string]string{"EXECUTOR_ONLY": "executor-default"}}, StartupTimeout: 3 * time.Second, CallTimeout: time.Second, ShutdownTimeout: 500 * time.Millisecond, ProbeInterval: 30 * time.Millisecond, ProbeTimeout: 100 * time.Millisecond}
}
func start(t *testing.T, o StartOptions) *Process {
	t.Helper()
	p, e := Start(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func launches(ids ...string) []api.Launch {
	var out []api.Launch
	for _, id := range ids {
		t := time.Date(9999, 1, 1, 0, 0, 0, 123, time.UTC)
		out = append(out, api.Launch{LaunchID: id, Allocation: api.Allocation{Base: "ubuntu[ab]*/arm*", CPUMillis: api.MaxQuantity, Capabilities: []string{"gpu", "network"}}, Job: api.JobRef{JobDB: "https://db/tenant", InstanceID: "db", TenantID: "tenant", JobID: "job"}, Lease: api.Lease("\xffexact-lease"), TimeoutSeconds: 2, StartBefore: &t})
	}
	return out
}
func events(dir string) string { b, _ := os.ReadFile(filepath.Join(dir, "events")); return string(b) }
func count(dir, s string) int {
	n := 0
	for _, line := range strings.Split(events(dir), "\n") {
		if line == s {
			n++
		}
	}
	return n
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func waitEvent(t *testing.T, dir, s string) {
	t.Helper()
	eventually(t, func() bool { return count(dir, s) > 0 })
}
func readPID(t *testing.T, dir, name string) int {
	t.Helper()
	var pid int
	eventually(t, func() bool {
		b, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			return false
		}
		pid, e = strconv.Atoi(string(b))
		return e == nil && pid > 0
	})
	return pid
}

func assertReaped(t *testing.T, pid int) {
	t.Helper()
	eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
}

func TestProcessReadyInitializationAndClose(t *testing.T) {
	o := options(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	p, e := Start(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	defer p.Close()
	c, e := p.GetCapabilities(context.Background())
	if e != nil || len(c.Bases) != 2 || c.Bases[0].Name != "busy" || c.BasesComplete {
		t.Fatalf("capabilities %#v %v", c, e)
	}
	r, e := p.List(context.Background(), api.ListRequest{LaunchID: "filter", PageToken: "\xff\xfe"})
	if e != nil || r.Items == nil || r.NextPageToken != "\xff\xfe" {
		t.Fatalf("list %#v %v", r, e)
	}
	s, e := p.Submit(context.Background(), launches("one"))
	if e != nil || s[0].Status != api.Accepted {
		t.Fatalf("submit %#v %v", s, e)
	}
	h := p.Health()
	if h.State != "ready" || h.Protocol != 2 || h.Generation != 1 || h.Implementation != "fixture" {
		t.Fatal(h)
	}
	dir := p.current.dir
	st, e := os.Stat(dir)
	if e != nil || st.Mode().Perm() != 0700 {
		t.Fatalf("directory %v %v", st, e)
	}
	pid := readPID(t, o.Init.BaseDir, "pid")
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	assertReaped(t, pid)
	if _, e = os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal("directory remains")
	}
	if count(o.Init.BaseDir, "cleanup") != 1 {
		t.Fatal(events(o.Init.BaseDir))
	}
	if runtime.GOOS != "linux" {
		t.Fatal("this fixture claims Linux only")
	}
}
func TestMixedOutcomesAndConcurrentObservations(t *testing.T) {
	o := options(t, "mixed")
	p := start(t, o)
	r, d, e := p.SubmitDetailed(context.Background(), launches("a", "b", "c", "d"))
	if e == nil || r[0].Status != api.Accepted || r[1].Status != api.NoCapacity || r[2].Status != api.Unknown || r[3].Status != api.Unknown || len(d.Outcomes) != 5 || d.MethodError.Text != "private-method-error" {
		t.Fatalf("%#v %#v %v", r, d, e)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if count(o.Init.BaseDir, "submit "+id) != 1 {
			t.Fatal(events(o.Init.BaseDir))
		}
	}
	o2 := options(t, "correlated")
	p2 := start(t, o2)
	var wg sync.WaitGroup
	for _, id := range []string{"parallel-a", "parallel-b", "parallel-c", "parallel-d"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r, d, e := p2.SubmitDetailed(context.Background(), launches(id))
			if e == nil || r[0].Status != api.Unknown || d.Outcomes[0].Reason != id+"-original" || d.MethodError.Text != id+"-error" {
				t.Errorf("correlation %s %#v %#v %v", id, r, d, e)
			}
		}(id)
	}
	wg.Wait()
	for _, id := range []string{"parallel-a", "parallel-b", "parallel-c", "parallel-d"} {
		if count(o2.Init.BaseDir, "submit "+id) != 1 {
			t.Fatal(events(o2.Init.BaseDir))
		}
	}
	t.Log("handler counts: mixed launches each 1; four overlapping detailed invocations each 1")
}
func TestDiscoveryAndListMethodErrors(t *testing.T) {
	o := options(t, "dataerror")
	p := start(t, o)
	c, e := p.GetCapabilities(context.Background())
	if e == nil || len(c.Bases) != 2 {
		t.Fatalf("%#v %v", c, e)
	}
	r, e := p.List(context.Background(), api.ListRequest{LaunchID: "filter", PageToken: "\xff\xfe"})
	if e == nil || r.NextPageToken != "\xff\xfe" {
		t.Fatalf("%#v %v", r, e)
	}
}
func TestPredispatchBoundsAndSaturation(t *testing.T) {
	o := options(t, "cancel")
	o.Concurrency = 1
	o.MaxMessageBytes = 4096
	o.Env["SERVER_LIMIT"] = "2048"
	p := start(t, o)
	if p.current.limit != 2048 {
		t.Fatal(p.current.limit)
	}
	bad := launches("invalid")
	bad[0].CPUMillis = -1
	r, e := p.Submit(context.Background(), bad)
	if e == nil || r[0].Status != api.Rejected {
		t.Fatal(r, e)
	}
	large := launches("large")
	large[0].Lease = api.Lease(strings.Repeat("x", 4096))
	r, e = p.Submit(context.Background(), large)
	if !errors.Is(e, ErrTooLarge) || r[0].Status == api.Unknown {
		t.Fatal(r, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = p.Submit(ctx, launches("active")) }()
	waitEvent(t, o.Init.BaseDir, "submit active")
	r, e = p.Submit(context.Background(), launches("saturated"))
	if !errors.Is(e, ErrSaturated) || r[0].Status == api.Unknown {
		t.Fatal(r, e)
	}
	cancel()
	<-done
	if count(o.Init.BaseDir, "submit invalid") != 0 || count(o.Init.BaseDir, "submit large") != 0 || count(o.Init.BaseDir, "submit saturated") != 0 || count(o.Init.BaseDir, "submit active") != 1 {
		t.Fatal(events(o.Init.BaseDir))
	}
	t.Log("handler counts: invalid=0 oversized=0 saturated=0 canceled active=1")
}
func TestFaultsNoReplayAndReplacement(t *testing.T) {
	for _, mode := range []string{"dieentry", "lost", "oversize", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			o := options(t, mode)
			o.MaxMessageBytes = 1 << 20
			o.Restart = RestartPolicy{MaxAttempts: 1, InitialDelay: 150 * time.Millisecond, MaxDelay: 150 * time.Millisecond}
			p := start(t, o)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var r []api.Submission
			var e error
			go func() { defer close(done); r, e = p.Submit(ctx, launches("fault")) }()
			waitEvent(t, o.Init.BaseDir, "submit fault")
			if mode == "cancel" {
				cancel()
			} else if mode == "lost" {
				_ = os.WriteFile(filepath.Join(o.Init.BaseDir, "release"), nil, 0600)
			}
			<-done
			cancel()
			if e == nil || r[0].Status != api.Unknown {
				t.Fatalf("%#v %v", r, e)
			}
			if mode == "cancel" || mode == "oversize" {
				p.mu.Lock()
				g := p.current
				p.mu.Unlock()
				_ = g.runner.Kill(context.Background())
			}
			eventually(t, func() bool { return p.Health().State == "restarting" })
			before := time.Now()
			local, le := p.Submit(context.Background(), launches("during-restart"))
			if le == nil || local[0].Status == api.Unknown || time.Since(before) > 100*time.Millisecond {
				t.Fatal(local, le)
			}
			eventually(t, func() bool { return p.Health().State == "ready" && p.Health().Generation == 2 })
			// New invocation is allowed; the fixture's fault mode still applies.
			c, cc := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, _ = p.Submit(c, launches("fresh"))
			cc()
			if count(o.Init.BaseDir, "submit fault") != 1 || count(o.Init.BaseDir, "submit fresh") != 1 || count(o.Init.BaseDir, "submit during-restart") != 0 {
				t.Fatal(events(o.Init.BaseDir))
			}
			t.Log("handler counts across replacement: original=1 fresh=1 during-restart=0")
		})
	}
}
func TestFactoryFailuresChecksumAndCleanup(t *testing.T) {
	for _, mode := range []string{"initerror", "hungfactory", "factorytimeout", "prehandshake", "v1"} {
		t.Run(mode, func(t *testing.T) {
			o := options(t, mode)
			o.StartupTimeout = 2 * time.Second
			if mode == "prehandshake" {
				o.StartupTimeout = 250 * time.Millisecond
			}
			started := time.Now()
			p, e := Start(context.Background(), o)
			if e == nil || p != nil || time.Since(started) > 3*time.Second {
				t.Fatal(p, e, time.Since(started))
			}
			if mode == "initerror" || mode == "factorytimeout" {
				if count(o.Init.BaseDir, "cleanup") != 1 {
					t.Fatal(events(o.Init.BaseDir))
				}
			}
			if mode == "hungfactory" && count(o.Init.BaseDir, "factory hungfactory") != 1 {
				t.Fatal("hung factory was not entered")
			}
			if mode == "v1" && count(o.Init.BaseDir, "factory v1") != 0 {
				t.Fatal("factory invoked")
			}
			if b, e := os.ReadFile(filepath.Join(o.Init.BaseDir, "pid")); e == nil {
				pid, _ := strconv.Atoi(string(b))
				assertReaped(t, pid)
			}
			if b, e := os.ReadFile(filepath.Join(o.Init.BaseDir, "tmpdir")); e == nil {
				if _, e = os.Stat(string(b)); !os.IsNotExist(e) {
					t.Fatal("private directory remains")
				}
			}
		})
	}
	o := options(t, "")
	o.SHA256 = strings.Repeat("00", 32)
	if p, e := Start(context.Background(), o); e == nil || p != nil || events(o.Init.BaseDir) != "" {
		t.Fatal("checksum mismatch executed")
	}
	b, e := os.ReadFile(fixtureBinary)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(b)
	o.SHA256 = hex.EncodeToString(sum[:])
	p := start(t, o)
	env, e := os.ReadFile(filepath.Join(o.Init.BaseDir, "environment"))
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(env), "EXECUTOR_ONLY=") || strings.Contains(string(env), "exact-lease") || strings.Contains(string(env), "HOME=") {
		t.Fatal("ambient or executor environment leaked")
	}
	_ = p.Close()
}
func TestCallDeadlinesAndBoundedClose(t *testing.T) {
	o := options(t, "hangmethods")
	o.CallTimeout = 60 * time.Millisecond
	p := start(t, o)
	_, e := p.GetCapabilities(context.Background())
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	_, e = p.List(context.Background(), api.ListRequest{})
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	r, e := p.Submit(context.Background(), launches("deadline"))
	if !errors.Is(e, context.DeadlineExceeded) || r[0].Status != api.Unknown {
		t.Fatal(r, e)
	}
	o2 := options(t, "hungcleanup")
	o2.ShutdownTimeout = 150 * time.Millisecond
	p2 := start(t, o2)
	pid := readPID(t, o2.Init.BaseDir, "pid")
	begin := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := p2.Close(); !errors.Is(e, ErrCleanup) {
				t.Errorf("close error %v", e)
			}
		}()
	}
	wg.Wait()
	if time.Since(begin) > 500*time.Millisecond {
		t.Fatal("unbounded close")
	}
	assertReaped(t, pid)
	if count(o2.Init.BaseDir, "cleanup") != 1 {
		t.Fatal(events(o2.Init.BaseDir))
	}
}
func TestRestartExhaustionHealthyResetAndCloseRace(t *testing.T) {
	o := options(t, "")
	o.Restart = RestartPolicy{MaxAttempts: 1, InitialDelay: 20 * time.Millisecond, MaxDelay: 20 * time.Millisecond, HealthyResetInterval: 100 * time.Millisecond}
	p := start(t, o)
	for generation := uint64(1); generation <= 2; generation++ {
		time.Sleep(130 * time.Millisecond)
		p.mu.Lock()
		g := p.current
		p.mu.Unlock()
		_ = g.runner.Kill(context.Background())
		eventually(t, func() bool { return p.Health().State == "ready" && p.Health().Generation == generation+1 })
	}
	p.mu.Lock()
	g := p.current
	p.mu.Unlock()
	_ = g.runner.Kill(context.Background())
	eventually(t, func() bool { return p.Health().State == "exhausted" })
	if p.Health().RestartCount != 2 {
		t.Fatal(p.Health())
	}
	o2 := options(t, "")
	o2.Restart = RestartPolicy{MaxAttempts: 2, InitialDelay: 300 * time.Millisecond, MaxDelay: 300 * time.Millisecond}
	p2 := start(t, o2)
	p2.mu.Lock()
	g = p2.current
	p2.mu.Unlock()
	_ = g.runner.Kill(context.Background())
	eventually(t, func() bool { return p2.Health().State == "restarting" })
	_ = p2.Close()
	if p2.Health().Generation != 1 || p2.Health().State != "closed" {
		t.Fatal(p2.Health())
	}
	time.Sleep(320 * time.Millisecond)
	if count(o2.Init.BaseDir, "factory ") != 1 {
		t.Fatal(events(o2.Init.BaseDir))
	}
}

func TestDisabledRestartNeverLaunchesReplacement(t *testing.T) {
	o := options(t, "")
	p := start(t, o)
	p.mu.Lock()
	g := p.current
	p.mu.Unlock()
	_ = g.runner.Kill(context.Background())
	eventually(t, func() bool { return p.Health().State == "exhausted" })
	r, e := p.Submit(context.Background(), launches("disabled"))
	if e == nil || r[0].Status == api.Unknown {
		t.Fatal(r, e)
	}
	if count(o.Init.BaseDir, "factory ") != 1 || count(o.Init.BaseDir, "submit disabled") != 0 || p.Health().RestartCount != 0 {
		t.Fatal(p.Health(), events(o.Init.BaseDir))
	}
	t.Log("disabled restarts: factory invocations=1 replacement invocations=0 unavailable Submit invocations=0")
}

func TestCloseOverlappingProbePreservesCleanupError(t *testing.T) {
	// Frequent authenticated probes exercise cancellation while Control is in flight.
	// Each iteration uses a real child whose cleanup deliberately never returns.
	for i := 0; i < 20; i++ {
		o := options(t, "hungcleanup")
		o.ShutdownTimeout = 150 * time.Millisecond
		o.ProbeInterval = time.Microsecond
		p := start(t, o)
		pid := readPID(t, o.Init.BaseDir, "pid")
		time.Sleep(time.Millisecond)
		begin := time.Now()
		var wg sync.WaitGroup
		for caller := 0; caller < 4; caller++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if e := p.Close(); !errors.Is(e, ErrCleanup) {
					t.Errorf("iteration %d: Close = %v", i, e)
				}
			}()
		}
		wg.Wait()
		if time.Since(begin) > time.Second {
			t.Fatal("unbounded Close")
		}
		assertReaped(t, pid)
		if count(o.Init.BaseDir, "cleanup") != 1 || count(o.Init.BaseDir, "factory hungcleanup") != 1 {
			t.Fatal(events(o.Init.BaseDir))
		}
		if p.Health().State != "closed" {
			t.Fatal(p.Health())
		}
	}
}
