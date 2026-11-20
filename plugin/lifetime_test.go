package plugin

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func reapAdopted(pid int) {
	var state syscall.WaitStatus
	_, _ = syscall.Wait4(pid, &state, syscall.WNOHANG, nil)
}
func verifyNativeCap(t *testing.T, dir string) {
	t.Helper()
	pid := readPID(t, dir, "native-pid")
	before := count(dir, "progress")
	eventually(t, func() bool { return count(dir, "progress") > before+2 })
	waitEvent(t, dir, "cap_expired")
	after := count(dir, "progress")
	eventually(t, func() bool { reapAdopted(pid); return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
	time.Sleep(100 * time.Millisecond)
	if count(dir, "progress") != after {
		t.Fatal("native work continued after cap expiry")
	}
	if count(dir, "native_started") != 1 {
		t.Fatal("native work replayed")
	}
	t.Logf("native work progressed after fault, cap_expired=1, process %d terminated, post-expiry progress=0", pid)
}
func TestAcceptedNativeWorkSurvivesAndExpires(t *testing.T) {
	for _, mode := range []string{"nativecancel", "native", "nativedie"} {
		t.Run(mode, func(t *testing.T) {
			o := options(t, mode)
			o.Restart = RestartPolicy{MaxAttempts: 1, InitialDelay: 20 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
			p := start(t, o)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var r []api.Submission
			var e error
			go func() { defer close(done); r, _, e = p.Submit(ctx, launches("native-work"), api.SubmitOptions{}) }()
			waitEvent(t, o.Init.BaseDir, "accepted native-work")
			switch mode {
			case "nativecancel":
				cancel()
				<-done
				if e == nil || r[0].Status != api.Unknown {
					t.Fatal(r, e)
				}
			case "native":
				<-done
				if e != nil || r[0].Status != api.Accepted {
					t.Fatal(r, e)
				}
				if e = p.Close(); e != nil {
					t.Fatal(e)
				}
			case "nativedie":
				<-done
				if e == nil || r[0].Status != api.Unknown {
					t.Fatal(r, e)
				}
				eventually(t, func() bool { return p.Health().Generation == 2 })
			}
			cancel()
			verifyNativeCap(t, o.Init.BaseDir)
			if count(o.Init.BaseDir, "submit native-work") != 1 {
				t.Fatal(events(o.Init.BaseDir))
			}
		})
	}
}
func TestAbruptHostLossCancelsMaintenanceSparesNativeWork(t *testing.T) {
	dir := t.TempDir()
	host := exec.Command(fixtureBinary, "host", dir)
	host.Env = []string{}
	var diagnostics bytes.Buffer
	host.Stderr = &diagnostics
	if e := host.Start(); e != nil {
		t.Fatal(e)
	}
	hostDone := make(chan struct{})
	var hostErr error
	go func() { hostErr = host.Wait(); close(hostDone) }()
	t.Cleanup(func() { _ = host.Process.Kill(); <-hostDone })
	eventually(t, func() bool {
		select {
		case <-hostDone:
			t.Fatalf("host exited before readiness: %v; stderr: %s; events: %s", hostErr, diagnostics.String(), events(dir))
		default:
		}
		_, e := os.Stat(filepath.Join(dir, "host-ready"))
		return e == nil
	})
	pluginPID := readPID(t, dir, "pid")
	tmp, e := os.ReadFile(filepath.Join(dir, "tmpdir"))
	if e != nil {
		t.Fatal(e)
	}
	_ = host.Process.Kill()
	<-hostDone
	waitEvent(t, dir, "maintenance_canceled")
	eventually(t, func() bool { reapAdopted(pluginPID); return errors.Is(syscall.Kill(pluginPID, 0), syscall.ESRCH) })
	verifyNativeCap(t, dir)
	if count(dir, "submit host-loss") != 1 || count(dir, "cleanup") != 1 {
		t.Fatal(events(dir))
	}
	// The host died before it could remove its directory. The parent-loss server
	// must remove only its transport files, without touching native cap state.
	if _, e = os.Stat(string(tmp)); !os.IsNotExist(e) {
		t.Error("parent-loss transport directory remains", string(tmp))
	}
}

func TestParentLossDuringHungFactoryAndCleanup(t *testing.T) {
	for _, mode := range []string{"hungfactory", "hungcleanup"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			host := exec.Command(fixtureBinary, "host", dir, mode)
			host.Env = []string{}
			if e := host.Start(); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = host.Process.Kill() })
			waitEvent(t, dir, "factory "+mode)
			pid := readPID(t, dir, "pid")
			_ = host.Process.Kill()
			_ = host.Wait()
			eventually(t, func() bool { reapAdopted(pid); return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
			if mode == "hungcleanup" && count(dir, "cleanup") != 1 {
				t.Fatal(events(dir))
			}
			tmp, e := os.ReadFile(filepath.Join(dir, "tmpdir"))
			if e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(string(tmp)); !os.IsNotExist(e) {
				t.Error("parent loss left transport files", string(tmp))
			}
			t.Log("parent loss terminated and reaped child during", mode)
		})
	}
}
