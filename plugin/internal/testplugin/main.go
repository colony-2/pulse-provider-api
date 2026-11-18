// A process fixture. Its native runner deliberately owns detachment and the cap;
// no SDK cleanup callback grants or removes execution authority.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

func appendEvent(dir, s string) {
	f, e := os.OpenFile(filepath.Join(dir, "events"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	_, _ = fmt.Fprintln(f, s)
	_ = f.Sync()
}
func waitFile(ctx context.Context, path string) {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		if _, e := os.Stat(path); e == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "native" {
		native()
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "host" {
		host()
		return
	}
	if os.Getenv("MODE") == "v1" {
		fmt.Println("1|1|unix|/nonexistent|grpc|")
		time.Sleep(time.Minute)
		return
	}
	if os.Getenv("MODE") == "prehandshake" {
		time.Sleep(time.Minute)
		return
	}
	e := plugin.Serve(func(ctx context.Context, in plugin.Init) (api.Provider, plugin.Cleanup, error) {
		dir := os.Getenv("EVENT_DIR")
		mode := os.Getenv("MODE")
		appendEvent(dir, "factory "+mode)
		_ = os.WriteFile(filepath.Join(dir, "pid"), []byte(strconv.Itoa(os.Getpid())), 0600)
		_ = os.WriteFile(filepath.Join(dir, "tmpdir"), []byte(filepath.Dir(os.Getenv("PLUGIN_UNIX_SOCKET_DIR"))), 0600)
		_ = os.WriteFile(filepath.Join(dir, "environment"), []byte(strings.Join(os.Environ(), "\n")), 0600)
		if in.Name != "fixture" || in.BaseDir != dir || string(in.ConfigYAML) != "mode: fixture\n" || in.ExecutorEnv["EXECUTOR_ONLY"] != "executor-default" {
			return nil, nil, errors.New("init mismatch")
		}
		go func() { <-in.Lifetime.Done(); appendEvent(dir, "maintenance_canceled") }()
		cleanup := func(ctx context.Context) error {
			appendEvent(dir, "cleanup")
			if mode == "hungcleanup" {
				select {}
			}
			return nil
		}
		if mode == "hungfactory" {
			select {}
		}
		if mode == "factorytimeout" {
			<-ctx.Done()
			return nil, cleanup, ctx.Err()
		}
		if mode == "initerror" {
			return nil, cleanup, errors.New("sentinel private initialization")
		}
		return &fixture{dir: dir, mode: mode}, cleanup, nil
	}, plugin.ServeOptions{Implementation: "fixture", Version: "1.0.0", FactoryTimeout: factoryTimeout(), ShutdownTimeout: 300 * time.Millisecond, MaxMessageBytes: serverLimit()})
	if e != nil {
		os.Exit(1)
	}
}
func serverLimit() int { v, _ := strconv.Atoi(os.Getenv("SERVER_LIMIT")); return v }
func factoryTimeout() time.Duration {
	if os.Getenv("MODE") == "factorytimeout" {
		return 50 * time.Millisecond
	}
	return time.Second
}

type fixture struct{ dir, mode string }

func (f *fixture) GetCapabilities(ctx context.Context) (api.Capabilities, error) {
	appendEvent(f.dir, "capabilities")
	if f.mode == "hangmethods" {
		<-ctx.Done()
		return api.Capabilities{}, ctx.Err()
	}
	c := api.Capabilities{Bases: []api.BaseCapabilities{{Name: "busy", Capabilities: []string{"network", "gpu"}}, {Name: "warming", Capabilities: []string{}}}, BasesComplete: false}
	if f.mode == "dataerror" {
		return c, errors.New("private-discovery-error")
	}
	return c, nil
}
func (f *fixture) List(ctx context.Context, r api.ListRequest) (api.ListResponse, error) {
	appendEvent(f.dir, "list")
	if f.mode == "hangmethods" {
		<-ctx.Done()
		return api.ListResponse{}, ctx.Err()
	}
	if r.PageSize != 100 || r.PageToken != "\xff\xfe" || r.LaunchID != "filter" {
		return api.ListResponse{}, errors.New("list mismatch")
	}
	v := api.ListResponse{Items: []api.Instance{}, NextPageToken: r.PageToken}
	if f.mode == "dataerror" {
		return v, errors.New("private-list-error")
	}
	return v, nil
}
func (f *fixture) Submit(ctx context.Context, ls []api.Launch) ([]api.Submission, error) {
	for _, l := range ls {
		appendEvent(f.dir, "submit "+l.LaunchID)
	}
	switch f.mode {
	case "dieentry":
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case "cancel", "lost":
		waitFile(ctx, filepath.Join(f.dir, "release"))
		if f.mode == "lost" {
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	case "hangmethods":
		<-ctx.Done()
		return nil, ctx.Err()
	case "oversize":
		return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.NoCapacity, Reason: strings.Repeat("x", 2<<20)}}, nil
	case "native", "nativecancel", "nativedie":
		l := ls[0]
		if l.TimeoutSeconds != 2 || l.StartBefore == nil || l.StartBefore.Year() != 9999 || string(l.Lease) != "\xffexact-lease" || l.Base != "ubuntu[ab]*/arm*" || l.CPUMillis != api.MaxQuantity {
			return nil, errors.New("constraint mismatch")
		}
		exe, _ := os.Executable()
		cmd := exec.Command(exe, "native", f.dir, "2")
		cmd.Env = []string{}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil
		if e := cmd.Start(); e != nil {
			return nil, e
		}
		go cmd.Wait()
		waitFile(ctx, filepath.Join(f.dir, "native-ready"))
		appendEvent(f.dir, "accepted "+l.LaunchID)
		if f.mode == "nativecancel" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if f.mode == "nativedie" {
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}
	var out []api.Submission
	for i, l := range ls {
		s := api.Submission{LaunchID: l.LaunchID, Status: api.Accepted}
		if f.mode == "mixed" {
			switch i {
			case 1:
				s.Status = api.NoCapacity
				s.Reason = "busy-original"
			case 2:
				s.Status = api.Status("future")
				s.Reason = "future-original"
			case 3:
				out = append(out, api.Submission{LaunchID: l.LaunchID, Status: api.Rejected, Reason: "duplicate-original"})
				s.Status = api.Rejected
				s.Reason = "second-original"
			}
		}
		out = append(out, s)
	}
	if f.mode == "correlated" {
		out = []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Rejected, Reason: ls[0].LaunchID + "-original"}, {LaunchID: ls[0].LaunchID, Status: api.Rejected, Reason: ls[0].LaunchID + "-duplicate"}}
		return out, errors.New(ls[0].LaunchID + "-error")
	}
	if f.mode == "mixed" {
		return out, errors.New("private-method-error")
	}
	return out, nil
}
func native() {
	dir := os.Args[2]
	seconds, _ := strconv.Atoi(os.Args[3])
	appendEvent(dir, "native_started")
	_ = os.WriteFile(filepath.Join(dir, "native-pid"), []byte(strconv.Itoa(os.Getpid())), 0600)
	_ = os.WriteFile(filepath.Join(dir, "native-ready"), []byte("ready"), 0600)
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for time.Now().Before(deadline) {
		appendEvent(dir, "progress")
		time.Sleep(30 * time.Millisecond)
	}
	appendEvent(dir, "cap_expired")
}
func host() {
	dir := os.Args[2]
	exe, _ := os.Executable()
	p, e := plugin.Start(context.Background(), plugin.StartOptions{Executable: exe, Env: map[string]string{"EVENT_DIR": dir, "MODE": hostMode()}, Init: plugin.Initialization{Name: "fixture", BaseDir: dir, ConfigYAML: []byte("mode: fixture\n"), ExecutorEnv: map[string]string{"EXECUTOR_ONLY": "executor-default"}}, ShutdownTimeout: 500 * time.Millisecond})
	if e != nil {
		os.Exit(2)
	}
	defer p.Close()
	t := time.Date(9999, 1, 1, 0, 0, 0, 123, time.UTC)
	_, e = p.Submit(context.Background(), []api.Launch{{LaunchID: "host-loss", Allocation: api.Allocation{Base: "ubuntu[ab]*/arm*", CPUMillis: api.MaxQuantity}, Lease: api.Lease("\xffexact-lease"), Job: api.JobRef{JobDB: "https://db/tenant", InstanceID: "db", TenantID: "tenant", JobID: "job"}, TimeoutSeconds: 2, StartBefore: &t}})
	if e != nil {
		os.Exit(3)
	}
	_ = os.WriteFile(filepath.Join(dir, "host-ready"), []byte("ready"), 0600)
	select {}
}

func hostMode() string {
	if len(os.Args) > 3 {
		return os.Args[3]
	}
	return "native"
}
