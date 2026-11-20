package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

type detailed interface {
	api.Provider
	SubmitDetailed(context.Context, []api.Launch, api.SubmitOptions) ([]api.Submission, plugin.SubmissionObservations, api.LogSubscription, error)
	Health() plugin.Health
}

func TestIndependentListener(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			dir := t.TempDir()

			exe, trust := os.Getenv("LOG_FIXTURE"), os.Getenv("LOG_TRUST")
			var p detailed
			if reverse {
				load := func(name string) tls.Certificate {
					v, e := tls.LoadX509KeyPair(filepath.Join(trust, name+".crt"), filepath.Join(trust, name+".key"))
					if e != nil {
						t.Fatal(e)
					}
					return v
				}
				ca, e := os.ReadFile(filepath.Join(trust, "ca.crt"))
				if e != nil {
					t.Fatal(e)
				}
				roots := x509.NewCertPool()
				if !roots.AppendCertsFromPEM(ca) {
					t.Fatal("CA")
				}
				listener, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					t.Fatal(e)
				}
				c, e := plugin.StartCollector(context.Background(), listener, plugin.CollectorOptions{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{plugin.ReverseALPN}, Certificates: []tls.Certificate{load("collector.test")}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, Authorize: func(_ context.Context, s tls.ConnectionState) (string, error) {
					if e := s.PeerCertificates[0].VerifyHostname("fixture"); e != nil {
						return "", e
					}
					return "fixture", nil
				}})
				if e != nil {
					t.Fatal(e)
				}
				defer c.Close()
				cmd := exec.Command(exe, "reverse", listener.Addr().String(), trust)
				cmd.Env = []string{"MODE=logs", "EVENT_DIR=" + dir}
				if e = cmd.Start(); e != nil {
					t.Fatal(e)
				}
				defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
				ctx, cc := context.WithTimeout(context.Background(), 3*time.Second)
				defer cc()
				snapshot, e := c.WaitForChange(ctx, 1)
				if e != nil || len(snapshot.Providers) != 1 {
					t.Fatal(snapshot, e)
				}
				p = snapshot.Providers[0].Provider.(detailed)
			} else {
				process, e := plugin.Start(context.Background(), plugin.StartOptions{Executable: exe, Env: map[string]string{"MODE": "logs", "EVENT_DIR": dir}, Init: plugin.Initialization{Name: "fixture", BaseDir: dir, ConfigYAML: []byte("mode: fixture\n"), ExecutorEnv: map[string]string{"EXECUTOR_ONLY": "executor-default"}}})
				if e != nil {
					t.Fatal(e)
				}
				defer process.Close()
				p = process
			}
			defer func() {
				_ = os.WriteFile(filepath.Join(dir, "log-exit"), nil, 0600)
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					b, e := os.ReadFile(filepath.Join(dir, "log-pid-independent"))
					if e != nil {
						return
					}
					pid, _ := strconv.Atoi(string(b))
					if syscall.Kill(pid, 0) != nil {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				t.Error("fixture child did not exit")
			}()
			if p.Health().Protocol != 3 {
				t.Fatal(p.Health())
			}
			ls := []api.Launch{{LaunchID: "independent", Allocation: api.Allocation{Base: "test", CPUMillis: 1}, Job: api.JobRef{JobDB: "db", InstanceID: "instance", TenantID: "tenant", JobID: "job"}, Lease: "private-lease"}}
			mu := sync.Mutex{}
			logs := map[api.LogSource]string{}
			ctx, cc := context.WithCancel(context.Background())
			ss, obs, h, e := p.SubmitDetailed(ctx, ls, api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), Duration: 0, OnLog: func(_ context.Context, r api.LaunchLog) error {
				mu.Lock()
				logs[r.Source] += string(r.Data)
				mu.Unlock()
				return nil
			}}})
			cc()
			if e != nil || len(ss) != 1 || ss[0].Status != api.Accepted || obs.Invocation == 0 || h == nil {
				t.Fatal(ss, obs, h, e)
			}
			defer h.Stop()
			if e = os.WriteFile(filepath.Join(dir, "log-output"), nil, 0600); e != nil {
				t.Fatal(e)
			}
			wait, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			result, e := h.Wait(wait)
			if e != nil || result.Reason != api.ListenerSourcesClosed {
				t.Fatal(result, e)
			}
			mu.Lock()
			defer mu.Unlock()
			for src, marker := range map[api.LogSource]string{api.LogSourceLaunch: "launch-before:independent", api.LogSourceStdout: "stdout-after:independent:299", api.LogSourceStderr: "stderr-after:independent"} {
				if !strings.Contains(logs[src], marker) {
					t.Fatal("missing source marker", src, marker)
				}
			}
			t.Log("baseline 3 independent listener received launch/stdout/stderr markers")
		})
	}
}
