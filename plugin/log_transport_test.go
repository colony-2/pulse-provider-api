package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func logFile(t *testing.T, dir, name string) {
	t.Helper()
	if e := os.WriteFile(filepath.Join(dir, name), nil, 0600); e != nil {
		t.Fatal(e)
	}
}
func logTransport(t *testing.T, reverse bool, mode string, configure func(*LogOptions)) (api.Provider, string) {
	t.Helper()
	var p api.Provider
	var dir string
	if reverse {
		tr := testTrust(t, "fixture")
		o := collectorOptions(tr)
		o.CallTimeout = 250 * time.Millisecond
		o.MaxMessageBytes = 1024
		if configure != nil {
			configure(&o.LogOptions)
		}
		c := testCollector(t, o)
		dir = t.TempDir()
		reverseProcess(t, c.listener.Addr().String(), tr.files(t), dir, mode)
		p = nextSnapshot(t, c, 1).Providers[0].Provider
	} else {
		o := options(t, mode)
		o.CallTimeout = 250 * time.Millisecond
		o.MaxMessageBytes = 1024
		if configure != nil {
			configure(&o.LogOptions)
		}
		dir = o.Init.BaseDir
		p = start(t, o)
	}
	t.Cleanup(func() {
		logFile(t, dir, "log-exit")
		files, _ := filepath.Glob(filepath.Join(dir, "log-pid-*"))
		for _, f := range files {
			pid := readPID(t, dir, filepath.Base(f))
			eventually(t, func() bool { reapAdopted(pid); return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
		}
	})
	return p, dir
}
func TestLogTransportsSourcesForeverAndSmallEnvelopes(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, combined := range []bool{false, true} {
			t.Run(fmt.Sprintf("reverse=%v/combined=%v", reverse, combined), func(t *testing.T) {
				mode := "logs"
				if combined {
					mode = "logs-combined"
				}
				p, dir := logTransport(t, reverse, mode, nil)
				var mu sync.Mutex
				output := map[string][]byte{}
				records := 0
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ss, sub, e := p.Submit(ctx, launches("one", "two"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), Duration: 0, OnLog: func(_ context.Context, v api.LaunchLog) error {
					mu.Lock()
					defer mu.Unlock()
					if len(v.Data) > api.MaxLogRecordBytes || v.SubscriptionID != "fixture-logs" || v.Timestamp.IsZero() {
						t.Error("invalid record", v)
					}
					output[v.LaunchID+"/"+string(v.Source)] = append(output[v.LaunchID+"/"+string(v.Source)], v.Data...)
					records++
					return nil
				}}})
				if e != nil || len(ss) != 2 || ss[0].Status != api.Accepted || ss[1].Status != api.Accepted || sub == nil {
					t.Fatal(ss, sub, e)
				}
				cancel() // accepted admission context ends; log context remains independent
				// The stream must survive the SDK admission timeout before post-startup output.
				<-time.After(300 * time.Millisecond)
				select {
				case <-sub.Done():
					t.Fatal("admission timeout ended logs", logWait(t, sub))
				default:
				}
				logFile(t, dir, "log-output")
				result := logWait(t, sub)
				if result.Reason != api.ListenerSourcesClosed || result.Cause != nil {
					t.Fatal(result)
				}
				mu.Lock()
				defer mu.Unlock()
				for _, id := range []string{"one", "two"} {
					launch := string(output[id+"/launch"])
					if launch != "launch-before:"+id+"\nlaunch-after:"+id+"\n" {
						t.Fatal("missing launch markers", launch)
					}
					stdout := []byte("stdout-before:" + id + "\n")
					stdout = append(stdout, bytes.Repeat([]byte{0xfe}, 4096)...)
					for i := 0; i < 300; i++ {
						stdout = append(stdout, append([]byte{0xff, 0, 27}, []byte(fmt.Sprintf("[err]stdout-after:%s:%03d:%s\n", id, i, string(make([]byte, 256))))...)...)
					}
					stderr := []byte("stderr-before:" + id + "\nstderr-after:" + id + "\n")
					if combined {
						data := output[id+"/combined"]
						if !bytes.Contains(data, []byte("stdout-before:"+id)) || !bytes.Contains(data, []byte("stderr-after:"+id)) || len(data) != len(stdout)+len(stderr) {
							t.Fatal("missing combined markers", id, len(data))
						}
					} else {
						if !bytes.Equal(output[id+"/stdout"], stdout) || !bytes.Equal(output[id+"/stderr"], stderr) {
							t.Fatalf("missing or altered execution bytes for %s: out=%d want=%d err=%q", id, len(output[id+"/stdout"]), len(stdout), output[id+"/stderr"])
						}
					}
					// Native work continues after every log source has closed.
					before, _ := os.ReadFile(filepath.Join(dir, "log-progress-"+id))
					eventually(t, func() bool {
						after, _ := os.ReadFile(filepath.Join(dir, "log-progress-"+id))
						return len(after) > len(before)+2
					})
				}
				if records <= 256 {
					t.Fatal("did not exceed former diagnostic event budget", records)
				}
				for _, q := range result.Coverage {
					if combined && q.Source != api.LogSourceLaunch && q.Source != api.LogSourceCombined {
						if q.State != api.SourceUnavailable || !errors.Is(q.Cause, api.ErrLogSource) {
							t.Fatal(q)
						}
					} else if q.State != api.SourceClosed || !q.Closed || q.LastSequence == 0 {
						t.Fatal(q)
					}
				}
				t.Logf("both launches delivered launch and execution bytes; %d records; sources closed while native work progressed", records)
			})
		}
	}
}
func TestLogTransportsStopDuringAdmission(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, dir := logTransport(t, reverse, "logs-hold", nil)
			logs, cancelLogs := context.WithCancel(context.Background())
			defer cancelLogs()
			entered := make(chan struct{}, 1)
			returned := make(chan struct{})
			var ss []api.Submission
			var sub api.LogSubscription
			var e error
			go func() {
				defer close(returned)
				ss, sub, e = p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: logs, OnLog: func(_ context.Context, v api.LaunchLog) error {
					if v.Source == api.LogSourceLaunch {
						select {
						case entered <- struct{}{}:
						default:
						}
					}
					return nil
				}}})
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("listener not armed before preparation")
			}
			cancelLogs()
			select {
			case <-returned:
				t.Fatal("log cancellation completed held admission")
			default:
			}
			logFile(t, dir, "log-admit")
			<-returned
			if e != nil || ss[0].Status != api.Accepted {
				t.Fatal(ss, e)
			}
			if r := logWait(t, sub); r.Reason != api.ListenerContextCanceled || !errors.Is(r.Cause, context.Canceled) {
				t.Fatal(r)
			}
			logFile(t, dir, "log-output")
			eventually(t, func() bool { v, _ := os.ReadFile(filepath.Join(dir, "log-progress-one")); return len(v) > 10 })
			if count(dir, "submit one") != 1 {
				t.Fatal("admission replayed", events(dir))
			}
			if _, e := p.GetCapabilities(context.Background()); e != nil {
				t.Fatal("business call blocked after stop", e)
			}
		})
	}
}
func TestLogTransportsDurationAndNoListener(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, dir := logTransport(t, reverse, "logs-hold", nil)
			noListener, _, e := p.Submit(context.Background(), []api.Launch{}, api.SubmitOptions{})
			if e == nil || len(noListener) != 0 {
				t.Fatal("invalid batch accepted")
			}
			ctx, cc := context.WithCancel(context.Background())
			cc()
			logFile(t, dir, "log-admit")
			ss, sub, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: ctx, OnLog: func(context.Context, api.LaunchLog) error { t.Error("canceled listener called"); return nil }}})
			if e != nil || ss[0].Status != api.Accepted || logWait(t, sub).Reason != api.ListenerContextCanceled {
				t.Fatal(ss, e)
			}
			ss, sub, e = p.Submit(context.Background(), launches("two"), api.SubmitOptions{})
			if e != nil || sub != nil || ss[0].Status != api.Accepted {
				t.Fatal(ss, sub, e)
			}
			if count(dir, "submit one") != 1 || count(dir, "submit two") != 1 {
				t.Fatal(events(dir))
			}
		})
	}
}

func TestLogTransportsStuckCallbacksKeepBusinessUsable(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, dir := logTransport(t, reverse, "logs", func(o *LogOptions) { o.MaxSubscriptions = 1; o.CallbackTimeout = 30 * time.Millisecond })
			entered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			_, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { close(entered); <-release; return nil }}})
			if e != nil {
				t.Fatal(e)
			}
			<-entered
			if r := logWait(t, h); r.Reason != api.ListenerSlow {
				t.Fatal(r)
			}
			ss, failed, e := p.Submit(context.Background(), launches("two"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
			if e != nil || ss[0].Status != api.Accepted || logWait(t, failed).Reason != api.ListenerCapacityExceeded {
				t.Fatal(ss, e)
			}
			if _, e := p.GetCapabilities(context.Background()); e != nil {
				t.Fatal(e)
			}
			if _, e := p.List(context.Background(), api.ListRequest{LaunchID: "filter", PageToken: "\xff\xfe"}); e != nil {
				t.Fatal(e)
			}
			logFile(t, dir, "log-output")
			eventually(t, func() bool { b, _ := os.ReadFile(filepath.Join(dir, "log-progress-two")); return len(b) > 20 })
		})
	}
}
func TestLogTransportsLossDuringAdmissionDoesNotReplay(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, dir := logTransport(t, reverse, "logs-hold", nil)
			entered := make(chan struct{}, 1)
			done := make(chan struct{})
			var ss []api.Submission
			var h api.LogSubscription
			var e error
			go func() {
				defer close(done)
				ss, h, e = p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error {
					select {
					case entered <- struct{}{}:
					default:
					}
					return nil
				}}})
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("no preparation record")
			}
			switch p := p.(type) {
			case *Process:
				p.mu.Lock()
				conn := p.current.conn
				p.mu.Unlock()
				_ = conn.Close()
			case *Connection:
				_ = p.g.conn.Close()
			}
			<-done
			if e == nil || ss[0].Status != api.Unknown {
				t.Fatal(ss, e)
			}
			if r := logWait(t, h); r.Reason != api.ListenerTransportLost || !errors.Is(r.Cause, api.ErrLogTransport) {
				t.Fatal(r)
			}
			if count(dir, "submit one") != 1 {
				t.Fatal("possible admission replayed", events(dir))
			}
			logFile(t, dir, "log-admit")
		})
	}
}
func TestLogProcessRejectsBaselineTwo(t *testing.T) {
	o := options(t, "v2")
	p, e := Start(context.Background(), o)
	if e == nil {
		_ = p.Close()
		t.Fatal("baseline 2 peer accepted")
	}
	if count(o.Init.BaseDir, "factory v2") != 0 {
		t.Fatal("factory initialized before version rejection")
	}
}

func TestLogTransportsRejectedLaunchAndUnavailableSources(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, mode := range []string{"logs-error", "logs-none"} {
			t.Run(fmt.Sprintf("%v/%s", reverse, mode), func(t *testing.T) {
				p, _ := logTransport(t, reverse, mode, nil)
				var mu sync.Mutex
				var text string
				ss, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(_ context.Context, v api.LaunchLog) error {
					mu.Lock()
					text += string(v.Data)
					mu.Unlock()
					if v.Source != api.LogSourceLaunch {
						t.Error("launch error relabeled as execution output")
					}
					return nil
				}}})
				if h == nil {
					t.Fatal(e)
				}
				r := logWait(t, h)
				if mode == "logs-error" {
					mu.Lock()
					defer mu.Unlock()
					if e == nil || ss[0].Status != api.Rejected || text != "launch-error:one" || r.Reason != api.ListenerSourcesClosed {
						t.Fatal(ss, text, r, e)
					}
				} else if e != nil || ss[0].Status != api.Accepted || r.Reason != api.ListenerSourceUnavailable || !errors.Is(r.Cause, api.ErrLogSource) {
					t.Fatal(ss, r, e)
				}
				for _, q := range r.Coverage {
					if mode == "logs-none" || q.Source != api.LogSourceLaunch {
						if q.State != api.SourceUnavailable || !errors.Is(q.Cause, api.ErrLogSource) {
							t.Fatal(q)
						}
					}
				}
			})
		}
	}
}

func TestLogTransportsAdministrativeClose(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, _ := logTransport(t, reverse, "logs", nil)
			_, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
			if e != nil {
				t.Fatal(e)
			}
			if e = p.(interface{ Close() error }).Close(); e != nil {
				t.Fatal(e)
			}
			r := logWait(t, h)
			if r.Reason != api.ListenerProviderClosed || !errors.Is(r.Cause, api.ErrProviderClosed) {
				t.Fatal(r)
			}
		})
	}
}

func TestLogTransportsKeepInputsOutOfOutputAndHealth(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, _ := logTransport(t, reverse, "logs-error", nil)
			ls := launches("one")
			ls[0].Lease = "private-lease-sentinel"
			ls[0].Job.JobDB = "private-jobdb-sentinel"
			var mu sync.Mutex
			var output string
			_, h, _ := p.Submit(context.Background(), ls, api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(_ context.Context, v api.LaunchLog) error {
				mu.Lock()
				output += string(v.Data)
				mu.Unlock()
				return nil
			}}})
			_ = logWait(t, h)
			mu.Lock()
			defer mu.Unlock()
			health := fmt.Sprint(p.(interface{ Health() Health }).Health())
			for _, sentinel := range []string{"private-lease-sentinel", "private-jobdb-sentinel", "mode: fixture", "executor-default", "PULSE_PLUGIN_COOKIE", "PLUGIN_CLIENT_CERT"} {
				if strings.Contains(output, sentinel) || strings.Contains(health, sentinel) {
					t.Fatal("private input escaped into logs or public health", sentinel)
				}
			}
		})
	}
}

func TestLogTransportsFiniteBudgetExpiresDuringAdmission(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			p, dir := logTransport(t, reverse, "logs-hold", nil)
			canceled := make(chan struct{})
			returned := make(chan struct{})
			var h api.LogSubscription
			var ss []api.Submission
			var e error
			go func() {
				defer close(returned)
				ss, h, e = p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), Duration: 80 * time.Millisecond, OnLog: func(c context.Context, _ api.LaunchLog) error { <-c.Done(); close(canceled); return nil }}})
			}()
			select {
			case <-canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("finite duration did not cancel in-flight launch callback")
			}
			select {
			case <-returned:
				t.Fatal("finite duration canceled held admission")
			default:
			}
			logFile(t, dir, "log-admit")
			<-returned
			if e != nil || ss[0].Status != api.Accepted {
				t.Fatal(ss, e)
			}
			if r := logWait(t, h); r.Reason != api.ListenerDurationElapsed || r.Cause != nil {
				t.Fatal(r)
			}
		})
	}
}
