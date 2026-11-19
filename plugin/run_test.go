package plugin

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func TestRunLocalInitializationAndLifetime(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	o := runOptions(t, c, tr, "alpha")
	var cleaned atomic.Int32
	maintenance := make(chan struct{})
	want := copyInitialization(o.Init)
	_, done := launchRun(t, func(ctx context.Context, in Init) (api.Provider, Cleanup, error) {
		if in.Name != want.Name || in.BaseDir != want.BaseDir || string(in.ConfigYAML) != string(want.ConfigYAML) || in.ExecutorEnv["EXECUTOR_ONLY"] != "local" {
			t.Error("initialization", in.Initialization)
		}
		go func() { <-in.Lifetime.Done(); close(maintenance) }()
		return &reverseProvider{}, func(context.Context) error { cleaned.Add(1); return nil }, nil
	}, o)
	nextSnapshot(t, c, 1)
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if e := receiveRun(t, done); e != nil {
		t.Fatal(e)
	}
	select {
	case <-maintenance:
	case <-time.After(time.Second):
		t.Fatal("maintenance not canceled")
	}
	if cleaned.Load() != 1 {
		t.Fatal(cleaned.Load())
	}
}
func TestRunLateFactoryCleanupAndNoReconnect(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	o := runOptions(t, c, tr, "alpha")
	o.StartupTimeout = 60 * time.Millisecond
	o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	entered := make(chan struct{})
	release := make(chan struct{})
	var constructed, cleaned atomic.Int32
	_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		constructed.Add(1)
		close(entered)
		<-release
		return &reverseProvider{}, func(context.Context) error { cleaned.Add(1); return nil }, nil
	}, o)
	<-entered
	if e := receiveRun(t, done); !errors.Is(e, ErrCleanup) {
		t.Fatal(e)
	}
	if constructed.Load() != 1 {
		t.Fatal("reconnected hung factory")
	}
	close(release)
	eventually(t, func() bool { return cleaned.Load() == 1 })
	s, _ := c.Snapshot()
	if len(s.Providers) != 0 || s.Revision != 1 {
		t.Fatal(s)
	}
}
func TestRunCleanupFailureStopsReconnect(t *testing.T) {
	for _, hang := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "timeout"}[hang], func(t *testing.T) {
			tr := testTrust(t)
			c := testCollector(t, collectorOptions(tr))
			o := runOptions(t, c, tr, "alpha")
			o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
			o.Serve.ShutdownTimeout = 40 * time.Millisecond
			release := make(chan struct{})
			defer close(release)
			var constructed, cleaned atomic.Int32
			_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
				constructed.Add(1)
				return &reverseProvider{}, func(context.Context) error {
					cleaned.Add(1)
					if hang {
						<-release
					}
					return errors.New("private cleanup failure")
				}, nil
			}, o)
			p := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			_ = p.socket.Close()
			if e := receiveRun(t, done); !errors.Is(e, ErrCleanup) {
				t.Fatal(e)
			}
			if constructed.Load() != 1 || cleaned.Load() != 1 {
				t.Fatal(constructed.Load(), cleaned.Load())
			}
		})
	}
}
func TestRunReconnectNewGenerationAndCancellation(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	o := runOptions(t, c, tr, "alpha")
	o.Reconnect = RestartPolicy{MaxAttempts: 2, InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond, JitterFraction: 0.1}
	var constructed atomic.Int32
	cancel, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		constructed.Add(1)
		return &reverseProvider{}, nil, nil
	}, o)
	first := nextSnapshot(t, c, 1).Providers[0]
	p := first.Provider.(*Connection)
	_ = p.socket.Close()
	eventually(t, func() bool {
		s, e := c.Snapshot()
		return e == nil && len(s.Providers) == 1 && s.Providers[0].Generation > first.Generation
	})
	if _, e := p.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if constructed.Load() != 2 {
		t.Fatal(constructed.Load())
	}
	cancel()
	if e := receiveRun(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestRunUninitializedHostStartupBound(t *testing.T) {
	tr := testTrust(t)
	release := make(chan struct{})
	defer close(release)
	addr := reversePeer(t, tr, func(c *tls.Conn) { _ = writeAdmission(c, 0, "alpha"); <-release })
	o := RunOptions{Address: addr, TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}, StartupTimeout: 50 * time.Millisecond}
	var constructed atomic.Int32
	_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		constructed.Add(1)
		return &reverseProvider{}, nil, nil
	}, o)
	if e := receiveRun(t, done); e == nil || constructed.Load() != 0 {
		t.Fatal(e, constructed.Load())
	}
}

func TestRunFiniteTransientRetriesAndBackoffCancellation(t *testing.T) {
	for _, cancelBackoff := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhaustion", true: "cancellation"}[cancelBackoff], func(t *testing.T) {
			tr := testTrust(t)
			raw, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer raw.Close()
			var accepts atomic.Int32
			first := make(chan struct{})
			go func() {
				for {
					c, e := raw.Accept()
					if e != nil {
						return
					}
					if accepts.Add(1) == 1 {
						close(first)
					}
					_ = c.Close()
				}
			}()
			o := RunOptions{Address: raw.Addr().String(), TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}, StartupTimeout: time.Second, Reconnect: RestartPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, JitterFraction: 0.2}}
			if cancelBackoff {
				o.Reconnect.InitialDelay = time.Second
				o.Reconnect.MaxDelay = time.Second
			}
			cancel, done := launchRun(t, factoryFor(&reverseProvider{}), o)
			<-first
			if cancelBackoff {
				cancel()
				if e = receiveRun(t, done); !errors.Is(e, context.Canceled) || accepts.Load() != 1 {
					t.Fatal(e, accepts.Load())
				}
			} else {
				if e = receiveRun(t, done); e == nil || accepts.Load() != 3 {
					t.Fatal(e, accepts.Load())
				}
			}
		})
	}
}

func TestRunHealthyResetExcludesCleanupTime(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	o := runOptions(t, c, tr, "alpha")
	o.Serve.ShutdownTimeout = 300 * time.Millisecond
	o.Reconnect = RestartPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, HealthyResetInterval: 100 * time.Millisecond}
	var constructed atomic.Int32
	_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		constructed.Add(1)
		return &reverseProvider{}, func(context.Context) error { time.Sleep(120 * time.Millisecond); return nil }, nil
	}, o)
	for i := 0; i < 2; i++ {
		s := nextSnapshot(t, c, 1)
		_ = s.Providers[0].Provider.(*Connection).socket.Close()
		nextSnapshot(t, c, 0)
	}
	if e := receiveRun(t, done); e == nil || constructed.Load() != 2 {
		t.Fatal("cleanup time reset retry budget", e, constructed.Load())
	}
}

func TestRunShutdownUsesOneNegotiatedBudget(t *testing.T) {
	tr := testTrust(t)
	co := collectorOptions(tr)
	co.ShutdownTimeout = 150 * time.Millisecond
	c := testCollector(t, co)
	o := runOptions(t, c, tr, "alpha")
	o.Serve.ShutdownTimeout = time.Second
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	p := &reverseProvider{caps: func(context.Context) (api.Capabilities, error) {
		close(entered)
		<-release
		return api.Capabilities{}, nil
	}}
	_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		return p, func(context.Context) error { time.Sleep(70 * time.Millisecond); return nil }, nil
	}, o)
	handle := nextSnapshot(t, c, 1).Providers[0].Provider
	call := make(chan error, 1)
	go func() { _, e := handle.GetCapabilities(context.Background()); call <- e }()
	<-entered
	start := time.Now()
	_ = c.Close()
	if e := receiveRun(t, done); !errors.Is(e, ErrCleanup) {
		t.Fatal(e)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("cleanup and drain received separate shutdown budgets", time.Since(start))
	}
	if e := <-call; e == nil {
		t.Fatal("inflight call survived shutdown")
	}
}
