package plugin

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

// StartCollector transfers listener ownership only after local validation.
func TestCollectorValidation(t *testing.T) {
	tr := testTrust(t)
	o := collectorOptions(tr)
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	for _, bad := range []CollectorOptions{{}, func() CollectorOptions { b := o; b.MaxPendingConnections = -1; return b }(), func() CollectorOptions { b := o; b.MaxWaiters = -1; return b }()} {
		if _, e = StartCollector(context.Background(), l, bad); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	if _, e = StartCollector(nil, l, o); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e = StartCollector(context.Background(), nil, o); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	conn, e := net.Dial("tcp", l.Addr().String())
	if e != nil {
		t.Fatal("invalid startup took listener ownership", e)
	}
	conn.Close()
}
func TestCollectorMembershipAndGenerations(t *testing.T) {
	tr := testTrust(t, "alpha", "beta")
	c := testCollector(t, collectorOptions(tr))
	s, e := c.Snapshot()
	if e != nil || s.Revision != 1 || len(s.Providers) != 0 {
		t.Fatal(s, e)
	}
	f := factoryFor(&reverseProvider{})
	cancelAlpha, doneAlpha := launchRun(t, f, runOptions(t, c, tr, "alpha"))
	s = nextSnapshot(t, c, 1)
	old := s.Providers[0]
	if old.ID != "alpha" {
		t.Fatal(s)
	}
	cancelBeta, doneBeta := launchRun(t, f, runOptions(t, c, tr, "beta"))
	s = nextSnapshot(t, c, 2)
	if s.Providers[0].ID != "alpha" || s.Providers[1].ID != "beta" {
		t.Fatal(s)
	}
	revision := s.Revision
	s.Providers[0].ID = "mutated"
	copied, _ := c.Snapshot()
	if copied.Providers[0].ID != "alpha" {
		t.Fatal("snapshot storage aliases")
	}
	cancelAlpha()
	if e = receiveRun(t, doneAlpha); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	s = nextSnapshot(t, c, 1)
	if s.Providers[0].ID != "beta" || s.Revision <= revision {
		t.Fatal(s)
	}
	_, e = old.Provider.GetCapabilities(context.Background())
	if !errors.Is(e, ErrUnavailable) {
		t.Fatal("stale handle", e)
	}
	cancelAlpha2, doneAlpha2 := launchRun(t, f, runOptions(t, c, tr, "alpha"))
	s = nextSnapshot(t, c, 2)
	if s.Providers[0].Generation <= old.Generation {
		t.Fatal(s)
	}
	ss, e := old.Provider.Submit(context.Background(), launches("old"))
	if !errors.Is(e, ErrUnavailable) || ss[0].Status != api.Unavailable {
		t.Fatal(ss, e)
	}
	cancelAlpha2()
	receiveRun(t, doneAlpha2)
	cancelBeta()
	receiveRun(t, doneBeta)
}
func TestCollectorPendingDuplicateAndFailure(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	f := func(ctx context.Context, in Init) (api.Provider, Cleanup, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		return &reverseProvider{}, nil, nil
	}
	_, done := launchRun(t, f, runOptions(t, c, tr, "alpha"))
	<-entered
	s, _ := c.Snapshot()
	if s.Revision != 1 || len(s.Providers) != 0 {
		t.Fatal(s)
	}
	o := runOptions(t, c, tr, "alpha")
	o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
	_, duplicate := launchRun(t, f, o)
	if e := receiveRun(t, duplicate); !errors.Is(e, ErrProtocol) {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate constructed", calls.Load())
	}
	close(release)
	nextSnapshot(t, c, 1)
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if e := receiveRun(t, done); e != nil {
		t.Fatal("orderly stop", e)
	}
	// Initialization rejection leaves ready revision unchanged and frees the ID.
	c2 := testCollector(t, collectorOptions(tr))
	var cleanup atomic.Int32
	_, failed := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		return nil, func(context.Context) error { cleanup.Add(1); return nil }, errors.New("private factory error")
	}, runOptions(t, c2, tr, "alpha"))
	if e := receiveRun(t, failed); e == nil {
		t.Fatal("factory failure succeeded")
	}
	eventually(t, func() bool { c2.mu.Lock(); defer c2.mu.Unlock(); return len(c2.reserved) == 0 })
	s, _ = c2.Snapshot()
	if s.Revision != 1 || len(s.Providers) != 0 || cleanup.Load() != 1 {
		t.Fatal(s, cleanup.Load())
	}
}
func TestCollectorConcurrentSnapshotsAndClose(t *testing.T) {
	tr := testTrust(t, "alpha", "beta")
	c := testCollector(t, collectorOptions(tr))
	f := factoryFor(&reverseProvider{})
	_, a := launchRun(t, f, runOptions(t, c, tr, "alpha"))
	_, b := launchRun(t, f, runOptions(t, c, tr, "beta"))
	nextSnapshot(t, c, 2)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := uint64(0)
			for j := 0; j < 100; j++ {
				s, e := c.Snapshot()
				if e != nil {
					if !errors.Is(e, ErrUnavailable) {
						t.Error(e)
					}
					return
				}
				if s.Revision < last {
					t.Error("revision regressed")
				}
				last = s.Revision
				for k := 1; k < len(s.Providers); k++ {
					if s.Providers[k-1].ID >= s.Providers[k].ID {
						t.Error("not unique sorted")
					}
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := c.Close(); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if e := receiveRun(t, a); e != nil {
		t.Fatal(e)
	}
	if e := receiveRun(t, b); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Snapshot(); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := c.WaitForChange(context.Background(), 0); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
func TestCollectorFatalListenerFailure(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	_, done := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	s := nextSnapshot(t, c, 1)
	_ = c.listener.Close()
	if e := c.Wait(); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := s.Providers[0].Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	receiveRun(t, done)
}

func TestCollectorContextCancellationSharesGracefulShutdown(t *testing.T) {
	tr := testTrust(t)
	for i := 0; i < 8; i++ {
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		c, e := StartCollector(ctx, listener, collectorOptions(tr))
		if e != nil {
			cancel()
			listener.Close()
			t.Fatal(e)
		}
		var cleaned atomic.Int32
		_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
			return &reverseProvider{}, func(context.Context) error { cleaned.Add(1); return nil }, nil
		}, runOptions(t, c, tr, "alpha"))
		s := nextSnapshot(t, c, 1)
		waiting := make(chan error, 1)
		go func() { _, e := c.WaitForChange(context.Background(), s.Revision); waiting <- e }()
		cancel()
		if e = c.Wait(); e != nil {
			t.Fatal("collector cancellation", e)
		}
		if e = receiveRun(t, done); e != nil {
			t.Fatal("provider did not receive graceful shutdown", e)
		}
		if cleaned.Load() != 1 {
			t.Fatal("cleanup count", cleaned.Load())
		}
		if e = <-waiting; !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
		if _, e = s.Providers[0].Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
	}
}
