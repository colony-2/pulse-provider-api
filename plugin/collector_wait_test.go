package plugin

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCollectorWaitRevisionsAndBounds(t *testing.T) {
	tr := testTrust(t)
	o := collectorOptions(tr)
	o.MaxWaiters = 2
	c := testCollector(t, o)
	if _, e := c.WaitForChange(context.Background(), 2); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.WaitForChange(ctx, 0); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e := c.WaitForChange(ctx, 1)
			if e == nil && (s.Revision <= 1 || len(s.Providers) != 1) {
				e = errors.New("wrong membership")
			}
			results <- e
		}()
	}
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.waiters == 2 })
	if _, e := c.WaitForChange(context.Background(), 1); !errors.Is(e, ErrSaturated) {
		t.Fatal(e)
	}
	// An already changed revision bypasses waiter saturation.
	if s, e := c.WaitForChange(context.Background(), 0); e != nil || s.Revision != 1 {
		t.Fatal(s, e)
	}
	cancelRun, done := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	eventually(t, func() bool { s, e := c.Snapshot(); return e == nil && len(s.Providers) == 1 })
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal(e)
		}
	}
	cancel()
	// A change preceding registration cannot be lost.
	if s, e := c.WaitForChange(context.Background(), 1); e != nil || len(s.Providers) != 1 {
		t.Fatal(s, e)
	}
	current, _ := c.Snapshot()
	ctx, cancel = context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { _, e := c.WaitForChange(ctx, current.Revision); finished <- e }()
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.waiters == 1 })
	cancel()
	if e := <-finished; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	c.mu.Lock()
	n := c.waiters
	c.mu.Unlock()
	if n != 0 {
		t.Fatal("waiter leaked", n)
	}
	cancelRun()
	receiveRun(t, done)
	nextSnapshot(t, c, 0)
	latest, e := c.WaitForChange(context.Background(), 1)
	if e != nil || len(latest.Providers) != 0 || latest.Revision <= current.Revision {
		t.Fatal(latest, e)
	}
}
func TestCollectorShutdownWakesWaiters(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	done := make(chan error, 1)
	go func() { _, e := c.WaitForChange(context.Background(), 1); done <- e }()
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.waiters == 1 })
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not woken")
	}
}
