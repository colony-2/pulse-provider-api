package plugin

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A transport-free collector uses the same membership/dispatch implementation.
// Pipes supply disposable ownership for synthetic members; no public test hooks.
func callbackCollector(t *testing.T, configure func(*CollectorOptions)) *Collector {
	t.Helper()
	o := CollectorOptions{CallbackTimeout: time.Second, CallbackQueue: 64, MaxSubscriptions: 32, ShutdownTimeout: 200 * time.Millisecond}
	if configure != nil {
		configure(&o)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{opts: o, listener: l, ctx: ctx, cancel: cancel, revision: 1, change: make(chan struct{}), ready: map[string]*Connection{}, reserved: map[string]*Connection{}, sockets: map[*sessionSocket]struct{}{}, subscriptions: map[*ProviderSubscription]struct{}{}, done: make(chan struct{}), transportDone: make(chan struct{})}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func callbackJoin(t *testing.T, c *Collector, id string) *Connection {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	g := &generation{id: c.generation}
	g.ctx, g.cancel = context.WithCancel(context.Background())
	p := &Connection{collector: c, id: id, g: g, socket: newSessionSocket(a), done: make(chan struct{}), sessionCaller: sessionCaller{current: g, ctx: g.ctx, cancel: g.cancel, permits: make(chan struct{}, 4)}}
	c.ready[id], c.reserved[id] = p, p
	c.sockets[p.socket] = struct{}{}
	if !c.changedLocked() {
		t.Fatal("revision exhausted")
	}
	c.publishLocked(ProviderChange{Kind: ProviderChangeMembership, Revision: c.revision, Reason: ProviderReasonReady, Joined: []AvailableProvider{{ID: id, Generation: g.id, Provider: p}}})
	return p
}

func callbackEvent(t *testing.T, ch <-chan ProviderChange) ProviderChange {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("callback did not arrive")
		return ProviderChange{}
	}
}
func callbackSubscribe(t *testing.T, c *Collector, fn func(context.Context, ProviderChange)) *ProviderSubscription {
	t.Helper()
	s, e := c.OnProvidersChanged(context.Background(), fn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Cancel)
	return s
}
func callbackWait(t *testing.T, s *ProviderSubscription) error {
	t.Helper()
	select {
	case <-s.done:
		return s.Wait()
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not finish")
		return nil
	}
}
func callbackSlots(c *Collector) int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.subscriptions) }

func TestCollectorCallbackBaselineAndRegistrationRace(t *testing.T) {
	c := callbackCollector(t, nil)
	before := c.change
	ch := make(chan ProviderChange, 100)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { ch <- v })
	v := callbackEvent(t, ch)
	if v.Kind != ProviderChangeInitial || v.Revision != 1 || len(v.Joined)+len(v.Left) != 0 {
		t.Fatal(v)
	}
	if c.change != before {
		t.Fatal("registration woke snapshot waiters")
	}
	s.Cancel()
	for i := 0; i < 20; i++ {
		var peer *Connection
		var sub *ProviderSubscription
		gate := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-gate
			sub = callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { ch <- v })
		}()
		go func() { defer wg.Done(); <-gate; peer = callbackJoin(t, c, "alpha") }()
		close(gate)
		wg.Wait()
		initial := callbackEvent(t, ch)
		if initial.Kind != ProviderChangeInitial {
			t.Fatal(initial)
		}
		if len(initial.Joined) == 0 {
			delta := callbackEvent(t, ch)
			if delta.Revision != initial.Revision+1 || len(delta.Joined) != 1 {
				t.Fatal(initial, delta)
			}
		} else if len(initial.Joined) != 1 || initial.Joined[0].Generation != peer.g.id {
			t.Fatal(initial)
		}
		sub.Cancel()
		peer.retire(ProviderReasonAdministrativeClose)
	}
}

func TestCollectorCallbackWaiterCapacityIndependent(t *testing.T) {
	c := callbackCollector(t, func(o *CollectorOptions) { o.MaxWaiters = 1; o.MaxSubscriptions = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waited := make(chan error, 1)
	go func() { _, e := c.WaitForChange(ctx, 1); waited <- e }()
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.waiters == 1 })
	log := make(chan ProviderChange, 2)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { log <- v })
	callbackEvent(t, log)
	if _, e := c.WaitForChange(context.Background(), 1); e != ErrSaturated {
		t.Fatal(e)
	}
	select {
	case e := <-waited:
		t.Fatal("registration woke waiter", e)
	default:
	}
	s.Cancel()
	eventually(t, func() bool { return callbackSlots(c) == 0 })
	cancel()
	if e := <-waited; e != context.Canceled {
		t.Fatal(e)
	}
}

func TestCollectorCallbackOrderingIsolationAndPayloadCopies(t *testing.T) {
	c := callbackCollector(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var concurrent atomic.Int32
	var calls atomic.Int32
	log := make(chan ProviderChange, 10)
	callbackSubscribe(t, c, func(ctx context.Context, v ProviderChange) {
		if concurrent.Add(1) != 1 {
			t.Error("concurrent delivery to one subscriber")
		}
		defer concurrent.Add(-1)
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		log <- v
	})
	<-entered
	fast := make(chan ProviderChange, 10)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) {
		fast <- v
		if len(v.Joined) > 0 {
			v.Joined[0].ID = "mutated"
		}
	})
	callbackEvent(t, fast)
	p := callbackJoin(t, c, "alpha")
	p.retire(ProviderReasonConnectionLost)
	a, b := callbackEvent(t, fast), callbackEvent(t, fast)
	if a.Revision != 2 || b.Revision != 3 || b.Left[0].Generation != p.g.id {
		t.Fatal(a, b)
	}
	// Release through its context without discarding queued deltas.
	close(release)
	initial, joined, left := callbackEvent(t, log), callbackEvent(t, log), callbackEvent(t, log)
	if initial.Revision != 1 || joined.Joined[0].ID != "alpha" || left.Left[0].ID != "alpha" {
		t.Fatal(initial, joined, left)
	}
	if _, e := joined.Joined[0].Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestCollectorCallbackOverflowAndFreshBaseline(t *testing.T) {
	c := callbackCollector(t, func(o *CollectorOptions) { o.CallbackQueue = 1 })
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	s := callbackSubscribe(t, c, func(ctx context.Context, _ ProviderChange) { entered <- ctx; <-release })
	ctx := <-entered
	p := callbackJoin(t, c, "alpha") // one queued event in addition to in-flight baseline
	s.mu.Lock()
	n := len(s.queue)
	s.mu.Unlock()
	if n != 1 {
		t.Fatal(n)
	}
	p.retire(ProviderReasonConnectionLost)
	if e := callbackWait(t, s); !errors.Is(e, ErrSaturated) {
		t.Fatal(e)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("overflow did not cancel in-flight callback")
	}
	s.mu.Lock()
	abandoned := s.queue
	s.mu.Unlock()
	if abandoned != nil {
		t.Fatal("abandoned queue retained")
	}
	fresh := make(chan ProviderChange, 1)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { fresh <- v })
	v := callbackEvent(t, fresh)
	if v.Kind != ProviderChangeInitial || v.Revision != 3 || len(v.Joined) != 0 {
		t.Fatal(v)
	}
	if callbackSlots(c) != 2 {
		t.Fatal("stuck worker slot was released")
	}
}

func TestCollectorCallbackCancelAndReentrancy(t *testing.T) {
	c := callbackCollector(t, nil)
	ready := make(chan *ProviderSubscription, 1)
	entered := make(chan context.Context, 1)
	var calls atomic.Int32
	s := callbackSubscribe(t, c, func(ctx context.Context, _ ProviderChange) {
		calls.Add(1)
		if _, e := c.Snapshot(); e != nil {
			t.Error(e)
		}
		self := <-ready
		self.Cancel()
		self.Cancel()
		entered <- ctx
	})
	ready <- s
	ctx := <-entered
	if e := callbackWait(t, s); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal(ctx.Err())
	}
	callbackJoin(t, c, "alpha")
	eventually(t, func() bool { return callbackSlots(c) == 0 })
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := s.Wait(); e != context.Canceled {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
}

func TestCollectorCallbackDispatchCancelBoundary(t *testing.T) {
	c := callbackCollector(t, nil)
	// A pre-canceled registration dispatches nothing. Cancellation after initial
	// dispatch prevents the membership event queued behind that callback.
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, e := c.OnProvidersChanged(ctx, func(context.Context, ProviderChange) { calls.Add(1) }); s != nil || e != context.Canceled {
		t.Fatal(s, e)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	s := callbackSubscribe(t, c, func(ctx context.Context, _ ProviderChange) { calls.Add(1); close(entered); <-release })
	<-entered
	callbackJoin(t, c, "alpha")
	s.Cancel()
	if callbackWait(t, s) != context.Canceled {
		t.Fatal("cancellation lost")
	}
	if calls.Load() != 1 {
		t.Fatal("undispatched callback ran")
	}
}

func TestCollectorCallbackPayloadSnapshotIsolation(t *testing.T) {
	c := callbackCollector(t, nil)
	p := callbackJoin(t, c, "alpha")
	first := make(chan ProviderChange, 4)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { first <- v })
	baseline := callbackEvent(t, first)
	mutated := make(chan struct{}, 4)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) {
		for i := range v.Joined {
			v.Joined[i] = AvailableProvider{ID: "private mutation"}
		}
		for i := range v.Left {
			v.Left[i] = AvailableProvider{ID: "private mutation"}
		}
		mutated <- struct{}{}
	})
	<-mutated
	snapshot, e := c.Snapshot()
	if e != nil || snapshot.Providers[0].ID != "alpha" || baseline.Joined[0].ID != "alpha" {
		t.Fatal(snapshot, baseline, e)
	}
	snapshot.Providers[0].ID = "snapshot mutation"
	if next, _ := c.Snapshot(); next.Providers[0].ID != "alpha" {
		t.Fatal(next)
	}
	p.retire(ProviderReasonConnectionLost)
	left := callbackEvent(t, first)
	<-mutated
	if left.Left[0].ID != "alpha" || left.Left[0].Generation != baseline.Joined[0].Generation || baseline.Joined[0].ID != "alpha" {
		t.Fatal(baseline, left)
	}
}

func TestCollectorCallbackPanicTimeoutAndStableResults(t *testing.T) {
	for _, kind := range []string{"panic", "timeout", "observer"} {
		t.Run(kind, func(t *testing.T) {
			c := callbackCollector(t, func(o *CollectorOptions) { o.CallbackTimeout = 40 * time.Millisecond })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "observer" {
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
			}
			s, e := c.OnProvidersChanged(ctx, func(ctx context.Context, _ ProviderChange) {
				if kind == "panic" {
					panic("private panic text")
				}
				<-ctx.Done()
			})
			if e != nil {
				t.Fatal(e)
			}
			e = callbackWait(t, s)
			if kind == "observer" {
				if e != context.DeadlineExceeded {
					t.Fatal(e)
				}
			} else if !errors.Is(e, ErrCallback) {
				t.Fatal(e)
			}
			if kind == "timeout" && !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal(e)
			}
			if strings.Contains(e.Error(), "private") {
				t.Fatal(e)
			}
			s.Cancel()
			c.stop(nil)
			if s.Wait() != e {
				t.Fatal("terminal result changed")
			}
			if c.Wait() != nil {
				t.Fatal("subscriber failure failed collector")
			}
		})
	}
}

func TestCollectorCallbackOptionsAndRegistrationErrors(t *testing.T) {
	tr := testTrust(t)
	for _, change := range []func(*CollectorOptions){
		func(o *CollectorOptions) { o.MaxSubscriptions = -1 }, func(o *CollectorOptions) { o.MaxSubscriptions = 1025 },
		func(o *CollectorOptions) { o.CallbackQueue = -1 }, func(o *CollectorOptions) { o.CallbackQueue = 4097 },
		func(o *CollectorOptions) { o.CallbackTimeout = -1 },
	} {
		o := collectorOptions(tr)
		change(&o)
		if e := o.defaults(); e != ErrInvalid {
			t.Fatal(e)
		}
	}
	o := collectorOptions(tr)
	if e := o.defaults(); e != nil {
		t.Fatal(e)
	}
	if o.MaxSubscriptions != 32 || o.CallbackQueue != 64 || o.CallbackTimeout != 5*time.Second {
		t.Fatal(o)
	}
	for _, n := range []int{1, 1024} {
		o := collectorOptions(tr)
		o.MaxSubscriptions = n
		if e := o.defaults(); e != nil {
			t.Fatal(e)
		}
	}
	for _, n := range []int{1, 4096} {
		o := collectorOptions(tr)
		o.CallbackQueue = n
		if e := o.defaults(); e != nil {
			t.Fatal(e)
		}
	}
	c := callbackCollector(t, func(o *CollectorOptions) { o.MaxSubscriptions = 1 })
	var calls atomic.Int32
	fn := func(context.Context, ProviderChange) { calls.Add(1) }
	if s, e := c.OnProvidersChanged(nil, fn); s != nil || e != ErrInvalid {
		t.Fatal(s, e)
	}
	if s, e := c.OnProvidersChanged(context.Background(), nil); s != nil || e != ErrInvalid {
		t.Fatal(s, e)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s := callbackSubscribe(t, c, func(context.Context, ProviderChange) { close(entered); <-release })
	<-entered
	if x, e := c.OnProvidersChanged(context.Background(), fn); x != nil || e != ErrSaturated {
		t.Fatal(x, e)
	}
	s.Cancel()
	if x, e := c.OnProvidersChanged(context.Background(), fn); x != nil || e != ErrSaturated {
		t.Fatal("worker bound bypassed", x, e)
	}
	_ = c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if x, e := c.OnProvidersChanged(ctx, fn); x != nil || e != context.Canceled {
		t.Fatal(x, e)
	}
	if x, e := c.OnProvidersChanged(context.Background(), fn); x != nil || e != ErrUnavailable {
		t.Fatal(x, e)
	}
	if calls.Load() != 0 {
		t.Fatal("failed registration dispatched")
	}
}

func TestCollectorCallbackStopOrderingAndEmptyFinal(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty", false: "members"}[empty], func(t *testing.T) {
			c := callbackCollector(t, nil)
			if !empty {
				callbackJoin(t, c, "zeta")
				callbackJoin(t, c, "alpha")
			}
			ch := make(chan ProviderChange, 10)
			s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { ch <- v })
			initial := callbackEvent(t, ch)
			if !empty && !reflect.DeepEqual([]string{initial.Joined[0].ID, initial.Joined[1].ID}, []string{"alpha", "zeta"}) {
				t.Fatal(initial)
			}
			if e := c.Close(); e != nil {
				t.Fatal(e)
			}
			final := callbackEvent(t, ch)
			if final.Kind != ProviderChangeStopped || final.Revision != initial.Revision+1 || final.Reason != ProviderReasonCollectorClosed || len(final.Joined) != 0 || len(final.Left) != len(initial.Joined) {
				t.Fatal(final)
			}
			for i, p := range final.Left {
				if p.ID != initial.Joined[i].ID {
					t.Fatal("final payload not sorted", final)
				}
				if _, e := p.Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
					t.Fatal(e)
				}
			}
			if callbackWait(t, s) != nil {
				t.Fatal(s.Wait())
			}
		})
	}
}

func TestCollectorCallbackFailedStopReason(t *testing.T) {
	c := callbackCollector(t, nil)
	log := make(chan ProviderChange, 2)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { log <- v })
	callbackEvent(t, log)
	c.stop(ErrUnavailable)
	if e := c.Wait(); e != ErrUnavailable {
		t.Fatal(e)
	}
	v := callbackEvent(t, log)
	if v.Kind != ProviderChangeStopped || v.Reason != ProviderReasonCollectorFailed || v.Revision != 2 || callbackWait(t, s) != ErrUnavailable {
		t.Fatal(v, s.Wait())
	}
}

func TestCollectorCallbackRevisionExhaustion(t *testing.T) {
	c := callbackCollector(t, nil)
	c.mu.Lock()
	c.revision = math.MaxUint64
	c.mu.Unlock()
	ch := make(chan ProviderChange, 2)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { ch <- v })
	if v := callbackEvent(t, ch); v.Revision != math.MaxUint64 {
		t.Fatal(v)
	}
	if e := c.Close(); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if e := callbackWait(t, s); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if len(ch) != 0 {
		t.Fatal("non-increasing final event")
	}
}

func TestCollectorCallbackStopCancelsObserverAndShortensDeadline(t *testing.T) {
	for _, observerCancel := range []bool{true, false} {
		t.Run(map[bool]string{true: "observer", false: "shutdown"}[observerCancel], func(t *testing.T) {
			c := callbackCollector(t, func(o *CollectorOptions) { o.ShutdownTimeout = 60 * time.Millisecond; o.CallbackTimeout = time.Hour })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan context.Context, 1)
			returned := make(chan struct{})
			s, e := c.OnProvidersChanged(ctx, func(ctx context.Context, _ ProviderChange) { entered <- ctx; <-ctx.Done(); close(returned) })
			if e != nil {
				t.Fatal(e)
			}
			inflight := <-entered
			before, _ := inflight.Deadline()
			if observerCancel {
				cancel()
			}
			start := time.Now()
			e = c.Close()
			if e != nil || time.Since(start) > 400*time.Millisecond {
				t.Fatal(e, time.Since(start))
			}
			if observerCancel {
				if s.Wait() != context.Canceled {
					t.Fatal(s.Wait())
				}
			} else {
				if !errors.Is(s.Wait(), ErrCallback) || !errors.Is(s.Wait(), context.DeadlineExceeded) {
					t.Fatal(s.Wait())
				}
				after, _ := inflight.Deadline()
				if !after.Before(before) {
					t.Fatal("running deadline not tightened")
				}
			}
			<-returned
		})
	}
}

func TestCollectorCallbackRemovalReasonWinsOnce(t *testing.T) {
	for _, winner := range []ProviderChangeReason{ProviderReasonConnectionLost, ProviderReasonProbeFailed, ProviderReasonAdministrativeClose} {
		c := callbackCollector(t, nil)
		ch := make(chan ProviderChange, 4)
		callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { ch <- v })
		callbackEvent(t, ch)
		p := callbackJoin(t, c, "alpha")
		callbackEvent(t, ch)
		p.retire(winner)
		var wg sync.WaitGroup
		for _, reason := range []ProviderChangeReason{ProviderReasonConnectionLost, ProviderReasonProbeFailed, ProviderReasonAdministrativeClose} {
			wg.Add(1)
			go func(reason ProviderChangeReason) { defer wg.Done(); p.retire(reason) }(reason)
		}
		wg.Wait()
		v := callbackEvent(t, ch)
		if v.Reason != winner || v.Revision != 3 || len(ch) != 0 {
			t.Fatal(v, len(ch))
		}
	}
}

func TestCollectorCallbackConcurrentTerminalResults(t *testing.T) {
	for i := 0; i < 20; i++ {
		c := callbackCollector(t, nil)
		entered, release := make(chan struct{}), make(chan struct{})
		s := callbackSubscribe(t, c, func(context.Context, ProviderChange) { close(entered); <-release; panic("private") })
		<-entered
		var wg sync.WaitGroup
		for _, e := range []error{context.Canceled, ErrSaturated, ErrCallback, callbackDeadlineError()} {
			wg.Add(1)
			go func(e error) { defer wg.Done(); s.fail(e) }(e)
		}
		wg.Wait()
		first := s.Wait()
		close(release)
		eventually(t, func() bool { return callbackSlots(c) == 0 })
		if first == nil || s.Wait() != first {
			t.Fatal("terminal result unstable")
		}
	}
}

func TestCollectorCallbackDispatchDeadlineAndSlotRelease(t *testing.T) {
	c := callbackCollector(t, func(o *CollectorOptions) { o.MaxSubscriptions = 1; o.CallbackTimeout = time.Minute })
	entered := make(chan context.Context, 2)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s := callbackSubscribe(t, c, func(ctx context.Context, v ProviderChange) {
		entered <- ctx
		if v.Kind == ProviderChangeInitial {
			<-release
		}
	})
	<-entered
	callbackJoin(t, c, "alpha")
	notBefore := time.Now().Add(c.opts.CallbackTimeout)
	close(release)
	ctx := <-entered
	deadline, _ := ctx.Deadline()
	if deadline.Before(notBefore) {
		t.Fatal("queued event used enqueue-time deadline", deadline, notBefore)
	}
	s.Cancel()
	s.Cancel()
	if e := callbackWait(t, s); e != context.Canceled {
		t.Fatal(e)
	}
	eventually(t, func() bool { return callbackSlots(c) == 0 })
	log := make(chan ProviderChange, 2)
	fresh := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { log <- v })
	if v := callbackEvent(t, log); v.Kind != ProviderChangeInitial || len(v.Joined) != 1 {
		t.Fatal(v)
	}
	fresh.Cancel()
	eventually(t, func() bool { return callbackSlots(c) == 0 })
}

func TestCollectorCallbackStopDrainsQueuedDeltas(t *testing.T) {
	c := callbackCollector(t, func(o *CollectorOptions) { o.ShutdownTimeout = time.Second })
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	log := make(chan ProviderChange, 8)
	s := callbackSubscribe(t, c, func(ctx context.Context, v ProviderChange) {
		if v.Kind == ProviderChangeInitial {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		log <- v
	})
	<-entered
	a := callbackJoin(t, c, "alpha")
	b := callbackJoin(t, c, "beta")
	a.retire(ProviderReasonConnectionLost)
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	<-c.transportDone // Handles have retired; subscription drain is still blocked.
	if _, e := b.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	close(release)
	for revision := uint64(1); revision <= 5; revision++ {
		v := callbackEvent(t, log)
		if v.Revision != revision {
			t.Fatal(v)
		}
		if revision == 5 && (v.Kind != ProviderChangeStopped || len(v.Left) != 1 || v.Left[0].ID != "beta") {
			t.Fatal(v)
		}
	}
	if e := <-closed; e != nil {
		t.Fatal(e)
	}
	if s.Wait() != nil {
		t.Fatal(s.Wait())
	}
}

func TestCollectorCallbackObserverCancellationDuringStop(t *testing.T) {
	c := callbackCollector(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s, e := c.OnProvidersChanged(ctx, func(context.Context, ProviderChange) { close(entered); <-release })
	if e != nil {
		t.Fatal(e)
	}
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	<-c.transportDone
	cancel()
	if e = callbackWait(t, s); e != context.Canceled {
		t.Fatal(e)
	}
	if e = <-closed; e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	queued := len(s.queue)
	s.mu.Unlock()
	if queued != 0 {
		t.Fatal(queued)
	}
}

// A host mock implementing the original two-method interface still compiles.
type callbackSnapshotOnly struct{}

func (callbackSnapshotOnly) Snapshot() (ProviderSnapshot, error) { return ProviderSnapshot{}, nil }
func (callbackSnapshotOnly) WaitForChange(context.Context, uint64) (ProviderSnapshot, error) {
	return ProviderSnapshot{}, nil
}

var _ ProviderCollector = callbackSnapshotOnly{}
