package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func logLimits(t *testing.T) LogOptions {
	t.Helper()
	o := LogOptions{}
	if e := o.defaults(); e != nil {
		t.Fatal(e)
	}
	return o
}
func logWait(t *testing.T, s api.LogSubscription) api.ListenerResult {
	t.Helper()
	c, cc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cc()
	r, e := s.Wait(c)
	if r.Reason == "" {
		t.Fatalf("no terminal result: %v", e)
	}
	if (r.Cause == nil) != (e == nil) {
		t.Fatal(r, e)
	}
	return r
}
func logRecord(n uint64) api.LaunchLog {
	return api.LaunchLog{LaunchID: "one", SubscriptionID: "subscription", Sequence: n, Source: api.LogSourceStdout, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 123, time.UTC), Data: []byte("output")}
}
func TestLogSubscriptionStopGateAndCapacity(t *testing.T) {
	o := logLimits(t)
	o.MaxSubscriptions = 1
	o.Queue = 1
	pool := newLogPool(o)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	s := newLogSubscription(o, pool, launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(c context.Context, _ api.LaunchLog) error { entered <- c; <-release; return nil }})
	if e := s.enqueue(logRecord(1)); e != nil {
		t.Fatal(e)
	}
	c := <-entered
	if e := s.enqueue(logRecord(2)); e != nil {
		t.Fatal(e)
	}
	s.Stop()
	s.Stop()
	r := logWait(t, s)
	if r.Reason != api.ListenerStopped || r.Cause != nil {
		t.Fatal(r)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("callback context not canceled")
	}
	if len(pool.slots) != 1 {
		t.Fatal("stuck worker capacity released early")
	}
	other := newLogSubscription(o, pool, launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { t.Error("callback after stop"); return nil }})
	if r := logWait(t, other); r.Reason != api.ListenerCapacityExceeded || !errors.Is(r.Cause, api.ErrLogCapacity) {
		t.Fatal(r)
	}
	if r.Coverage[1].LastSequence != 1 {
		t.Fatal(r)
	}
	if e := s.enqueue(logRecord(3)); !errors.Is(e, api.ErrStopListening) {
		t.Fatal(e)
	}
}
func TestLogSubscriptionOverflowAndSlowCallback(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprint(slow), func(t *testing.T) {
			o := logLimits(t)
			o.MaxSubscriptions = 1
			o.Queue = 1
			if slow {
				o.CallbackTimeout = 20 * time.Millisecond
			}
			pool := newLogPool(o)
			entered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			s := newLogSubscription(o, pool, launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { close(entered); <-release; return nil }})
			_ = s.enqueue(logRecord(1))
			<-entered
			if !slow {
				_ = s.enqueue(logRecord(2))
				if e := s.enqueue(logRecord(3)); !errors.Is(e, api.ErrLogCapacity) {
					t.Fatal(e)
				}
			}
			r := logWait(t, s)
			want := api.ListenerOverflow
			if slow {
				want = api.ListenerSlow
			}
			if r.Reason != want || len(pool.slots) != 1 {
				t.Fatal(r, len(pool.slots))
			}
			if slow && (!errors.Is(r.Cause, api.ErrLogDelivery) || !errors.Is(r.Cause, context.DeadlineExceeded)) {
				t.Fatal(r)
			}
		})
	}
}
func TestLogSubscriptionCallbackOutcomes(t *testing.T) {
	original := errors.New("local callback")
	for name, fn := range map[string]func(context.Context, api.LaunchLog) error{
		"intentional": func(context.Context, api.LaunchLog) error { return fmt.Errorf("wrapped: %w", api.ErrStopListening) },
		"error":       func(context.Context, api.LaunchLog) error { return original },
		"panic":       func(context.Context, api.LaunchLog) error { panic("private panic sentinel") },
	} {
		t.Run(name, func(t *testing.T) {
			o := logLimits(t)
			s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: fn})
			_ = s.enqueue(logRecord(1))
			r := logWait(t, s)
			if name == "intentional" {
				if r.Reason != api.ListenerStopped || r.Cause != nil {
					t.Fatal(r)
				}
			} else {
				if r.Reason != api.ListenerError || !errors.Is(r.Cause, api.ErrLogDelivery) {
					t.Fatal(r)
				}
				if name == "error" && !errors.Is(r.Cause, original) {
					t.Fatal(r)
				}
				if name == "panic" && r.Cause.Error() != api.ErrLogDelivery.Error() {
					t.Fatal(r)
				}
			}
		})
	}
}
func TestLogSubscriptionWaitSnapshotsAndConcurrentStop(t *testing.T) {
	o := logLimits(t)
	s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }})
	c, cc := context.WithCancel(context.Background())
	cc()
	if _, e := s.Wait(c); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case <-s.Done():
		t.Fatal("wait stopped listening")
	default:
	}
	coverage := s.Coverage()
	coverage[0].State = api.SourceClosed
	if s.Coverage()[0].State == api.SourceClosed {
		t.Fatal("coverage aliases state")
	}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				s.Stop()
			} else {
				s.finish(api.ListenerTransportLost, api.ErrLogTransport)
			}
		}(i)
	}
	wg.Wait()
	want := logWait(t, s)
	want.Coverage[0].State = api.SourceClosed
	for i := 0; i < 24; i++ {
		r, e := s.Wait(c)
		if r.Reason != want.Reason || !errors.Is(e, want.Cause) || r.Coverage[0].State == api.SourceClosed {
			t.Fatal(r, e)
		}
	}
}
func TestLogSubscriptionContextsDurationAndReentrancy(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		o := logLimits(t)
		c, cc := context.WithCancel(context.Background())
		if deadline {
			c, cc = context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		}
		s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: c, OnLog: func(context.Context, api.LaunchLog) error { return nil }})
		cc()
		r := logWait(t, s)
		if r.Reason != contextReason(c.Err()) || !errors.Is(r.Cause, c.Err()) {
			t.Fatal(r)
		}
	}
	o := logLimits(t)
	s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), Duration: time.Nanosecond, OnLog: func(context.Context, api.LaunchLog) error { return nil }})
	if r := logWait(t, s); r.Reason != api.ListenerDurationElapsed || r.Cause != nil {
		t.Fatal(r)
	}
	var reentrant *logSubscription
	gate := make(chan struct{})
	reentrant = newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { <-gate; reentrant.Stop(); return nil }})
	_ = reentrant.enqueue(logRecord(1))
	close(gate)
	if r := logWait(t, reentrant); r.Reason != api.ListenerStopped {
		t.Fatal(r)
	}
}
func TestLogSubscriptionForeverVolumeAndCopies(t *testing.T) {
	o := logLimits(t)
	o.Queue = 1
	pool := newLogPool(o)
	ack := make(chan api.LaunchLog, 1)
	var concurrent atomic.Int32
	s := newLogSubscription(o, pool, launches("one"), api.ListenOptions{Context: context.Background(), Duration: 0, OnLog: func(_ context.Context, v api.LaunchLog) error {
		if concurrent.Add(1) != 1 {
			t.Error("parallel callbacks")
		}
		defer concurrent.Add(-1)
		ack <- v
		return nil
	}})
	defer s.Stop()
	data := make([]byte, api.MaxLogRecordBytes)
	data[0] = 0xff
	data[1] = 0
	for n := uint64(1); n <= 300; n++ {
		v := logRecord(n)
		v.Data = data
		if e := s.enqueue(v); e != nil {
			t.Fatal(n, e)
		}
		received := <-ack
		if received.Sequence != n || len(received.Data) != len(data) || received.Data[0] != 0xff {
			t.Fatal(n, received)
		}
		received.Data[0] = 0
		if data[0] != 0xff {
			t.Fatal("consumer mutation reached producer")
		}
	}
	s.mu.Lock()
	queueLen := len(s.queue)
	s.mu.Unlock()
	if queueLen > 1 || len(s.launches) != 1 || len(s.Coverage()) != 3 || len(pool.slots) != 1 {
		t.Fatal("unbounded state")
	}
	s.complete(api.ListenerResult{Reason: api.ListenerSourcesClosed})
	if r := logWait(t, s); r.Reason != api.ListenerSourcesClosed || r.Coverage[1].LastSequence != 300 {
		t.Fatal(r)
	}
}
func TestLogSubscriptionNormalCloseDrains(t *testing.T) {
	o := logLimits(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}})
	_ = s.enqueue(logRecord(1))
	<-entered
	_ = s.enqueue(logRecord(2))
	s.complete(api.ListenerResult{Reason: api.ListenerSourcesClosed})
	select {
	case <-s.Done():
		t.Fatal("normal closure skipped queued output")
	default:
	}
	close(release)
	if r := logWait(t, s); r.Reason != api.ListenerSourcesClosed || calls.Load() != 2 {
		t.Fatal(r, calls.Load())
	}
}
func TestLogOptionsAndValidation(t *testing.T) {
	o := logLimits(t)
	if o.MaxSubscriptions != 32 || o.Queue != 64 || o.CallbackTimeout != 5*time.Second || o.SetupTimeout != 5*time.Second {
		t.Fatal(o)
	}
	for _, bad := range []LogOptions{{MaxSubscriptions: -1}, {MaxSubscriptions: 1025}, {Queue: -1}, {Queue: 4097}, {CallbackTimeout: -1}, {SetupTimeout: -1}} {
		if bad.defaults() == nil {
			t.Fatal(bad)
		}
	}
	for _, l := range []api.ListenOptions{{}, {Context: context.Background()}, {OnLog: func(context.Context, api.LaunchLog) error { return nil }}, {Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }, Duration: -1}} {
		if validateListener(api.SubmitOptions{Listener: &l}) == nil {
			t.Fatal("accepted invalid listener")
		}
	}
}

type manualLogTimer struct {
	at     time.Duration
	fn     func()
	active bool
}
type manualLogClock struct {
	mu     sync.Mutex
	now    time.Duration
	timers []*manualLogTimer
}

func (c *manualLogClock) after(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	v := &manualLogTimer{at: c.now + d, fn: f, active: true}
	c.timers = append(c.timers, v)
	c.mu.Unlock()
	return func() bool { c.mu.Lock(); defer c.mu.Unlock(); old := v.active; v.active = false; return old }
}
func (c *manualLogClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	var fs []func()
	for _, v := range c.timers {
		if v.active && v.at <= c.now {
			v.active = false
			fs = append(fs, v.fn)
		}
	}
	c.mu.Unlock()
	for _, f := range fs {
		f()
	}
}
func TestLogSubscriptionManualLifetimeAndIsolation(t *testing.T) {
	o := logLimits(t)
	pool := newLogPool(o)
	clock := &manualLogClock{}
	listener := api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}
	forever := newLogSubscriptionWithTimer(o, pool, launches("one"), listener, clock.after)
	defer forever.Stop()
	listener.Duration = 30 * time.Minute
	finite := newLogSubscriptionWithTimer(o, pool, launches("one"), listener, clock.after)
	if len(clock.timers) != 1 || clock.timers[0].at != 30*time.Minute {
		t.Fatal("forever timer created or finite duration substituted", clock.timers)
	}
	clock.advance(29 * time.Minute)
	select {
	case <-finite.Done():
		t.Fatal("finite lifetime shortened")
	case <-forever.Done():
		t.Fatal("forever stopped")
	default:
	}
	clock.advance(time.Minute)
	if r := logWait(t, finite); r.Reason != api.ListenerDurationElapsed {
		t.Fatal(r)
	}
	clock.advance(24 * time.Hour)
	select {
	case <-forever.Done():
		t.Fatal("forever acquired startup cutoff")
	default:
	}
}
func TestLogSubscriptionProducerMutationAndFrozenNativeClosure(t *testing.T) {
	o := logLimits(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	received := make(chan []byte, 2)
	s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(_ context.Context, v api.LaunchLog) error {
		if v.Sequence == 1 {
			close(entered)
			<-release
		}
		received <- v.Data
		return nil
	}})
	_ = s.enqueue(logRecord(1))
	<-entered
	v := logRecord(2)
	v.Data = []byte("original")
	_ = s.enqueue(v)
	v.Data[0] = 'X'
	coverage := s.Coverage()
	coverage[1].Closed = true
	coverage[1].State = api.SourceActive
	if e := s.setCoverage(coverage); e != nil {
		t.Fatal(e)
	}
	close(release)
	<-received
	if got := string(<-received); got != "original" {
		t.Fatal(got)
	}
	s.Stop()
	result := logWait(t, s)
	if !result.Coverage[1].Closed || result.Coverage[1].State != api.SourceInterrupted || result.Coverage[1].LastSequence != 2 {
		t.Fatal(result)
	}
}
