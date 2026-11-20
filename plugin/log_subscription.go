package plugin

import (
	"context"
	"errors"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

// LogOptions bounds resident resources, independently of listening lifetime.
// Zero fields select defaults; ListenOptions.Duration zero still means forever.
type LogOptions struct {
	MaxSubscriptions int
	Queue            int
	CallbackTimeout  time.Duration
	SetupTimeout     time.Duration
}

func (o *LogOptions) defaults() error {
	if o.MaxSubscriptions == 0 {
		o.MaxSubscriptions = 32
	}
	if o.Queue == 0 {
		o.Queue = 64
	}
	if o.MaxSubscriptions < 1 || o.MaxSubscriptions > 1024 || o.Queue < 1 || o.Queue > 4096 {
		return ErrInvalid
	}
	if e := duration(&o.CallbackTimeout, 5*time.Second); e != nil {
		return e
	}
	return duration(&o.SetupTimeout, 5*time.Second)
}
func validateListener(o api.SubmitOptions) error {
	if o.Listener != nil && (o.Listener.Context == nil || o.Listener.OnLog == nil || o.Listener.Duration < 0) {
		return ErrInvalid
	}
	return nil
}

type logPool struct{ slots chan struct{} }

func newLogPool(o LogOptions) *logPool {
	return &logPool{slots: make(chan struct{}, o.MaxSubscriptions)}
}

// mu is also the callback dispatch gate. Never execute user code under mu.
type logSubscription struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	wake      chan struct{}
	queue     []api.LaunchLog
	options   LogOptions
	listener  api.ListenOptions
	coverage  []api.SourceStatus
	launches  map[string]uint64
	identity  string
	result    api.ListenerResult
	terminal  bool
	draining  *api.ListenerResult
	cleanup   []func()
	afterFunc func(time.Duration, func()) func() bool
}

func newLogSubscription(o LogOptions, pool *logPool, ls []api.Launch, l api.ListenOptions) *logSubscription {
	return newLogSubscriptionWithTimer(o, pool, ls, l, func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop })
}
func newLogSubscriptionWithTimer(o LogOptions, pool *logPool, ls []api.Launch, l api.ListenOptions, after func(time.Duration, func()) func() bool) *logSubscription {
	s := &logSubscription{done: make(chan struct{}), wake: make(chan struct{}, 1), options: o, listener: l, afterFunc: after, launches: make(map[string]uint64, len(ls))}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for _, v := range ls {
		s.launches[v.LaunchID] = 0
		for _, src := range []api.LogSource{api.LogSourceLaunch, api.LogSourceStdout, api.LogSourceStderr} {
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: v.LaunchID, Source: src, State: api.SourcePending})
		}
	}
	if l.Context.Err() != nil {
		s.finish(contextReason(l.Context.Err()), l.Context.Err())
		return s
	}
	select {
	case pool.slots <- struct{}{}:
	default:
		s.finish(api.ListenerCapacityExceeded, api.ErrLogCapacity)
		return s
	}
	s.addCleanupStop(context.AfterFunc(l.Context, func() { s.finish(contextReason(l.Context.Err()), l.Context.Err()) }))
	if l.Duration > 0 {
		stopTimer := s.afterFunc(l.Duration, func() { s.finish(api.ListenerDurationElapsed, nil) })
		s.addCleanup(func() { stopTimer() })
	}
	go func() { defer func() { <-pool.slots }(); s.run() }()
	return s
}
func contextReason(e error) api.ListenerReason {
	if errors.Is(e, context.DeadlineExceeded) {
		return api.ListenerContextDeadline
	}
	return api.ListenerContextCanceled
}
func (s *logSubscription) addCleanupStop(stop func() bool) { s.addCleanup(func() { stop() }) }
func (s *logSubscription) addCleanup(f func()) {
	s.mu.Lock()
	if !s.terminal {
		s.cleanup = append(s.cleanup, f)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	f()
}
func (s *logSubscription) Stop()                 { s.finish(api.ListenerStopped, nil) }
func (s *logSubscription) Done() <-chan struct{} { return s.done }
func (s *logSubscription) Coverage() []api.SourceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]api.SourceStatus(nil), s.coverage...)
}
func (s *logSubscription) Wait(ctx context.Context) (api.ListenerResult, error) {
	result := func() (api.ListenerResult, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.result
		r.Coverage = append([]api.SourceStatus(nil), r.Coverage...)
		return r, r.Cause
	}
	select {
	case <-s.done:
		return result()
	default:
	}
	select {
	case <-s.done:
		return result()
	case <-ctx.Done():
		select {
		case <-s.done:
			return result()
		default:
			return api.ListenerResult{}, ctx.Err()
		}
	}
}
func (s *logSubscription) finish(reason api.ListenerReason, cause error) {
	s.mu.Lock()
	fs := s.finishLocked(reason, cause)
	s.mu.Unlock()
	for _, f := range fs {
		f()
	}
}
func (s *logSubscription) finishLocked(reason api.ListenerReason, cause error) []func() {
	if s.terminal {
		return nil
	}
	s.terminal = true
	for i := range s.coverage {
		v := &s.coverage[i]
		if v.State == api.SourcePending || v.State == api.SourceActive {
			v.State = api.SourceInterrupted
		}
	}
	s.result = api.ListenerResult{Reason: reason, Cause: cause, Coverage: append([]api.SourceStatus(nil), s.coverage...)}
	s.queue = nil
	s.draining = nil
	s.cancel()
	close(s.done)
	fs := s.cleanup
	s.cleanup = nil
	return fs
}
func (s *logSubscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *logSubscription) enqueue(v api.LaunchLog) error {
	s.mu.Lock()
	if s.terminal {
		s.mu.Unlock()
		return api.ErrStopListening
	}
	n, ok := s.launches[v.LaunchID]
	bad := !ok || v.Source.Prefix() == "" || !validLogID(v.SubscriptionID) || v.Sequence == 0 || n == ^uint64(0) || v.Sequence != n+1 || len(v.Data) > api.MaxLogRecordBytes || (!v.Timestamp.IsZero() && !validLogTime(v.Timestamp))
	if s.identity != "" && s.identity != v.SubscriptionID {
		bad = true
	}
	if bad {
		s.mu.Unlock()
		s.finish(api.ListenerProtocolError, api.ErrLogProtocol)
		return api.ErrLogProtocol
	}
	if s.draining != nil {
		s.mu.Unlock()
		s.finish(api.ListenerProtocolError, api.ErrLogProtocol)
		return api.ErrLogProtocol
	}
	if len(s.queue) >= s.options.Queue {
		s.mu.Unlock()
		s.finish(api.ListenerOverflow, api.ErrLogCapacity)
		return api.ErrLogCapacity
	}
	s.identity = v.SubscriptionID
	s.launches[v.LaunchID] = v.Sequence
	v.Data = append([]byte(nil), v.Data...)
	s.queue = append(s.queue, v)
	s.mu.Unlock()
	s.signal()
	return nil
}
func (s *logSubscription) setCoverage(v []api.SourceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return nil
	}
	if len(v) > len(s.launches)*4 {
		return api.ErrLogProtocol
	}
	seen := make(map[string]bool, len(v))
	out := append([]api.SourceStatus(nil), v...)
	for i := range out {
		q := &out[i]
		key := q.LaunchID + "/" + string(q.Source)
		if _, ok := s.launches[q.LaunchID]; !ok || q.Source.Prefix() == "" || seen[key] || !validSourceState(q.State) {
			return api.ErrLogProtocol
		}
		seen[key] = true
		q.LastSequence = 0
		for _, old := range s.coverage {
			if old.LaunchID == q.LaunchID && old.Source == q.Source {
				q.LastSequence = old.LastSequence
				q.Closed = q.Closed || old.Closed
				break
			}
		}
	}
	// Every expected native source must be accounted for, including unavailable.
	for id := range s.launches {
		for _, src := range []api.LogSource{api.LogSourceLaunch, api.LogSourceStdout, api.LogSourceStderr} {
			if !seen[id+"/"+string(src)] {
				return api.ErrLogProtocol
			}
		}
	}
	s.coverage = out
	return nil
}
func validSourceState(v api.SourceState) bool {
	switch v {
	case api.SourcePending, api.SourceActive, api.SourceClosed, api.SourceUnavailable, api.SourceInterrupted:
		return true
	}
	return false
}
func (s *logSubscription) complete(r api.ListenerResult) {
	if r.Reason != api.ListenerSourcesClosed {
		s.finish(r.Reason, r.Cause)
		return
	}
	s.mu.Lock()
	if !s.terminal {
		s.draining = &r
	}
	s.mu.Unlock()
	s.signal()
}
func (s *logSubscription) run() {
	for {
		s.mu.Lock()
		if s.terminal {
			s.mu.Unlock()
			return
		}
		if len(s.queue) == 0 {
			r := s.draining
			s.mu.Unlock()
			if r != nil {
				s.finish(r.Reason, r.Cause)
				return
			}
			select {
			case <-s.done:
				return
			case <-s.wake:
			}
			continue
		}
		v := s.queue[0]
		s.queue[0] = api.LaunchLog{}
		s.queue = s.queue[1:]
		found := false
		for i := range s.coverage {
			q := &s.coverage[i]
			if q.LaunchID == v.LaunchID && q.Source == v.Source {
				found = true
				q.LastSequence = v.Sequence
				if q.State == api.SourcePending {
					q.State = api.SourceActive
				}
			}
		}
		if !found {
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: v.LaunchID, Source: v.Source, State: api.SourceActive, LastSequence: v.Sequence})
		}
		c, cc := context.WithTimeout(s.ctx, s.options.CallbackTimeout)
		active := true
		stopTimer := s.afterFunc(s.options.CallbackTimeout, func() {
			s.mu.Lock()
			var fs []func()
			if active {
				fs = s.finishLocked(api.ListenerSlow, errors.Join(api.ErrLogDelivery, context.DeadlineExceeded))
			}
			s.mu.Unlock()
			for _, f := range fs {
				f()
			}
		})
		// Dispatch commits here, under the same gate as Stop. Context cancellation
		// reaches this committed callback even if scheduling delays its first instruction.
		s.mu.Unlock()
		e := invokeLog(s.listener.OnLog, c, v)
		s.mu.Lock()
		active = false
		stopTimer()
		s.mu.Unlock()
		deadlineMissed := errors.Is(c.Err(), context.DeadlineExceeded)
		cc()
		if deadlineMissed {
			s.finish(api.ListenerSlow, errors.Join(api.ErrLogDelivery, context.DeadlineExceeded))
		}
		if errors.Is(e, api.ErrStopListening) {
			s.Stop()
		} else if e != nil {
			cause := e
			if !errors.Is(e, api.ErrLogDelivery) {
				cause = errors.Join(api.ErrLogDelivery, e)
			}
			s.finish(api.ListenerError, cause)
		}
	}
}
func invokeLog(f func(context.Context, api.LaunchLog) error, c context.Context, v api.LaunchLog) (e error) {
	defer func() {
		if recover() != nil {
			e = api.ErrLogDelivery
		}
	}()
	return f(c, v)
}

// Known non-dispatch cannot leave pending coverage looking observable.
func (s *logSubscription) notStarted() {
	s.mu.Lock()
	if !s.terminal {
		for i := range s.coverage {
			q := &s.coverage[i]
			q.State = api.SourceUnavailable
			q.Cause = api.ErrLogSource
		}
	}
	fs := s.finishLocked(api.ListenerSourceUnavailable, api.ErrLogSource)
	s.mu.Unlock()
	for _, f := range fs {
		f()
	}
}
