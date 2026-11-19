package plugin

import (
	"context"
	"errors"
	"sync"
	"time"
)

type ProviderChangeKind string
type ProviderChangeReason string

const (
	ProviderChangeInitial    ProviderChangeKind = "initial"
	ProviderChangeMembership ProviderChangeKind = "membership"
	ProviderChangeStopped    ProviderChangeKind = "stopped"

	ProviderReasonInitial             ProviderChangeReason = "initial"
	ProviderReasonReady               ProviderChangeReason = "ready"
	ProviderReasonConnectionLost      ProviderChangeReason = "connection_lost"
	ProviderReasonProbeFailed         ProviderChangeReason = "probe_failed"
	ProviderReasonAdministrativeClose ProviderChangeReason = "administrative_close"
	ProviderReasonCollectorClosed     ProviderChangeReason = "collector_closed"
	ProviderReasonCollectorFailed     ProviderChangeReason = "collector_failed"
)

// ProviderChange describes a transition, not present connectivity or capacity.
// Initial Joined is a baseline. Left handles are already retired. Each subscriber
// owns its payload slices; the session-bound provider handles remain shared.
type ProviderChange struct {
	Kind     ProviderChangeKind
	Revision uint64
	Reason   ProviderChangeReason
	Joined   []AvailableProvider
	Left     []AvailableProvider
}

// ProviderChangeSource is optional; ProviderCollector implementations are unchanged.
type ProviderChangeSource interface {
	OnProvidersChanged(context.Context, func(context.Context, ProviderChange)) (*ProviderSubscription, error)
}

var _ ProviderChangeSource = (*Collector)(nil)

// ProviderSubscription delivers callbacks serially. Wait reports delivery loss;
// recovery requires explicit registration with a new baseline, not replay.
type ProviderSubscription struct {
	collector      *Collector
	ctx            context.Context
	callback       func(context.Context, ProviderChange)
	mu             sync.Mutex
	queue          []ProviderChange
	wake           chan struct{}
	done           chan struct{}
	terminal       bool
	result         error
	deadline       time.Time
	inflight       *callbackContext
	timer          *time.Timer
	finalDelivered bool
	stopObserver   func() bool
}

// OnProvidersChanged atomically registers a baseline and subsequent ordered
// deltas. Callbacks may begin before this method returns. ctx governs only this
// observer. A failed registration returns no subscription and invokes nothing.
func (c *Collector) OnProvidersChanged(ctx context.Context, callback func(context.Context, ProviderChange)) (*ProviderSubscription, error) {
	if ctx == nil || callback == nil {
		return nil, ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil, ErrUnavailable
	}
	if len(c.subscriptions) >= c.opts.MaxSubscriptions {
		return nil, ErrSaturated
	}
	s := &ProviderSubscription{collector: c, ctx: ctx, callback: callback, wake: make(chan struct{}, 1), done: make(chan struct{})}
	s.queue = []ProviderChange{{Kind: ProviderChangeInitial, Revision: c.revision, Reason: ProviderReasonInitial, Joined: c.snapshotLocked().Providers}}
	c.subscriptions[s] = struct{}{}
	s.stopObserver = context.AfterFunc(ctx, func() { s.fail(ctx.Err()) })
	// This single worker retains its capacity slot until caller-owned code has
	// actually returned, even if cancellation or a deadline completes Wait first.
	go s.run()
	return s, nil
}

// Cancel detaches without waiting. An already dispatched callback may still run;
// its context is canceled. Calling Wait from that callback would wait on itself.
func (s *ProviderSubscription) Cancel() { s.fail(context.Canceled) }

// Wait returns one stable outcome. It may finish before uncooperative code exits.
func (s *ProviderSubscription) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *ProviderSubscription) finishLocked(e error) {
	if s.terminal {
		return
	}
	s.terminal, s.result = true, e
	s.queue = nil
	if s.timer != nil {
		s.timer.Stop()
	}
	if s.inflight != nil {
		cause := context.Canceled
		if errors.Is(e, context.DeadlineExceeded) {
			cause = context.DeadlineExceeded
		}
		s.inflight.finish(cause)
	}
	close(s.done)
}

func (s *ProviderSubscription) fail(e error) {
	s.mu.Lock()
	s.finishLocked(e)
	s.mu.Unlock()
}

func copyChange(v ProviderChange) ProviderChange {
	v.Joined = append([]AvailableProvider(nil), v.Joined...)
	v.Left = append([]AvailableProvider(nil), v.Left...)
	return v
}

// Called with membership held. No subscriber path holds its own lock while
// acquiring membership, and user code never executes under either lock.
func (c *Collector) publishLocked(v ProviderChange) {
	for s := range c.subscriptions {
		s.mu.Lock()
		if !s.terminal {
			if e := s.ctx.Err(); e != nil {
				s.finishLocked(e)
			} else if len(s.queue) == c.opts.CallbackQueue {
				s.finishLocked(ErrSaturated)
			} else {
				s.queue = append(s.queue, copyChange(v))
				select {
				case s.wake <- struct{}{}:
				default:
				}
			}
		}
		s.mu.Unlock()
	}
}

func callbackDeadlineError() error { return errors.Join(ErrCallback, context.DeadlineExceeded) }

func (s *ProviderSubscription) expiredLocked() error {
	if e := s.ctx.Err(); e != nil {
		return e
	}
	if d, ok := s.ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return callbackDeadlineError()
}

func (s *ProviderSubscription) armLocked(d *callbackContext) {
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer = time.AfterFunc(time.Until(d.limit()), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.inflight == d && !s.terminal {
			s.finishLocked(s.expiredLocked())
		}
	})
}

func (s *ProviderSubscription) drainLocked(deadline time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = deadline
	if s.inflight != nil && !s.terminal {
		s.inflight.tighten(deadline)
		s.armLocked(s.inflight)
	}
}

func callProviderCallback(fn func(context.Context, ProviderChange), ctx context.Context, v ProviderChange) (panicked bool) {
	panicked = true
	defer func() { _ = recover() }()
	fn(ctx, v)
	return false
}

func (s *ProviderSubscription) run() {
	defer s.stopObserver()
	defer func() {
		s.collector.mu.Lock()
		delete(s.collector.subscriptions, s)
		s.collector.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		if e := s.ctx.Err(); e != nil {
			s.finishLocked(e)
		}
		if !s.deadline.IsZero() && !time.Now().Before(s.deadline) {
			s.finishLocked(s.expiredLocked())
		}
		if s.terminal {
			s.mu.Unlock()
			return
		}
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
			case <-s.done:
			}
			continue
		}
		v := s.queue[0]
		copy(s.queue, s.queue[1:])
		s.queue[len(s.queue)-1] = ProviderChange{}
		s.queue = s.queue[:len(s.queue)-1]
		d := &callbackContext{parent: s.ctx, done: make(chan struct{}), deadline: time.Now().Add(s.collector.opts.CallbackTimeout)}
		if deadline, ok := s.ctx.Deadline(); ok {
			d.tighten(deadline)
		}
		if !s.deadline.IsZero() {
			d.tighten(s.deadline)
		}
		s.inflight = d // Dispatch boundary: cancellation before this prevents dispatch.
		s.armLocked(d)
		s.mu.Unlock()

		panicked := callProviderCallback(s.callback, d, v)
		s.mu.Lock()
		s.timer.Stop()
		if !s.terminal {
			switch {
			case panicked:
				s.finishLocked(ErrCallback)
			case s.ctx.Err() != nil:
				s.finishLocked(s.ctx.Err())
			case !time.Now().Before(d.limit()):
				s.finishLocked(s.expiredLocked())
			}
		}
		d.finish(context.Canceled)
		s.inflight = nil
		if s.terminal {
			s.mu.Unlock()
			return
		}
		if v.Kind == ProviderChangeStopped {
			s.finalDelivered = true
			s.mu.Unlock()
			// A delivered final event cannot conceal a later transport failure.
			select {
			case <-s.collector.transportDone:
				s.collector.mu.Lock()
				e := s.collector.result
				s.collector.mu.Unlock()
				s.mu.Lock()
				if s.ctx.Err() != nil {
					s.finishLocked(s.ctx.Err())
				} else if e != nil {
					s.finishLocked(ErrUnavailable)
				} else {
					s.finishLocked(nil)
				}
				s.mu.Unlock()
			case <-s.done:
			}
			return
		}
		s.mu.Unlock()
	}
}

// callbackContext allows shutdown to shorten the deadline of an already running
// invocation. Its timer and ownership live in the subscription, not in user code.
type callbackContext struct {
	parent   context.Context
	mu       sync.Mutex
	done     chan struct{}
	err      error
	deadline time.Time
}

func (c *callbackContext) Deadline() (time.Time, bool) { return c.limit(), true }
func (c *callbackContext) Done() <-chan struct{}       { return c.done }
func (c *callbackContext) Value(key any) any           { return c.parent.Value(key) }
func (c *callbackContext) Err() error                  { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *callbackContext) limit() time.Time            { c.mu.Lock(); defer c.mu.Unlock(); return c.deadline }
func (c *callbackContext) tighten(d time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.Before(c.deadline) {
		c.deadline = d
	}
}
func (c *callbackContext) finish(e error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = e
		close(c.done)
	}
}
