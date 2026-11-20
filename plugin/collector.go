package plugin

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
)

// AvailableProvider is a session-bound candidate, not a capacity reservation.
type AvailableProvider struct {
	ID         string
	Generation uint64
	Provider   api.Provider
}
type ProviderSnapshot struct {
	Revision  uint64
	Providers []AvailableProvider
}

// ProviderCollector supplies fresh membership to each host invocation.
type ProviderCollector interface {
	Snapshot() (ProviderSnapshot, error)
	WaitForChange(context.Context, uint64) (ProviderSnapshot, error)
}

// Collector owns its listener on successful startup. It collects membership;
// scheduling, admission policy, and accepted native work belong to the host/provider.
type Collector struct {
	opts                 CollectorOptions
	logs                 *logPool
	listener             net.Listener
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	stopped              bool
	revision, generation uint64
	change               chan struct{}
	waiters              int
	ready                map[string]*Connection
	reserved             map[string]*Connection
	sockets              map[*sessionSocket]struct{}
	pending              chan struct{}
	done                 chan struct{}
	transportDone        chan struct{}
	subscriptions        map[*ProviderSubscription]struct{}
	stopOnce             sync.Once
	result               error
}

var _ ProviderCollector = (*Collector)(nil)

// StartCollector starts immediately, even with zero providers. Ownership of
// listener transfers only on success. ctx governs the entire collector lifetime.
func StartCollector(ctx context.Context, listener net.Listener, options CollectorOptions) (*Collector, error) {
	if ctx == nil || listener == nil {
		return nil, ErrInvalid
	}
	if e := options.defaults(); e != nil {
		return nil, e
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	c := &Collector{opts: options, listener: listener, revision: 1, change: make(chan struct{}), ready: map[string]*Connection{}, reserved: map[string]*Connection{}, sockets: map[*sessionSocket]struct{}{}, pending: make(chan struct{}, options.MaxPendingConnections), done: make(chan struct{}), transportDone: make(chan struct{}), subscriptions: map[*ProviderSubscription]struct{}{}}
	c.ctx, c.cancel = context.WithCancel(ctx)
	go c.accept()
	go func() { <-c.ctx.Done(); c.stop(nil) }()
	return c, nil
}
func (c *Collector) snapshotLocked() ProviderSnapshot {
	s := ProviderSnapshot{Revision: c.revision, Providers: make([]AvailableProvider, 0, len(c.ready))}
	for id, p := range c.ready {
		s.Providers = append(s.Providers, AvailableProvider{ID: id, Generation: p.g.id, Provider: p})
	}
	sort.Slice(s.Providers, func(i, j int) bool { return s.Providers[i].ID < s.Providers[j].ID })
	return s
}
func (c *Collector) Snapshot() (ProviderSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return ProviderSnapshot{}, ErrUnavailable
	}
	return c.snapshotLocked(), nil
}
func (c *Collector) WaitForChange(ctx context.Context, revision uint64) (ProviderSnapshot, error) {
	if ctx == nil {
		return ProviderSnapshot{}, ErrInvalid
	}
	c.mu.Lock()
	if e := ctx.Err(); e != nil {
		c.mu.Unlock()
		return ProviderSnapshot{}, e
	}
	if c.stopped {
		c.mu.Unlock()
		return ProviderSnapshot{}, ErrUnavailable
	}
	if revision > c.revision {
		c.mu.Unlock()
		return ProviderSnapshot{}, ErrInvalid
	}
	if revision < c.revision {
		s := c.snapshotLocked()
		c.mu.Unlock()
		return s, nil
	}
	if c.waiters >= c.opts.MaxWaiters {
		c.mu.Unlock()
		return ProviderSnapshot{}, ErrSaturated
	}
	c.waiters++
	ch := c.change
	c.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-ch:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiters--
	if c.stopped {
		return ProviderSnapshot{}, ErrUnavailable
	}
	if e := ctx.Err(); e != nil {
		return ProviderSnapshot{}, e
	}
	return c.snapshotLocked(), nil
}
func (c *Collector) changedLocked() bool {
	if c.revision == math.MaxUint64 {
		go c.stop(ErrUnavailable)
		return false
	}
	c.revision++
	close(c.change)
	c.change = make(chan struct{})
	return true
}
func (c *Collector) accept() {
	for {
		raw, e := c.listener.Accept()
		if e != nil {
			c.stop(ErrUnavailable)
			return
		}
		select {
		case c.pending <- struct{}{}:
		default:
			_ = raw.Close()
			continue
		}
		socket := newSessionSocket(raw)
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			<-c.pending
			_ = socket.Close()
			return
		}
		c.sockets[socket] = struct{}{}
		c.mu.Unlock()
		go c.admit(socket)
	}
}
func (c *Collector) admit(socket *sessionSocket) {
	defer func() { <-c.pending }()
	// This timer closes transport even when caller-owned authorization ignores ctx.
	ctx, cancel := context.WithTimeout(c.ctx, c.opts.StartupTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()
	_ = socket.SetDeadline(time.Now().Add(c.opts.StartupTimeout))
	var tc net.Conn = socket
	handed := false
	defer func() {
		if !handed {
			_ = socket.Close()
			c.mu.Lock()
			delete(c.sockets, socket)
			c.mu.Unlock()
		}
	}()
	var state tls.ConnectionState
	if c.opts.Transport == ReverseTransportTLS {
		secured := tls.Server(socket, c.opts.TLSConfig)
		if secured.HandshakeContext(ctx) != nil || secured.ConnectionState().NegotiatedProtocol != reverseProtocol(c.opts.AuthMode) {
			return
		}
		state = secured.ConnectionState()
		tc = secured
	}
	var id string
	var e error
	if c.opts.AuthMode == ReverseAuthMutualKey {
		var key []byte
		var deny bool
		id, key, deny, e = readKeyRequest(tc)
		if e != nil {
			if deny {
				_ = writeAdmission(tc, 1, "")
			}
			return
		}
		if !c.opts.matchesKey(id, key) {
			_ = writeAdmission(tc, 1, "")
			return
		}
	} else {
		id, e = c.opts.Authorize(ctx, state)
	}
	if ctx.Err() != nil {
		return
	}
	if e != nil || !validIdentity(id) {
		_ = writeAdmission(tc, 1, "")
		return
	}
	c.mu.Lock()
	status := byte(0)
	if c.stopped {
		status = 1
	} else if c.reserved[id] != nil {
		status = 2
	} else if len(c.reserved) >= c.opts.MaxProviders {
		status = 3
	} else if c.generation == math.MaxUint64 {
		status = 4
	}
	if status != 0 {
		c.mu.Unlock()
		_ = writeAdmission(tc, status, "")
		if status == 4 {
			c.stop(ErrUnavailable)
		}
		return
	}
	c.generation++
	g := &generation{id: c.generation, limit: c.opts.MaxMessageBytes}
	// Retirement owns generation cancellation, so a parent cancellation cannot
	// remove a peer before collector shutdown captures it for disposal.
	g.ctx, g.cancel = context.WithCancel(context.Background())
	p := &Connection{sessionCaller: sessionCaller{opts: StartOptions{CallTimeout: c.opts.CallTimeout, LogOptions: c.opts.LogOptions}, sharedLogs: c.logsPool, permits: make(chan struct{}, c.opts.Concurrency), ctx: g.ctx, cancel: g.cancel}, collector: c, id: id, g: g, socket: socket, done: make(chan struct{})}
	c.reserved[id] = p
	c.mu.Unlock()
	success := false
	defer func() {
		if !success {
			p.retire(ProviderReasonConnectionLost)
			_ = p.dispose(ctx)
		}
	}()
	if writeAdmission(tc, 0, id) != nil {
		return
	}
	adopted := newSessionSocket(tc)
	conn, e := adoptSocket(adopted, g.limit, c.opts.MaxMetadataBytes)
	if e != nil {
		return
	}
	g.conn = conn
	g.rpc = wire.NewProviderClient(conn)
	n, e := g.rpc.Negotiate(ctx, &wire.Negotiation{Protocol: Protocol, Required: semantics, MaxMessageBytes: int64(g.limit), ShutdownNanos: int64(c.opts.ShutdownTimeout)})
	if e != nil || n.MaxMessageBytes < 1024 || n.MaxMessageBytes > int64(g.limit) {
		return
	}
	g.limit = int(n.MaxMessageBytes)
	g.implementation = string(n.Implementation)
	g.version = string(n.Version)
	r, e := g.rpc.Initialize(ctx, &wire.Initialization{Name: []byte(id)}, grpc.MaxCallRecvMsgSize(g.limit), grpc.MaxCallSendMsgSize(g.limit))
	if e != nil || r.Error != nil || ctx.Err() != nil {
		return
	}
	if !stop() || ctx.Err() != nil {
		return
	}
	_ = socket.SetDeadline(time.Time{})
	c.mu.Lock()
	if c.stopped || c.reserved[id] != p || !c.changedLocked() {
		c.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.current = g
	p.mu.Unlock()
	c.ready[id] = p
	c.publishLocked(ProviderChange{Kind: ProviderChangeMembership, Revision: c.revision, Reason: ProviderReasonReady, Joined: []AvailableProvider{{ID: id, Generation: g.id, Provider: p}}})
	c.mu.Unlock()
	handed = true
	success = true
	go p.monitor()
}
func (c *Collector) stop(cause error) {
	c.stopOnce.Do(func() {
		deadline := time.Now().Add(c.opts.ShutdownTimeout)
		c.mu.Lock()
		c.stopped = true
		c.result = cause
		close(c.change)
		left := c.snapshotLocked().Providers
		peers := make([]*Connection, 0, len(c.reserved))
		for _, p := range c.reserved {
			p.logClosing.Store(true)
			p.invalidate()
		}
		for _, p := range c.ready {
			if c.ready[p.id] == p {
				peers = append(peers, p)
			}
		}
		sockets := make([]*sessionSocket, 0, len(c.sockets))
		for s := range c.sockets {
			sockets = append(sockets, s)
		}
		c.ready = map[string]*Connection{}
		c.reserved = map[string]*Connection{}
		subscriptions := make([]*ProviderSubscription, 0, len(c.subscriptions))
		for s := range c.subscriptions {
			subscriptions = append(subscriptions, s)
			s.drainLocked(deadline)
		}
		if c.revision == math.MaxUint64 {
			c.result = ErrUnavailable
			for _, s := range subscriptions {
				s.fail(ErrUnavailable)
			}
		} else {
			c.revision++
			reason := ProviderReasonCollectorClosed
			if cause != nil {
				reason = ProviderReasonCollectorFailed
			}
			c.publishLocked(ProviderChange{Kind: ProviderChangeStopped, Revision: c.revision, Reason: reason, Left: left})
		}
		c.mu.Unlock()
		c.cancel()
		_ = c.listener.Close()
		go func() {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var cleanup error
			for _, p := range peers {
				wg.Add(1)
				go func(p *Connection) {
					defer wg.Done()
					if p.dispose(ctx) != nil {
						mu.Lock()
						cleanup = ErrCleanup
						mu.Unlock()
					}
				}(p)
			}
			finished := make(chan struct{})
			go func() { wg.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-ctx.Done():
				mu.Lock()
				cleanup = ErrCleanup
				mu.Unlock()
			}
			for _, s := range sockets {
				_ = s.Close()
			}
			mu.Lock()
			e := cleanup
			mu.Unlock()
			c.mu.Lock()
			if e != nil {
				c.result = e
			}
			c.mu.Unlock()
			close(c.transportDone)
			for _, s := range subscriptions {
				select {
				case <-s.done:
				case <-ctx.Done():
					s.mu.Lock()
					if s.ctx.Err() != nil {
						s.finishLocked(s.ctx.Err())
					} else if s.finalDelivered {
						if e != nil || cause != nil {
							s.finishLocked(ErrUnavailable)
						} else {
							s.finishLocked(nil)
						}
					} else {
						s.finishLocked(callbackDeadlineError())
					}
					s.mu.Unlock()
				}
			}
			close(c.done)
		}()
	})
}

// Close shares one bounded completion among all callers.
func (c *Collector) Close() error { c.stop(nil); return c.Wait() }
func (c *Collector) Wait() error  { <-c.done; c.mu.Lock(); defer c.mu.Unlock(); return c.result }

// The pool belongs to the collector, so reconnects cannot replace stuck workers.
func (c *Collector) logsPool() *logPool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logs == nil {
		c.logs = newLogPool(c.opts.LogOptions)
	}
	return c.logs
}
