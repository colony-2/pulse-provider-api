package plugin

import (
	"context"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
)

// Connection is permanently bound to one authenticated provider generation.
// Invocation callers retain it without closing it; administrative Close retires it.
type Connection struct {
	sessionCaller
	collector *Collector
	id        string
	g         *generation
	socket    *sessionSocket
	closeOnce sync.Once
	done      chan struct{}
	closeErr  error
}

var _ api.Provider = (*Connection)(nil)

func (p *Connection) invalidate() { p.mu.Lock(); p.current = nil; p.cancel(); p.mu.Unlock() }
func (p *Connection) retire(reason ProviderChangeReason) {
	c := p.collector
	c.mu.Lock()
	defer c.mu.Unlock()
	if reason == ProviderReasonAdministrativeClose {
		p.mu.Lock()
		if p.current != nil {
			p.logClosing.Store(true)
		}
		p.mu.Unlock()
	}
	p.invalidate()
	if c.reserved[p.id] == p {
		delete(c.reserved, p.id)
	}
	if c.ready[p.id] == p {
		delete(c.ready, p.id)
		if !c.stopped {
			if c.changedLocked() {
				c.publishLocked(ProviderChange{Kind: ProviderChangeMembership, Revision: c.revision, Reason: reason, Left: []AvailableProvider{{ID: p.id, Generation: p.g.id, Provider: p}}})
			}
		}
	}
}
func (p *Connection) Health() Health {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := "unavailable"
	if p.current != nil {
		state = "ready"
	}
	return Health{Name: p.id, Implementation: p.g.implementation, Version: p.g.version, Protocol: Protocol, State: state, Generation: p.g.id}
}
func (p *Connection) dispose(ctx context.Context) error {
	p.closeOnce.Do(func() {
		go func() {
			if p.g.rpc != nil {
				// Leave part of the shared budget for transport disposal.
				deadline, _ := ctx.Deadline()
				grace := time.Until(deadline) * 4 / 5
				c, cc := context.WithTimeout(ctx, grace)
				_, e := p.g.rpc.Control(c, &wire.ControlRequest{Shutdown: true})
				cc()
				if e != nil {
					p.closeErr = ErrCleanup
				}
			}
			if p.g.conn != nil {
				_ = p.g.conn.Close()
			}
			_ = p.socket.Close()
			p.collector.mu.Lock()
			delete(p.collector.sockets, p.socket)
			p.collector.mu.Unlock()
			close(p.done)
		}()
	})
	select {
	case <-p.done:
		return p.closeErr
	case <-ctx.Done():
		_ = p.socket.Close()
		return ErrCleanup
	}
}
func (p *Connection) Close() error {
	p.retire(ProviderReasonAdministrativeClose)
	ctx, cancel := context.WithTimeout(context.Background(), p.collector.opts.ShutdownTimeout)
	defer cancel()
	return p.dispose(ctx)
}
func (p *Connection) monitor() {
	ticker := time.NewTicker(p.collector.opts.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.g.ctx.Done():
			p.retire(ProviderReasonConnectionLost)
			return
		case <-p.socket.done:
			p.retire(ProviderReasonConnectionLost)
			_ = p.Close()
			return
		case <-ticker.C:
			ctx, cc := context.WithTimeout(p.g.ctx, p.collector.opts.ProbeTimeout)
			_, e := p.g.rpc.Control(ctx, &wire.ControlRequest{})
			cc()
			if e != nil {
				p.retire(ProviderReasonProbeFailed)
				_ = p.Close()
				return
			}
		}
	}
}
