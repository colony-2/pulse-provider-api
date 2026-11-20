package plugin

import (
	"context"
	"fmt"
	"io"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (p *sessionCaller) logsPool() (*logPool, LogOptions) {
	if p.sharedLogs != nil {
		o := p.opts.LogOptions
		_ = o.defaults()
		return p.sharedLogs(), o
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	o := p.opts.LogOptions
	_ = o.defaults()
	if p.logs == nil {
		p.logs = newLogPool(o)
	}
	return p.logs, o
}
func (p *sessionCaller) armLogs(admission context.Context, g *generation, req *wire.SubmitRequest, s *logSubscription, ls []api.Launch) {
	req.Listen = true
	req.DurationNanos = int64(s.listener.Duration)
	req.Registration = []byte(fmt.Sprintf("s-%d-%d", g.id, req.Invocation))
	select {
	case <-s.done:
		req.Stopped = true
		return
	default:
	}
	c, cancel := context.WithCancel(g.ctx)
	s.addCleanup(cancel)
	// A session loss has a different meaning from caller-selected log cancellation.
	s.addCleanupStop(context.AfterFunc(g.ctx, func() { p.logSessionEnded(s) }))
	reg := &wire.LogRegistration{Invocation: req.Invocation, DurationNanos: req.DurationNanos, Identity: req.Registration}
	for _, l := range ls {
		reg.LaunchIds = append(reg.LaunchIds, []byte(l.LaunchID))
	}
	control := &wire.ObserveControl{Kind: &wire.ObserveControl_Register{Register: reg}}
	if proto.Size(control) > g.limit {
		s.finish(api.ListenerProtocolError, api.ErrLogProtocol)
		req.Stopped = true
		return
	}
	ready := make(chan error, 1)
	go func() {
		stream, e := g.rpc.Observe(c, callOptions(g)...)
		if e != nil {
			ready <- logSetupCause(e)
			return
		}
		if e = stream.Send(control); e != nil {
			// Send can report EOF before the remote rejection status arrives.
			if e == io.EOF {
				_, e = stream.Recv()
			}
			ready <- logSetupCause(e)
			return
		}
		reader := frameReader{}
		first, e := stream.Recv()
		if e != nil {
			ready <- logSetupCause(e)
			return
		}
		if first.GetReady() == nil || proto.Size(first) > g.limit {
			ready <- api.ErrLogProtocol
			return
		}
		if e = reader.accept(first, s); e != nil {
			ready <- e
			return
		}
		ready <- nil
		for {
			f, e := stream.Recv()
			if e != nil {
				if !reader.terminal {
					p.logSessionEnded(s)
				}
				return
			}
			if proto.Size(f) > g.limit || reader.accept(f, s) != nil {
				// Cancellation caused by our terminal decision is not a remote failure.
				select {
				case <-s.done:
					return
				default:
					s.finish(api.ListenerProtocolError, api.ErrLogProtocol)
				}
				return
			}
		}
	}()
	timer := time.NewTimer(s.options.SetupTimeout)
	defer timer.Stop()
	select {
	case e := <-ready:
		if e != nil {
			reason := api.ListenerTransportLost
			if e == api.ErrLogProtocol {
				reason = api.ListenerProtocolError
			} else if e == api.ErrLogCapacity {
				reason = api.ListenerCapacityExceeded
			}
			s.finish(reason, e)
		}
	case <-s.done:
	case <-admission.Done():
		s.finish(api.ListenerSourceUnavailable, api.ErrLogSource)
	case <-timer.C:
		p.logSessionEnded(s)
	}
	select {
	case <-s.done:
		req.Stopped = true
		cancel()
	default:
	}
}

func logSetupCause(e error) error {
	if status.Code(e) == codes.ResourceExhausted {
		return api.ErrLogCapacity
	}
	return api.ErrLogTransport
}

func (p *sessionCaller) logSessionEnded(s *logSubscription) {
	if p.logClosing.Load() {
		s.finish(api.ListenerProviderClosed, api.ErrProviderClosed)
	} else {
		s.finish(api.ListenerTransportLost, api.ErrLogTransport)
	}
}
