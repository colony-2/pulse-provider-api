package plugin

import (
	"context"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (p *Process) acquire(ctx context.Context) (*generation, context.Context, func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, nil, nil, e
	}
	select {
	case p.permits <- struct{}{}:
	default:
		return nil, nil, nil, ErrSaturated
	}
	p.mu.Lock()
	g := p.current
	closing := p.ctx.Err() != nil
	p.mu.Unlock()
	if g == nil || closing {
		<-p.permits
		return nil, nil, nil, ErrUnavailable
	}
	c, cc := context.WithTimeout(ctx, p.opts.CallTimeout)
	stop := context.AfterFunc(g.ctx, cc)
	return g, c, func() { stop(); cc(); <-p.permits }, nil
}
func (p *Process) beforeDispatch(ctx context.Context, g *generation, m proto.Message) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if proto.Size(m) > g.limit {
		return ErrTooLarge
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil || p.current != g {
		return ErrUnavailable
	}
	return nil
}
func callOptions(g *generation) []grpc.CallOption {
	return []grpc.CallOption{grpc.MaxCallRecvMsgSize(g.limit), grpc.MaxCallSendMsgSize(g.limit), grpc.WaitForReady(false)}
}
func (p *Process) GetCapabilities(ctx context.Context) (api.Capabilities, error) {
	g, c, release, e := p.acquire(ctx)
	if e != nil {
		return api.Capabilities{}, e
	}
	defer release()
	req := &wire.Empty{}
	if e = p.beforeDispatch(c, g, req); e != nil {
		return api.Capabilities{}, e
	}
	r, e := g.rpc.GetCapabilities(c, req, callOptions(g)...)
	if e != nil {
		return api.Capabilities{}, callError(c, e)
	}
	return fromCaps(r), fromError(r.Error)
}
func (p *Process) List(ctx context.Context, q api.ListRequest) (api.ListResponse, error) {
	if e := validateList(&q); e != nil {
		return api.ListResponse{}, e
	}
	g, c, release, e := p.acquire(ctx)
	if e != nil {
		return api.ListResponse{}, e
	}
	defer release()
	req := &wire.ListRequest{PageSize: int32(q.PageSize), PageToken: []byte(q.PageToken), LaunchId: []byte(q.LaunchID)}
	if e = p.beforeDispatch(c, g, req); e != nil {
		return api.ListResponse{}, e
	}
	r, e := g.rpc.List(c, req, callOptions(g)...)
	if e != nil {
		return api.ListResponse{}, callError(c, e)
	}
	v, e := fromList(r)
	if e != nil {
		return api.ListResponse{}, e
	}
	return v, fromError(r.Error)
}
func (p *Process) Submit(ctx context.Context, ls []api.Launch) ([]api.Submission, error) {
	r, _, e := p.SubmitDetailed(ctx, ls)
	return r, e
}
func (p *Process) SubmitDetailed(ctx context.Context, ls []api.Launch) ([]api.Submission, SubmissionObservations, error) {
	id := p.sequence.Add(1)
	o := SubmissionObservations{Invocation: id, Capture: "not_captured"}
	if e := validateLaunches(ls); e != nil {
		return nonStart(ls, api.Rejected, e), o, e
	}
	req := &wire.SubmitRequest{Invocation: id}
	for _, l := range ls {
		v, e := toLaunch(l)
		if e != nil {
			return nonStart(ls, api.Rejected, e), o, e
		}
		req.Launches = append(req.Launches, v)
	}
	g, c, release, e := p.acquire(ctx)
	if e != nil {
		return nonStart(ls, api.Unavailable, safeError(ErrUnavailable, e)), o, e
	}
	defer release()
	o.Generation = g.id
	if e = p.beforeDispatch(c, g, req); e != nil {
		return nonStart(ls, api.Unavailable, safeError(ErrUnavailable, e)), o, e
	}
	// This is the possible-dispatch boundary. Transport failure can never prove
	// non-start. Never resend this call on this or a replacement generation.
	r, e := g.rpc.Submit(c, req, callOptions(g)...)
	if e != nil {
		return nonStart(ls, api.Unknown, ErrTransport), o, callError(c, e)
	}
	ss := make([]api.Submission, len(r.Results))
	for i, s := range r.Results {
		ss[i] = fromSub(s)
	}
	o = observations(id, g.id, ss, r.Error)
	out, bad := normalize(ls, ss)
	return out, o, malformedError(bad, fromError(r.Error))
}
func callError(ctx context.Context, e error) error {
	if ctx.Err() != nil {
		return safeError(ErrTransport, ctx.Err())
	}
	switch status.Code(e) {
	case codes.Canceled:
		return safeError(ErrTransport, context.Canceled)
	case codes.DeadlineExceeded:
		return safeError(ErrTransport, context.DeadlineExceeded)
	}
	return ErrTransport
}
