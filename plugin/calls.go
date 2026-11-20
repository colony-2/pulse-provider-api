package plugin

import (
	"context"
	"sync"
	"sync/atomic"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// sessionCaller holds the transport-independent call safeguards. A Process may
// replace its current generation; a Connection permanently owns just one.
type sessionCaller struct {
	opts       StartOptions
	mu         sync.Mutex
	current    *generation
	ctx        context.Context
	cancel     context.CancelFunc
	permits    chan struct{}
	sequence   atomic.Uint64
	logClosing atomic.Bool
	logs       *logPool
	sharedLogs func() *logPool
}

func (p *sessionCaller) acquire(ctx context.Context) (*generation, context.Context, func(), error) {
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
func (p *sessionCaller) beforeDispatch(ctx context.Context, g *generation, m proto.Message) error {
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
func (p *sessionCaller) GetCapabilities(ctx context.Context) (api.Capabilities, error) {
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
func (p *sessionCaller) List(ctx context.Context, q api.ListRequest) (api.ListResponse, error) {
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
func (p *sessionCaller) Submit(ctx context.Context, ls []api.Launch, options api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	r, _, sub, e := p.SubmitDetailed(ctx, ls, options)
	return r, sub, e
}
func (p *sessionCaller) SubmitDetailed(ctx context.Context, ls []api.Launch, options api.SubmitOptions) ([]api.Submission, SubmissionObservations, api.LogSubscription, error) {
	id := p.sequence.Add(1)
	o := SubmissionObservations{Invocation: id, Capture: "not_captured"}
	if e := validateLaunches(ls); e != nil {
		return nonStart(ls, api.Rejected, e), o, nil, e
	}
	if e := validateListener(options); e != nil {
		return nonStart(ls, api.Rejected, e), o, nil, e
	}
	req := &wire.SubmitRequest{Invocation: id}
	for _, l := range ls {
		v, e := toLaunch(l)
		if e != nil {
			return nonStart(ls, api.Rejected, e), o, nil, e
		}
		req.Launches = append(req.Launches, v)
	}
	var sub *logSubscription
	var handle api.LogSubscription
	if options.Listener != nil {
		pool, limits := p.logsPool()
		sub = newLogSubscription(limits, pool, ls, *options.Listener)
		handle = sub
	}
	notStarted := func() {
		if sub != nil {
			sub.notStarted()
		}
	}
	g, c, release, e := p.acquire(ctx)
	if e != nil {
		notStarted()
		return nonStart(ls, api.Unavailable, safeError(ErrUnavailable, e)), o, handle, e
	}
	defer release()
	o.Generation = g.id
	if sub != nil {
		p.armLogs(c, g, req, sub, ls)
	}
	if e = p.beforeDispatch(c, g, req); e != nil {
		notStarted()
		return nonStart(ls, api.Unavailable, safeError(ErrUnavailable, e)), o, handle, e
	}
	// The sole possible-dispatch boundary. Logging never causes a retry.
	r, e := g.rpc.Submit(c, req, callOptions(g)...)
	if e != nil {
		return nonStart(ls, api.Unknown, ErrTransport), o, handle, callError(c, e)
	}
	ss := make([]api.Submission, len(r.Results))
	for i, s := range r.Results {
		ss[i] = fromSub(s)
	}
	o = observations(id, g.id, ss, r.Error)
	out, bad := normalize(ls, ss)
	return out, o, handle, malformedError(bad, fromError(r.Error))
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
