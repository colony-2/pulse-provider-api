package plugin

import (
	"context"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type logRegistration struct {
	request  *wire.LogRegistration
	sub      *logSubscription
	bound    bool
	sendMu   sync.Mutex
	stream   grpc.BidiStreamingServer[wire.ObserveControl, wire.ObserveEvent]
	revision uint64
	limit    int
	ended    bool
}

func (r *logRegistration) send(frames []*wire.ObserveEvent) error {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.ended {
		return api.ErrStopListening
	}
	for _, f := range frames {
		if proto.Size(f) > r.limit {
			return api.ErrLogProtocol
		}
		if e := r.stream.Send(f); e != nil {
			return api.ErrLogTransport
		}
		if f.GetTerminal() != nil {
			r.ended = true
		}
	}
	return nil
}
func (r *logRegistration) sendRecord(_ context.Context, v api.LaunchLog) error {
	f, e := recordFrames(v, r.limit)
	if e != nil {
		return e
	}
	return r.send(f)
}
func (r *logRegistration) sendCoverage(v []api.SourceStatus) error {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.ended {
		return api.ErrStopListening
	}
	if r.revision == ^uint64(0) {
		return api.ErrLogProtocol
	}
	r.revision++
	f, e := coverageFrames(v, r.revision, r.limit)
	if e != nil {
		return e
	}
	for _, q := range f {
		if e = r.stream.Send(q); e != nil {
			return api.ErrLogTransport
		}
	}
	return nil
}
func (s *server) Observe(stream grpc.BidiStreamingServer[wire.ObserveControl, wire.ObserveEvent]) error {
	s.mu.Lock()
	ready := s.negotiated && s.provider != nil && !s.closing
	limit := s.max
	if !ready {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "not ready")
	}
	if s.logStreams == nil {
		s.logStreams = make(chan struct{}, s.opts.LogOptions.MaxSubscriptions)
	}
	select {
	case s.logStreams <- struct{}{}:
	default:
		s.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "observation capacity")
	}
	defer func() { <-s.logStreams }()
	if s.logs == nil {
		s.logs = make(map[uint64]*logRegistration)
	}
	if s.logRetired == nil {
		s.logRetired = make(map[uint64]struct{})
	}
	if s.logPool == nil {
		s.logPool = newLogPool(s.opts.LogOptions)
	}
	pool := s.logPool
	s.mu.Unlock()
	if !ready {
		return status.Error(codes.Unavailable, "not ready")
	}
	// Bound initial receive as well as pending registration lifetime. Closing the
	// handler tears down a gRPC Recv blocked by a peer that never registers.
	received := make(chan struct {
		f *wire.ObserveControl
		e error
	}, 1)
	go func() {
		f, e := stream.Recv()
		received <- struct {
			f *wire.ObserveControl
			e error
		}{f, e}
	}()
	var first *wire.ObserveControl
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-time.After(s.opts.LogOptions.SetupTimeout):
		return status.Error(codes.DeadlineExceeded, "registration")
	case v := <-received:
		if v.e != nil {
			return v.e
		}
		first = v.f
	}
	if proto.Size(first) > limit {
		return status.Error(codes.ResourceExhausted, "message bound")
	}
	v := first.GetRegister()
	if v == nil || v.Invocation == 0 || v.DurationNanos < 0 || !validLogID(string(v.Identity)) || len(v.LaunchIds) < 1 || len(v.LaunchIds) > api.MaxBatch {
		return status.Error(codes.InvalidArgument, "registration")
	}
	launches := make([]api.Launch, len(v.LaunchIds))
	seen := map[string]bool{}
	for i, id := range v.LaunchIds {
		if !validLogID(string(id)) || seen[string(id)] {
			return status.Error(codes.InvalidArgument, "launch")
		}
		seen[string(id)] = true
		launches[i].LaunchID = string(id)
	}
	r := &logRegistration{request: proto.Clone(v).(*wire.LogRegistration), stream: stream, limit: limit}
	s.mu.Lock()
	_, used := s.tokens[v.Invocation]
	_, retired := s.logRetired[v.Invocation]
	if used || retired || s.logs[v.Invocation] != nil || (s.high >= 4096 && v.Invocation <= s.high-4096) || (s.logHigh >= 4096 && v.Invocation <= s.logHigh-4096) || len(s.logs) >= s.opts.LogOptions.MaxSubscriptions {
		s.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "registration unavailable")
	}
	r.sub = newLogSubscription(s.opts.LogOptions, pool, launches, api.ListenOptions{Context: stream.Context(), Duration: time.Duration(v.DurationNanos), OnLog: r.sendRecord})
	s.logs[v.Invocation] = r
	s.mu.Unlock()
	defer func() {
		r.sub.Stop()
		s.mu.Lock()
		if s.logs[v.Invocation] == r {
			delete(s.logs, v.Invocation)
		}
		s.retireLogLocked(v.Invocation)
		s.mu.Unlock()
	}()
	stopSession := context.AfterFunc(s.lifetime, func() { r.sub.finish(api.ListenerProviderClosed, api.ErrProviderClosed) })
	defer stopSession()
	if e := r.send([]*wire.ObserveEvent{{Kind: &wire.ObserveEvent_Ready{Ready: &wire.Empty{}}}}); e != nil {
		return e
	}
	setup := time.AfterFunc(s.opts.LogOptions.SetupTimeout, func() {
		s.mu.Lock()
		bound := r.bound
		s.mu.Unlock()
		if !bound {
			r.sub.notStarted()
		}
	})
	defer setup.Stop()
	go func() {
		f, e := stream.Recv()
		if e != nil {
			r.sub.finish(api.ListenerTransportLost, api.ErrLogTransport)
			return
		}
		if proto.Size(f) > limit || f.GetStop() == nil {
			r.sub.finish(api.ListenerProtocolError, api.ErrLogProtocol)
			return
		}
		r.sub.Stop()
	}()
	<-r.sub.Done()
	result, _ := r.sub.Wait(context.Background())
	if e := r.sendCoverage(result.Coverage); e != nil {
		return e
	}
	cause, ok := terminalCause(result.Reason)
	if !ok {
		return api.ErrLogProtocol
	}
	return r.send([]*wire.ObserveEvent{{Kind: &wire.ObserveEvent_Terminal{Terminal: &wire.LogTerminal{Reason: string(result.Reason), Cause: logCauseCode(cause)}}}})
}
func (s *server) retireLogLocked(id uint64) {
	if s.logRetired == nil {
		s.logRetired = make(map[uint64]struct{})
	}
	if id > s.logHigh {
		s.logHigh = id
	}
	s.logRetired[id] = struct{}{}
	if s.logHigh >= 4096 {
		for k := range s.logRetired {
			if k <= s.logHigh-4096 {
				delete(s.logRetired, k)
			}
		}
	}
}
func (s *server) bindLogs(r *wire.SubmitRequest, ls []api.Launch) (api.SubmitOptions, *logRegistration, error) {
	if !r.Listen {
		if r.DurationNanos != 0 || len(r.Registration) != 0 || r.Stopped {
			return api.SubmitOptions{}, nil, ErrInvalid
		}
		return api.SubmitOptions{}, nil, nil
	}
	if r.DurationNanos < 0 || !validLogID(string(r.Registration)) {
		return api.SubmitOptions{}, nil, ErrInvalid
	}
	s.mu.Lock()
	reg := s.logs[r.Invocation]
	valid := reg != nil && !reg.bound && string(reg.request.Identity) == string(r.Registration) && reg.request.DurationNanos == r.DurationNanos && len(reg.request.LaunchIds) == len(ls)
	if valid {
		for i, l := range ls {
			if string(reg.request.LaunchIds[i]) != l.LaunchID {
				valid = false
				break
			}
		}
	}
	if valid {
		reg.bound = true
	}
	s.mu.Unlock()
	if !valid || r.Stopped {
		if valid {
			reg.sub.Stop()
		}
		c, cancel := context.WithCancel(context.Background())
		cancel()
		return api.SubmitOptions{Listener: &api.ListenOptions{Context: c, Duration: time.Duration(r.DurationNanos), OnLog: func(context.Context, api.LaunchLog) error { return api.ErrStopListening }}}, nil, nil
	}
	return api.SubmitOptions{Listener: &api.ListenOptions{Context: reg.sub.ctx, Duration: time.Duration(r.DurationNanos), OnLog: func(_ context.Context, v api.LaunchLog) error { return reg.sub.enqueue(v) }}}, reg, nil
}
func (s *server) watchNative(reg *logRegistration, native api.LogSubscription) {
	if native == nil {
		if reg != nil {
			reg.sub.finish(api.ListenerProtocolError, api.ErrLogProtocol)
		}
		return
	}
	if reg == nil {
		native.Stop()
		return
	}
	reg.sub.addCleanup(native.Stop)
	publish := func() bool {
		v := native.Coverage()
		if e := reg.sub.setCoverage(v); e != nil {
			reg.sub.finish(api.ListenerProtocolError, api.ErrLogProtocol)
			return false
		}
		if e := reg.sendCoverage(reg.sub.Coverage()); e != nil {
			reg.sub.finish(api.ListenerTransportLost, api.ErrLogTransport)
			return false
		}
		return true
	}
	go func() {
		if !publish() {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reg.sub.Done():
				return
			case <-ticker.C:
				if !publish() {
					return
				}
			case <-native.Done():
				r, e := native.Wait(reg.sub.ctx)
				if _, ok := terminalCause(r.Reason); !ok {
					reg.sub.finish(api.ListenerProtocolError, api.ErrLogProtocol)
					return
				}
				if e != nil && r.Cause == nil {
					r.Cause = e
				}
				if e = reg.sub.setCoverage(r.Coverage); e != nil {
					reg.sub.finish(api.ListenerProtocolError, api.ErrLogProtocol)
					return
				}
				// Preserve stable categories; native arbitrary errors never cross the wire.
				r.Cause, _ = terminalCause(r.Reason)
				reg.sub.complete(r)
				return
			}
		}
	}()
}

// Validation failures retire only the exact unconsumed registration. A foreign
// request must not disturb an independently valid listener.
func (s *server) rejectLogRequest(req *wire.SubmitRequest) {
	s.mu.Lock()
	r := s.logs[req.Invocation]
	valid := r != nil && !r.bound && string(r.request.Identity) == string(req.Registration)
	if valid {
		r.bound = true
	}
	s.mu.Unlock()
	if valid {
		r.sub.notStarted()
	}
}
