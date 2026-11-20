package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func logHarness(t *testing.T, options LogOptions) (*sessionCaller, *server, *reverseProvider) {
	t.Helper()
	if e := options.defaults(); e != nil {
		t.Fatal(e)
	}
	native := &reverseProvider{}
	lifetime, cancel := context.WithCancel(context.Background())
	s := &server{opts: ServeOptions{LogOptions: options}, negotiated: true, provider: native, max: 1024, shutdown: time.Second, lifetime: lifetime, cancel: cancel, permits: make(chan struct{}, 4), tokens: map[uint64]struct{}{}}
	listener := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.UnaryInterceptor(s.intercept))
	wire.RegisterProviderServer(gs, s)
	go gs.Serve(listener)
	conn, e := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if e != nil {
		t.Fatal(e)
	}
	g := &generation{id: 1, limit: 1024, rpc: wire.NewProviderClient(conn), ctx: lifetime, cancel: cancel}
	p := &sessionCaller{opts: StartOptions{CallTimeout: time.Second, LogOptions: options}, current: g, ctx: lifetime, cancel: cancel, permits: make(chan struct{}, 4)}
	t.Cleanup(func() { cancel(); gs.Stop(); _ = conn.Close(); _ = listener.Close() })
	return p, s, native
}
func observe(t *testing.T, p *sessionCaller, id uint64, ids ...string) (wire.Provider_ObserveClient, context.CancelFunc) {
	t.Helper()
	c, cc := context.WithCancel(context.Background())
	stream, e := p.current.rpc.Observe(c)
	if e != nil {
		cc()
		t.Fatal(e)
	}
	r := &wire.LogRegistration{Invocation: id, Identity: []byte(fmt.Sprintf("reg-%d", id))}
	for _, id := range ids {
		r.LaunchIds = append(r.LaunchIds, []byte(id))
	}
	if e = stream.Send(&wire.ObserveControl{Kind: &wire.ObserveControl_Register{Register: r}}); e != nil {
		cc()
		t.Fatal(e)
	}
	return stream, cc
}
func TestLogServerRegistrationAndFences(t *testing.T) {
	p, s, native := logHarness(t, LogOptions{SetupTimeout: 100 * time.Millisecond})
	stream, cancel := observe(t, p, 1, "one")
	defer cancel()
	f, e := stream.Recv()
	if e != nil || f.GetReady() == nil {
		t.Fatal(f, e)
	}
	if native.calls.Load() != 0 {
		t.Fatal("registration launched work")
	}
	dup, cc := observe(t, p, 1, "one")
	defer cc()
	if _, e := dup.Recv(); e == nil {
		t.Fatal("duplicate registration accepted")
	}
	native.submitWithLogs = func(ctx context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		if o.Listener == nil || o.Listener.Context.Err() != nil {
			t.Error("live listener not bound")
		}
		h := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
		_ = h.enqueue(logRecord(1))
		return []api.Submission{{LaunchID: "one", Status: api.Accepted}}, h, nil
	}
	l, _ := toLaunch(launches("one")[0])
	req := &wire.SubmitRequest{Invocation: 1, Launches: []*wire.Launch{l}, Listen: true, Registration: []byte("reg-1")}
	if _, e := p.current.rpc.Submit(context.Background(), req); e != nil {
		t.Fatal(e)
	}
	if native.calls.Load() != 1 {
		t.Fatal(native.calls.Load())
	}
	if _, e := p.current.rpc.Submit(context.Background(), req); e == nil {
		t.Fatal("admission replayed")
	}
	cancel()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.logs) == 0 })
	late, cc := observe(t, p, 1, "one")
	defer cc()
	if _, e := late.Recv(); e == nil {
		t.Fatal("consumed registration resurrected")
	}
}
func TestLogServerMissingStoppedAndExpiredRegistration(t *testing.T) {
	for _, mode := range []string{"missing", "stop", "expire", "foreign", "mismatch"} {
		t.Run(mode, func(t *testing.T) {
			p, s, native := logHarness(t, LogOptions{SetupTimeout: 20 * time.Millisecond})
			if mode != "missing" {
				stream, cc := observe(t, p, 1, "one")
				defer cc()
				if f, e := stream.Recv(); e != nil || f.GetReady() == nil {
					t.Fatal(f, e)
				}
				if mode == "stop" {
					_ = stream.Send(&wire.ObserveControl{Kind: &wire.ObserveControl_Stop{Stop: &wire.Empty{}}})
				}
				if mode == "expire" || mode == "stop" {
					for {
						f, e := stream.Recv()
						if e != nil || f.GetTerminal() != nil {
							break
						}
					}
					eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.logs) == 0 })
				}
			}
			native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
				if o.Listener == nil || o.Listener.Context.Err() == nil {
					t.Error("invalid registration retained live logging")
				}
				return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Accepted}}, nil, nil
			}
			id := "one"
			identity := "reg-1"
			if mode == "mismatch" {
				id = "two"
			}
			if mode == "foreign" {
				identity = "foreign"
			}
			l, _ := toLaunch(launches(id)[0])
			r, e := p.current.rpc.Submit(context.Background(), &wire.SubmitRequest{Invocation: 1, Launches: []*wire.Launch{l}, Listen: true, Registration: []byte(identity)})
			if e != nil || len(r.Results) != 1 || native.calls.Load() != 1 {
				t.Fatal(r, e, native.calls.Load())
			}
		})
	}
}
func TestLogCallsValidationNoListenerAndNilHandle(t *testing.T) {
	p, _, native := logHarness(t, LogOptions{})
	r, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{})
	if e != nil || h != nil || r[0].Status != api.Accepted || p.logs != nil || native.calls.Load() != 1 {
		t.Fatal(r, h, e)
	}
	for _, o := range []api.SubmitOptions{{Listener: &api.ListenOptions{}}, {Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }, Duration: -1}}} {
		if _, h, e := p.Submit(context.Background(), launches("one"), o); e == nil || h != nil {
			t.Fatal(h, e)
		}
	}
	if native.calls.Load() != 1 {
		t.Fatal("validation dispatched")
	}
	_, h, e = p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e != nil || h == nil {
		t.Fatal(h, e)
	}
	if r := logWait(t, h); r.Reason != api.ListenerProtocolError || !errors.Is(r.Cause, api.ErrLogProtocol) {
		t.Fatal(r)
	}
}
func TestLogCallsCapacityAndMethodErrorIndependence(t *testing.T) {
	p, _, native := logHarness(t, LogOptions{MaxSubscriptions: 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		if o.Listener.Context.Err() != nil {
			return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Accepted}}, nil, errors.New("admission diagnostic")
		}
		h := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
		_ = h.enqueue(logRecord(1))
		return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.NoCapacity, Reason: "original"}}, h, errors.New("admission diagnostic")
	}
	_, obs, h, e := p.SubmitDetailed(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { close(entered); <-release; return nil }}})
	if e == nil || !obs.MethodError.Present {
		t.Fatal(obs, e)
	}
	<-entered
	h.Stop()
	ss, _, failed, e := p.SubmitDetailed(context.Background(), launches("two"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e == nil || ss[0].Status != api.Accepted || native.calls.Load() != 2 {
		t.Fatal(ss, e)
	}
	if r := logWait(t, failed); r.Reason != api.ListenerCapacityExceeded {
		t.Fatal(r)
	}
	if _, e = p.GetCapabilities(context.Background()); e != nil {
		t.Fatal("logs exhausted business permits", e)
	}
}

func TestLogCallsRemoteCapacityPreservesAdmission(t *testing.T) {
	p, _, native := logHarness(t, LogOptions{MaxSubscriptions: 1})
	// The client has room, but the healthy remote server can only host one stream.
	p.opts.LogOptions.MaxSubscriptions = 2
	created := make(chan *logSubscription, 1)
	native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		var h api.LogSubscription
		if o.Listener.Context.Err() == nil {
			s := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
			created <- s
			h = s
		}
		return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Accepted}}, h, nil
	}
	received := make(chan api.LaunchLog, 1)
	o := api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(_ context.Context, v api.LaunchLog) error { received <- v; return nil }}}
	ss, active, err := p.Submit(context.Background(), launches("one"), o)
	if err != nil || len(ss) != 1 || ss[0].Status != api.Accepted || active == nil {
		t.Fatal(ss, active, err)
	}
	defer active.Stop()
	producer := <-created
	ss, failed, err := p.Submit(context.Background(), launches("two"), o)
	if err != nil || len(ss) != 1 || ss[0].Status != api.Accepted || failed == nil || native.calls.Load() != 2 {
		t.Fatal(ss, failed, err, native.calls.Load())
	}
	if result := logWait(t, failed); result.Reason != api.ListenerCapacityExceeded || !errors.Is(result.Cause, api.ErrLogCapacity) {
		t.Fatal(result)
	}
	if _, err := p.GetCapabilities(context.Background()); err != nil {
		t.Fatal("observation rejection affected business calls", err)
	}
	if err := producer.enqueue(logRecord(1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("existing listener stopped delivering")
	}
	select {
	case <-active.Done():
		t.Fatal("existing listener stopped", logWait(t, active))
	default:
	}
}
func TestLogServerForeverSurvivesWindowAndRetirementBound(t *testing.T) {
	p, s, native := logHarness(t, LogOptions{SetupTimeout: time.Second})
	var handle api.LogSubscription
	native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		h := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
		handle = h
		return []api.Submission{{LaunchID: "one", Status: api.Accepted}}, h, nil
	}
	_, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Stop()
	s.mu.Lock()
	s.high = 10000
	for i := uint64(1); i <= 10000; i++ {
		s.retireLogLocked(i)
	}
	n := len(s.logRetired)
	active := s.logs[1]
	s.mu.Unlock()
	if n > 4096 || active == nil {
		t.Fatal("unbounded retirement or evicted live registration", n)
	}
	select {
	case <-h.Done():
		t.Fatal(logWait(t, h))
	case <-handle.Done():
		t.Fatal("native listener stopped")
	default:
	}
}
func TestLogServerReadinessAndMalformedControl(t *testing.T) {
	p, s, native := logHarness(t, LogOptions{})
	s.mu.Lock()
	s.negotiated = false
	s.mu.Unlock()
	stream, cc := observe(t, p, 1, "one")
	defer cc()
	if _, e := stream.Recv(); e == nil {
		t.Fatal("unready observation accepted")
	}
	if native.calls.Load() != 0 {
		t.Fatal("unready observation dispatched")
	}
	s.mu.Lock()
	s.negotiated = true
	s.mu.Unlock()
	stream, cc = observe(t, p, 2, "one")
	defer cc()
	if f, e := stream.Recv(); e != nil || f.GetReady() == nil {
		t.Fatal(f, e)
	}
	_ = stream.Send(&wire.ObserveControl{Kind: &wire.ObserveControl_Register{Register: &wire.LogRegistration{Invocation: 3}}})
	for {
		f, e := stream.Recv()
		if e != nil {
			t.Fatal("missing explicit protocol terminal", e)
		}
		if f.GetTerminal() != nil {
			if f.GetTerminal().Reason != string(api.ListenerProtocolError) {
				t.Fatal(f)
			}
			break
		}
	}
}

func TestLogCallsKnownNonDispatchAndExactDuration(t *testing.T) {
	p, _, native := logHarness(t, LogOptions{})
	var observed time.Duration
	native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		observed = o.Listener.Duration
		h := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
		return []api.Submission{{LaunchID: "one", Status: api.Accepted}}, h, nil
	}
	const huge = time.Duration(1<<63 - 1)
	_, h, e := p.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), Duration: huge, OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e != nil || observed != huge {
		t.Fatal(observed, e)
	}
	h.Stop()
	c, cc := context.WithCancel(context.Background())
	cc()
	_, h, e = p.Submit(c, launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e == nil || h == nil || native.calls.Load() != 1 {
		t.Fatal(h, e)
	}
	r := logWait(t, h)
	if r.Reason != api.ListenerSourceUnavailable {
		t.Fatal(r)
	}
	for _, q := range r.Coverage {
		if q.State != api.SourceUnavailable {
			t.Fatal(q)
		}
	}
}
func TestLogCollectorCapacitySurvivesReconnect(t *testing.T) {
	tr := testTrust(t)
	o := collectorOptions(tr)
	o.LogOptions.MaxSubscriptions = 1
	c := testCollector(t, o)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	provider := &reverseProvider{submitWithLogs: func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		var h api.LogSubscription
		if o.Listener != nil && o.Listener.Context.Err() == nil {
			s := newLogSubscription(logLimits(t), newLogPool(logLimits(t)), ls, *o.Listener)
			_ = s.enqueue(logRecord(1))
			h = s
		}
		return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Accepted}}, h, nil
	}}
	_, run := launchRun(t, factoryFor(provider), runOptions(t, c, tr, "alpha"))
	old := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
	_, h, e := old.Submit(context.Background(), launches("one"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { close(entered); <-release; return nil }}})
	if e != nil {
		t.Fatal(e)
	}
	<-entered
	h.Stop()
	_ = old.Close()
	receiveRun(t, run)
	nextSnapshot(t, c, 0)
	_, run = launchRun(t, factoryFor(provider), runOptions(t, c, tr, "alpha"))
	newer := nextSnapshot(t, c, 1).Providers[0].Provider
	ss, failed, e := newer.Submit(context.Background(), launches("two"), api.SubmitOptions{Listener: &api.ListenOptions{Context: context.Background(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if e != nil || ss[0].Status != api.Accepted || logWait(t, failed).Reason != api.ListenerCapacityExceeded {
		t.Fatal(ss, e)
	}
	if provider.calls.Load() != 2 {
		t.Fatal("reconnect replayed admission", provider.calls.Load())
	}
	_ = c.Close()
	receiveRun(t, run)
}

func TestLogServerStreamLossPreservesHeldUnaryAdmission(t *testing.T) {
	p, s, native := logHarness(t, LogOptions{})
	entered := make(chan struct{})
	release := make(chan struct{})
	native.submitWithLogs = func(_ context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
		close(entered)
		<-release
		return []api.Submission{{LaunchID: "one", Status: api.Accepted}}, nil, nil
	}
	stream, cancel := observe(t, p, 1, "one")
	defer cancel()
	if f, e := stream.Recv(); e != nil || f.GetReady() == nil {
		t.Fatal(f, e)
	}
	l, _ := toLaunch(launches("one")[0])
	done := make(chan struct{})
	var result *wire.SubmitResponse
	var e error
	go func() {
		defer close(done)
		result, e = p.current.rpc.Submit(context.Background(), &wire.SubmitRequest{Invocation: 1, Launches: []*wire.Launch{l}, Listen: true, Registration: []byte("reg-1")})
	}()
	<-entered
	cancel()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.logs) == 0 })
	select {
	case <-done:
		t.Fatal("stream loss canceled unary admission")
	default:
	}
	close(release)
	<-done
	if e != nil || len(result.Results) != 1 || string(result.Results[0].Status) != string(api.Accepted) || native.calls.Load() != 1 {
		t.Fatal(result, e, native.calls.Load())
	}
}
func TestLogServerInvalidSubmitRetiresExactRegistration(t *testing.T) {
	p, s, native := logHarness(t, LogOptions{})
	stream, cancel := observe(t, p, 1, "one")
	defer cancel()
	if f, e := stream.Recv(); e != nil || f.GetReady() == nil {
		t.Fatal(f, e)
	}
	l, _ := toLaunch(launches("one")[0])
	_, e := p.current.rpc.Submit(context.Background(), &wire.SubmitRequest{Invocation: 1, Launches: []*wire.Launch{l}, Listen: true, Registration: []byte("reg-1"), DurationNanos: -1})
	if e == nil || native.calls.Load() != 0 {
		t.Fatal(e, native.calls.Load())
	}
	for {
		f, e := stream.Recv()
		if e != nil {
			t.Fatal("missing non-dispatch terminal", e)
		}
		if f.GetTerminal() != nil {
			if f.GetTerminal().Reason != string(api.ListenerSourceUnavailable) {
				t.Fatal(f)
			}
			break
		}
	}
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.logs) == 0 })
}
