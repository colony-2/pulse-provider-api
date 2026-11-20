package plugin

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"github.com/hashicorp/go-hclog"
	hp "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type server struct {
	wire.UnimplementedProviderServer
	opts             ServeOptions
	factory          Factory
	mu               sync.Mutex
	negotiated       bool
	initialized      bool
	closing          bool
	max              int
	shutdown         time.Duration
	provider         api.Provider
	cleanup          Cleanup
	lifetime         context.Context
	cancel           context.CancelFunc
	cleanupOnce      sync.Once
	cleanupErr       error
	shutdownDeadline time.Time
	permits          chan struct{}
	high             uint64
	tokens           map[uint64]struct{}
	logs             map[uint64]*logRegistration
	logPool          *logPool
	logRetired       map[uint64]struct{}
	logHigh          uint64
	logStreams       chan struct{}
	// reverseInit is provider-local; hosts may supply only its bound identity.
	reverseInit      *Initialization
	terminal         chan error
	ready            chan struct{}
	factoryDone      chan struct{}
	shutdownReceived chan struct{}
	shutdownOnce     sync.Once
}

// Serve blocks until shutdown. Call it once from a dedicated provider command;
// return a nonzero process exit code if it returns an error. It never logs factory
// errors, payloads or configuration. Cleanup is bounded, including on parent loss.
func Serve(factory Factory, options ServeOptions) error {
	if factory == nil {
		return ErrInvalid
	}
	if e := options.defaults(); e != nil {
		return e
	}
	if !platformSupported() {
		return errors.New("plugin: process safeguards unavailable on this platform")
	}
	if os.Getenv(handshake.MagicCookieKey) != handshake.MagicCookieValue || os.Getenv("PLUGIN_CLIENT_CERT") == "" || os.Getenv("PULSE_PLUGIN_PARENT_FD") != "3" {
		return ErrProtocol
	}
	socketDir := os.Getenv(hp.EnvUnixSocketDir)
	defer removeTransport(socketDir, filepath.Dir(socketDir))
	parent := parentFile()
	if parent == nil {
		return ErrProtocol
	}
	defer parent.Close()
	s := &server{opts: options, factory: factory, max: options.MaxMessageBytes, shutdown: options.ShutdownTimeout, permits: make(chan struct{}, options.Concurrency), tokens: map[uint64]struct{}{}}
	s.lifetime, s.cancel = context.WithCancel(context.Background())
	defer s.cancel()
	var gs *grpc.Server
	var gmu sync.Mutex
	stopped := make(chan struct{})
	defer close(stopped)
	// The inherited pipe is private and marked close-on-exec in parentFile, so
	// provider-owned detached work cannot accidentally keep the host alive.
	go func() {
		_, _ = io.Copy(io.Discard, parent)
		s.cancel()
		go func() {
			select {
			case <-stopped:
				return
			case <-time.After(options.ShutdownTimeout):
				_ = removeTransport(socketDir, filepath.Dir(socketDir))
				killSelf()
			}
		}()
		_ = s.close()
		gmu.Lock()
		if gs != nil {
			gs.Stop()
		}
		gmu.Unlock()
	}()
	hp.Serve(&hp.ServeConfig{HandshakeConfig: handshake, Plugins: hp.PluginSet{"provider": &transportPlugin{server: s}}, Logger: hclog.NewNullLogger(), GRPCServer: func(opts []grpc.ServerOption) *grpc.Server {
		opts = append(opts, grpc.MaxRecvMsgSize(options.MaxMessageBytes), grpc.MaxSendMsgSize(options.MaxMessageBytes), grpc.MaxConcurrentStreams(uint32(options.Concurrency+options.LogOptions.MaxSubscriptions+2)), grpc.UnaryInterceptor(s.intercept))
		gmu.Lock()
		defer gmu.Unlock()
		gs = grpc.NewServer(opts...)
		return gs
	}})
	return s.close()
}
func (s *server) close() error {
	s.cleanupOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		cb := s.cleanup
		deadline := time.Now().Add(s.shutdown)
		s.shutdownDeadline = deadline
		s.mu.Unlock()
		s.cancel()
		if cb != nil {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- cb(ctx) }()
			select {
			case e := <-done:
				if e != nil {
					s.cleanupErr = ErrCleanup
				}
			case <-ctx.Done():
				s.cleanupErr = ErrCleanup
			}
		}
	})
	return s.cleanupErr
}
func (s *server) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	// Control-plane negotiation itself has a fixed pre-negotiation bound.
	s.mu.Lock()
	limit := s.max
	neg := s.negotiated
	s.mu.Unlock()
	if !neg && limit > MaxConfigBytes+65536 {
		limit = MaxConfigBytes + 65536
	}
	if m, ok := req.(proto.Message); ok && proto.Size(m) > limit {
		return nil, status.Error(codes.ResourceExhausted, "message bound")
	}
	if info.FullMethod == wire.Provider_Negotiate_FullMethodName || info.FullMethod == wire.Provider_Initialize_FullMethodName || info.FullMethod == wire.Provider_Control_FullMethodName {
		r, e := s.boundedResponse(ctx, req, next, limit)
		if s.terminal != nil && info.FullMethod != wire.Provider_Control_FullMethodName {
			failed := e != nil
			if v, ok := r.(*wire.Initialized); ok && v.Error != nil {
				failed = true
			}
			if failed {
				select {
				case s.terminal <- ErrProtocol:
				default:
				}
			}
		}
		return r, e
	}
	select {
	case s.permits <- struct{}{}:
		defer func() { <-s.permits }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "concurrency bound")
	}
	s.mu.Lock()
	ready := s.provider != nil && !s.closing
	s.mu.Unlock()
	if !ready {
		return nil, status.Error(codes.Unavailable, "not ready")
	}
	return s.boundedResponse(ctx, req, next, limit)
}
func (s *server) boundedResponse(ctx context.Context, req any, next grpc.UnaryHandler, limit int) (any, error) {
	r, e := next(ctx, req)
	if e == nil {
		if m, ok := r.(proto.Message); ok && proto.Size(m) > limit {
			return nil, status.Error(codes.ResourceExhausted, "message bound")
		}
	}
	return r, e
}
func (s *server) Negotiate(_ context.Context, n *wire.Negotiation) (*wire.Negotiated, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.negotiated || n.Protocol != Protocol || n.MaxMessageBytes < 1024 || n.MaxMessageBytes > MaxMessageBytes || n.ShutdownNanos <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "protocol")
	}
	have := map[string]bool{}
	for _, f := range n.Required {
		have[f] = true
	}
	if len(have) != len(semantics) {
		return nil, status.Error(codes.FailedPrecondition, "semantics")
	}
	for _, f := range semantics {
		if !have[f] {
			return nil, status.Error(codes.FailedPrecondition, "semantics")
		}
	}
	s.negotiated = true
	s.max = min(s.max, int(n.MaxMessageBytes))
	s.shutdown = min(s.shutdown, time.Duration(n.ShutdownNanos))
	return &wire.Negotiated{Implementation: []byte(s.opts.Implementation), Version: []byte(s.opts.Version), MaxMessageBytes: int64(s.max)}, nil
}
func (s *server) Initialize(ctx context.Context, n *wire.Initialization) (*wire.Initialized, error) {
	s.mu.Lock()
	if !s.negotiated || s.initialized || s.closing {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "initialization")
	}
	s.initialized = true
	s.mu.Unlock()
	if s.reverseInit == nil && (len(n.ConfigYaml) > MaxConfigBytes || len(n.Name) == 0 || !filepath.IsAbs(string(n.BaseDir))) {
		return &wire.Initialized{Error: toError(ErrInvalid)}, nil
	}
	input := Initialization{Name: string(n.Name), ConfigYAML: append([]byte(nil), n.ConfigYaml...), BaseDir: string(n.BaseDir), ExecutorEnv: map[string]string{}}
	for _, e := range n.ExecutorEnv {
		input.ExecutorEnv[string(e.GetKey())] = string(e.GetValue())
	}
	if s.reverseInit != nil {
		defer close(s.factoryDone)
		if string(n.Name) != s.reverseInit.Name || len(n.ConfigYaml) != 0 || len(n.BaseDir) != 0 || len(n.ExecutorEnv) != 0 {
			return &wire.Initialized{Error: toError(ErrInvalid)}, nil
		}
		input = copyInitialization(*s.reverseInit)
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.FactoryTimeout)
	defer cancel()
	p, cleanup, e := s.factory(ctx, Init{Initialization: input, Lifetime: s.lifetime})
	// Register even if the factory returned an error. No callback can be
	// registered for a factory that never returns before process termination.
	s.mu.Lock()
	closing := s.closing
	if !closing {
		s.cleanup = cleanup
	}
	s.mu.Unlock()
	if closing {
		if cleanup != nil {
			c, cc := context.WithTimeout(context.Background(), s.shutdown)
			defer cc()
			done := make(chan error, 1)
			go func() { done <- cleanup(c) }()
			select {
			case <-done:
			case <-c.Done():
			}
		}
		return &wire.Initialized{Error: toError(ErrUnavailable)}, nil
	}
	if e == nil {
		e = ctx.Err()
	}
	if e == nil && p == nil {
		e = ErrInvalid
	}
	if e != nil {
		_ = s.close()
		return &wire.Initialized{Error: toError(ErrUnavailable)}, nil
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return &wire.Initialized{Error: toError(ErrUnavailable)}, nil
	}
	s.provider = p
	s.mu.Unlock()
	if s.ready != nil {
		close(s.ready)
	}
	return &wire.Initialized{}, nil
}
func (s *server) Control(ctx context.Context, n *wire.ControlRequest) (*wire.Empty, error) {
	if n.Shutdown {
		e := s.close()
		if s.shutdownReceived != nil {
			s.shutdownOnce.Do(func() { close(s.shutdownReceived) })
		}
		if e != nil {
			return nil, status.Error(codes.DeadlineExceeded, "cleanup")
		}
	}
	return &wire.Empty{}, nil
}
func (s *server) GetCapabilities(ctx context.Context, _ *wire.Empty) (*wire.CapabilitiesResponse, error) {
	c, e := s.provider.GetCapabilities(ctx)
	r := toCaps(c)
	r.Error = toError(e)
	return r, nil
}
func (s *server) Submit(ctx context.Context, r *wire.SubmitRequest) (*wire.SubmitResponse, error) {
	if len(r.Launches) < 1 || len(r.Launches) > api.MaxBatch {
		s.rejectLogRequest(r)
		return nil, status.Error(codes.InvalidArgument, "launch")
	}
	ls := make([]api.Launch, len(r.Launches))
	for i, l := range r.Launches {
		v, e := fromLaunch(l)
		if e != nil {
			s.rejectLogRequest(r)
			return nil, status.Error(codes.InvalidArgument, "launch")
		}
		ls[i] = v
	}
	if validateLaunches(ls) != nil {
		s.rejectLogRequest(r)
		return nil, status.Error(codes.InvalidArgument, "launch")
	}
	logOptions, registration, le := s.bindLogs(r, ls)
	if le != nil {
		s.rejectLogRequest(r)
		return nil, status.Error(codes.InvalidArgument, "listener")
	}
	dispatched := false
	defer func() {
		if !dispatched && registration != nil {
			registration.sub.notStarted()
		}
	}()
	// Commit headers before entering native code, disabling transparent retry after
	// dispatch. A sliding fence rejects duplicate and retired tokens without replay.
	s.mu.Lock()
	id := r.Invocation
	_, duplicate := s.tokens[id]
	if id == 0 || duplicate || (s.high >= 4096 && id <= s.high-4096) {
		s.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "invocation fence")
	}
	if id > s.high {
		s.high = id
	}
	s.tokens[id] = struct{}{}
	if s.high >= 4096 {
		for k := range s.tokens {
			if k <= s.high-4096 {
				delete(s.tokens, k)
			}
		}
	}
	s.mu.Unlock()
	if e := grpc.SendHeader(ctx, metadata.MD{}); e != nil {
		return nil, e
	}
	dispatched = true
	ss, native, e := s.provider.Submit(ctx, ls, logOptions)
	if r.Listen {
		s.watchNative(registration, native)
	} else if native != nil {
		native.Stop()
	}
	out := &wire.SubmitResponse{Error: toError(e)}
	for _, v := range ss {
		out.Results = append(out.Results, toSub(v))
	}
	return out, nil
}
func (s *server) List(ctx context.Context, r *wire.ListRequest) (*wire.ListResponse, error) {
	q := api.ListRequest{PageSize: int(r.PageSize), PageToken: string(r.PageToken), LaunchID: string(r.LaunchId)}
	if validateList(&q) != nil {
		return nil, status.Error(codes.InvalidArgument, "list")
	}
	v, e := s.provider.List(ctx, q)
	out, ce := toList(v)
	if ce != nil {
		return nil, status.Error(codes.Internal, "response conversion")
	}
	out.Error = toError(e)
	return out, nil
}
