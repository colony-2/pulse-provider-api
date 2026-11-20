package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"github.com/hashicorp/go-hclog"
	hp "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/go-plugin/runner"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Process is a ready provider handle. Calls never queue for another generation.
// Close releases hosting resources without terminating detached native work.
type Process struct {
	sessionCaller
	health        Health
	done          chan struct{}
	closeOnce     sync.Once
	closeDeadline time.Time
	closeErr      error
}

var _ api.Provider = (*Process)(nil)

type generation struct {
	socketDir      string
	implementation string
	version        string
	id             uint64
	client         *hp.Client
	conn           *grpc.ClientConn
	rpc            wire.ProviderClient
	runner         *processRunner
	parent         *os.File
	dir            string
	limit          int
	ctx            context.Context
	cancel         context.CancelFunc
}

// Start returns only after authenticated negotiation and successful construction.
// ctx bounds startup only, independently of the returned process's lifetime.
func Start(ctx context.Context, options StartOptions) (*Process, error) {
	if !platformSupported() {
		return nil, errors.New("plugin: process safeguards unavailable on this platform")
	}
	if e := options.defaults(); e != nil {
		return nil, e
	}
	if ctx == nil {
		return nil, ErrInvalid
	}
	options.Args = append([]string(nil), options.Args...)
	options.Env = copyMap(options.Env)
	options.Init.ExecutorEnv = copyMap(options.Init.ExecutorEnv)
	options.Init.ConfigYAML = append([]byte(nil), options.Init.ConfigYAML...)
	if options.SHA256 != "" {
		b, e := hex.DecodeString(options.SHA256)
		if e != nil || len(b) != sha256.Size {
			return nil, ErrInvalid
		}
		options.SHA256 = hex.EncodeToString(b)
	}
	p := &Process{sessionCaller: sessionCaller{opts: options, permits: make(chan struct{}, options.Concurrency)}, done: make(chan struct{}), health: Health{Name: options.Init.Name, Protocol: Protocol, State: "starting"}}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	ready := make(chan error, 1)
	go p.supervise(ctx, ready)
	if e := <-ready; e != nil {
		<-p.done
		return nil, e
	}
	return p, nil
}
func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func (p *Process) Health() Health { p.mu.Lock(); defer p.mu.Unlock(); return p.health }
func (p *Process) Close() error {
	p.closeOnce.Do(func() {
		p.logClosing.Store(true)
		p.mu.Lock()
		p.health.State = "closing"
		p.closeDeadline = time.Now().Add(p.opts.ShutdownTimeout)
		p.cancel()
		p.mu.Unlock()
	})
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeErr
}
func (p *Process) shutdownContext() (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(p.opts.ShutdownTimeout)
	p.mu.Lock()
	if !p.closeDeadline.IsZero() && p.closeDeadline.Before(deadline) {
		deadline = p.closeDeadline
	}
	p.mu.Unlock()
	return context.WithDeadline(context.Background(), deadline)
}
func (p *Process) dispose(g *generation) error {
	if g == nil {
		return nil
	}
	g.cancel()
	ctx, cancel := p.shutdownContext()
	defer cancel()
	deadline, _ := ctx.Deadline()
	grace := time.Until(deadline) * 4 / 5
	var result error
	if g.rpc != nil && grace > 0 {
		c, cc := context.WithTimeout(ctx, grace)
		_, e := g.rpc.Control(c, &wire.ControlRequest{Shutdown: true})
		cc()
		if e != nil {
			result = ErrCleanup
		}
	}
	if g.conn != nil {
		_ = g.conn.Close()
	}
	if g.parent != nil {
		_ = g.parent.Close()
	}
	if g.runner != nil {
		_ = g.runner.Kill(ctx)
		select {
		case <-g.runner.done:
		case <-ctx.Done():
			result = ErrCleanup
		}
		g.runner.closePipes()
	}
	if g.client != nil {
		done := make(chan struct{})
		go func() { g.client.Kill(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			result = ErrCleanup
		}
	}
	if g.dir != "" {
		if e := removeTransport(g.socketDir, g.dir); e != nil {
			result = ErrCleanup
		}
	}
	// Close may cancel a probe or a replacement launch while that path owns
	// disposal. Retain its failure for every Close caller, regardless of which
	// supervisor branch observed cancellation. Earlier generation failures do
	// not become Close errors unless their disposal overlaps this shutdown.
	p.mu.Lock()
	if !p.closeDeadline.IsZero() && result != nil {
		p.closeErr = result
	}
	p.mu.Unlock()
	return result
}
func (p *Process) launch(ctx context.Context, id uint64) (g *generation, err error) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.StartupTimeout)
	defer cancel()
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()
	g = &generation{id: id, limit: p.opts.MaxMessageBytes}
	g.ctx, g.cancel = context.WithCancel(p.ctx)
	defer func() {
		if err != nil {
			_ = p.dispose(g)
		}
	}()
	g.dir, err = os.MkdirTemp("", "pulse-plugin-")
	if err != nil {
		return g, ErrUnavailable
	}
	read, write, e := os.Pipe()
	if e != nil {
		return g, ErrUnavailable
	}
	defer read.Close()
	g.parent = write
	cmd := exec.Command(p.opts.Executable, p.opts.Args...)
	cmd.ExtraFiles = []*os.File{read}
	cmd.Env = []string{"PULSE_PLUGIN_PARENT_FD=3"}
	for k, v := range p.opts.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	deadline, _ := ctx.Deadline()
	config := &hp.ClientConfig{HandshakeConfig: handshake, Plugins: hp.PluginSet{"provider": &transportPlugin{}}, SkipHostEnv: true, AutoMTLS: true, AllowedProtocols: []hp.Protocol{hp.ProtocolGRPC}, StartTimeout: time.Until(deadline), Logger: hclog.NewNullLogger(), Stderr: io.Discard, SyncStdout: io.Discard, SyncStderr: io.Discard, UnixSocketConfig: &hp.UnixSocketConfig{TempDir: g.dir}, GRPCDialOptions: []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.WaitForReady(false), grpc.MaxCallRecvMsgSize(p.opts.MaxMessageBytes), grpc.MaxCallSendMsgSize(p.opts.MaxMessageBytes))}}
	if p.opts.SHA256 != "" {
		file, e := os.Open(p.opts.Executable)
		if e != nil {
			return g, ErrUnavailable
		}
		digest := sha256.New()
		_, e = io.Copy(digest, file)
		file.Close()
		if e != nil || hex.EncodeToString(digest.Sum(nil)) != p.opts.SHA256 {
			return g, ErrInvalid
		}
	}
	config.RunnerFunc = func(_ hclog.Logger, c *exec.Cmd, socketDir string) (runner.Runner, error) {
		g.socketDir = socketDir
		c.Path = cmd.Path
		c.Args = cmd.Args
		c.Env = append(c.Env, cmd.Env...)
		c.ExtraFiles = cmd.ExtraFiles
		c.Dir = g.dir
		r, e := newRunner(c, ctx)
		if e == nil {
			g.runner = r
		}
		return r, e
	}
	g.client = hp.NewClient(config)
	protoClient, e := g.client.Client()
	if g.runner != nil && g.runner.stop != nil {
		defer g.runner.stop()
	}
	if e != nil {
		return g, safeError(ErrUnavailable, ctx.Err())
	}
	if ctx.Err() != nil {
		return g, safeError(ErrUnavailable, ctx.Err())
	}
	gc, ok := protoClient.(*hp.GRPCClient)
	if !ok {
		return g, ErrProtocol
	}
	g.conn = gc.Conn
	g.rpc = wire.NewProviderClient(g.conn)
	n, e := g.rpc.Negotiate(ctx, &wire.Negotiation{Protocol: Protocol, Required: semantics, MaxMessageBytes: int64(p.opts.MaxMessageBytes), ShutdownNanos: int64(p.opts.ShutdownTimeout)})
	if e != nil {
		return g, safeError(ErrProtocol, ctx.Err())
	}
	if n.MaxMessageBytes < 1024 || n.MaxMessageBytes > int64(g.limit) {
		return g, ErrProtocol
	}
	g.limit = int(n.MaxMessageBytes)
	in := p.opts.Init
	init := &wire.Initialization{Name: []byte(in.Name), ConfigYaml: in.ConfigYAML, BaseDir: []byte(in.BaseDir)}
	for k, v := range in.ExecutorEnv {
		init.ExecutorEnv = append(init.ExecutorEnv, &wire.Env{Key: []byte(k), Value: []byte(v)})
	}
	if proto.Size(init) > g.limit {
		return g, ErrTooLarge
	}
	r, e := g.rpc.Initialize(ctx, init, grpc.MaxCallRecvMsgSize(g.limit), grpc.MaxCallSendMsgSize(g.limit))
	if e != nil {
		return g, safeError(ErrUnavailable, ctx.Err())
	}
	if r.Error != nil {
		return g, ErrUnavailable
	}
	g.implementation = string(n.Implementation)
	g.version = string(n.Version)
	return g, nil
}
func (p *Process) supervise(startup context.Context, ready chan<- error) {
	defer close(p.done)
	g, e := p.launch(startup, 1)
	if e != nil {
		p.cancel()
		p.mu.Lock()
		p.health.State = "closed"
		p.health.LastFailure = FailureStartup
		p.mu.Unlock()
		ready <- e
		return
	}
	publish := func(g *generation) bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.ctx.Err() != nil {
			return false
		}
		p.current = g
		p.health.State = "ready"
		p.health.Generation = g.id
		p.health.Implementation = g.implementation
		p.health.Version = g.version
		return true
	}
	if !publish(g) {
		_ = p.dispose(g)
		ready <- ErrUnavailable
		return
	}
	ready <- nil
	ticker := time.NewTicker(p.opts.ProbeInterval)
	defer ticker.Stop()
	attempts := 0
	nextID := uint64(1)
	healthySince := time.Now()
	for {
		failed := false
		code := FailureProcessExit
		select {
		case <-p.ctx.Done():
			p.mu.Lock()
			p.current = nil
			p.mu.Unlock()
			_ = p.dispose(g)
			p.mu.Lock()
			p.health.State = "closed"
			p.mu.Unlock()
			return
		case <-g.runner.done:
			failed = true
		case <-ticker.C:
			c, cc := context.WithTimeout(p.ctx, p.opts.ProbeTimeout)
			_, pe := g.rpc.Control(c, &wire.ControlRequest{})
			cc()
			if pe != nil {
				failed = true
				code = FailureProbe
			}
		}
		if p.opts.Restart.HealthyResetInterval > 0 && time.Since(healthySince) >= p.opts.Restart.HealthyResetInterval {
			attempts = 0
		}
		if !failed {
			continue
		}
		p.mu.Lock()
		p.current = nil
		if p.ctx.Err() == nil {
			p.health.State = "restarting"
		}
		p.health.LastFailure = code
		p.mu.Unlock()
		_ = p.dispose(g)
		g = nil
		for g == nil {
			if p.ctx.Err() != nil {
				p.mu.Lock()
				p.health.State = "closed"
				p.mu.Unlock()
				return
			}
			if attempts >= p.opts.Restart.MaxAttempts {
				p.mu.Lock()
				p.health.State = "exhausted"
				p.mu.Unlock()
				<-p.ctx.Done()
				p.mu.Lock()
				p.health.State = "closed"
				p.mu.Unlock()
				return
			}
			timer := time.NewTimer(restartDelay(p.opts.Restart, attempts, rand.Float64()))
			select {
			case <-p.ctx.Done():
				timer.Stop()
				continue
			case <-timer.C:
			}
			if p.ctx.Err() != nil {
				continue
			}
			attempts++
			nextID++
			p.mu.Lock()
			p.health.RestartCount++
			p.mu.Unlock()
			g, e = p.launch(p.ctx, nextID)
			if e != nil {
				g = nil
				continue
			}
			if !publish(g) {
				_ = p.dispose(g)
				g = nil
				continue
			}
			healthySince = time.Now()
		}
	}
}
func restartDelay(r RestartPolicy, attempt int, jitter float64) time.Duration {
	x := math.Min(float64(r.InitialDelay)*math.Pow(r.Multiplier, float64(attempt)), float64(r.MaxDelay))
	x *= 1 + r.JitterFraction*(2*jitter-1)
	if x >= float64(r.MaxDelay) {
		return r.MaxDelay
	}
	if x <= 0 {
		return 0
	}
	return time.Duration(x)
}
