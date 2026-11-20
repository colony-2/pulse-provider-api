package plugin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
)

// Run dials the central server and serves one locally configured provider per
// session. ctx bounds its entire lifecycle, including explicitly enabled retries.
// It never listens on a provider port or replays a business operation.
func Run(ctx context.Context, factory Factory, options RunOptions) error {
	if ctx == nil || factory == nil {
		return ErrInvalid
	}
	if e := options.defaults(); e != nil {
		return e
	}
	attempts := 0
	for {
		var healthyFor time.Duration
		terminal, e := runSession(ctx, factory, options, &healthyFor)
		if errors.Is(e, ErrCleanup) {
			return e
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if terminal || e == nil {
			return e
		}
		if options.Reconnect.HealthyResetInterval > 0 && healthyFor >= options.Reconnect.HealthyResetInterval {
			attempts = 0
		}
		if attempts >= options.Reconnect.MaxAttempts {
			return e
		}
		timer := time.NewTimer(restartDelay(options.Reconnect, attempts, rand.Float64()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		attempts++
	}
}
func runSession(ctx context.Context, factory Factory, o RunOptions, healthyFor *time.Duration) (terminal bool, result error) {
	startup, cancel := context.WithTimeout(ctx, o.StartupTimeout)
	defer cancel()
	raw, e := (&net.Dialer{}).DialContext(startup, "tcp", o.Address)
	if e != nil {
		return false, safeError(ErrUnavailable, startup.Err())
	}
	socket := newSessionSocket(raw)
	defer socket.Close()
	stopStartup := context.AfterFunc(startup, func() { _ = socket.Close() })
	defer stopStartup()
	tc := tls.Client(socket, o.TLSConfig)
	if e = tc.HandshakeContext(startup); e != nil {
		terminal = terminalTLSError(e)
		return terminal, safeError(ErrProtocol, startup.Err())
	}
	if tc.ConnectionState().NegotiatedProtocol != reverseALPN {
		return true, ErrProtocol
	}
	_ = tc.SetDeadline(deadlineOf(startup))
	id, e := readAdmission(tc)
	if e != nil {
		return errors.Is(e, ErrProtocol) || terminalTLSError(e), safeError(ErrProtocol, startup.Err())
	}
	if id != o.Init.Name {
		return true, ErrProtocol
	}
	_ = tc.SetDeadline(time.Time{})
	s := &server{opts: o.Serve, factory: factory, max: o.Serve.MaxMessageBytes, shutdown: o.Serve.ShutdownTimeout, permits: make(chan struct{}, o.Serve.Concurrency), tokens: map[uint64]struct{}{}, reverseInit: &o.Init, terminal: make(chan error, 1), ready: make(chan struct{}), factoryDone: make(chan struct{}), shutdownReceived: make(chan struct{})}
	s.lifetime, s.cancel = context.WithCancel(ctx)
	adopted := newSessionSocket(tc)
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(o.Serve.MaxMessageBytes), grpc.MaxSendMsgSize(o.Serve.MaxMessageBytes), grpc.MaxHeaderListSize(16<<10), grpc.MaxConcurrentStreams(uint32(o.Serve.Concurrency+o.Serve.LogOptions.MaxSubscriptions+2)), grpc.UnaryInterceptor(s.intercept))
	wire.RegisterProviderServer(gs, s)
	served := make(chan struct{})
	go func() { _ = gs.Serve(newSocketListener(adopted)); close(served) }()
	var healthySince time.Time
	ready := s.ready
	orderly := false
	for {
		select {
		case <-ctx.Done():
			result = ctx.Err()
		case <-startup.Done():
			result = safeError(ErrUnavailable, startup.Err())
			terminal = true
		case <-ready:
			if !stopStartup() || startup.Err() != nil {
				result = ErrUnavailable
				terminal = true
				break
			}
			healthySince = time.Now()
			cancel()
			ready = nil
			startup = context.Background()
			continue
		case <-socket.done:
			result = ErrUnavailable
		case <-served:
			result = ErrUnavailable
		case <-s.shutdownReceived:
			orderly = true
			terminal = true
			result = nil
		case <-s.terminal:
			terminal = true
			result = ErrProtocol
		}
		break
	}
	// The host can close its socket immediately after the shutdown response.
	// If both notifications are ready, retain the acknowledged orderly shutdown
	// instead of classifying the same session as an unexpected transport loss.
	select {
	case <-s.shutdownReceived:
		orderly = true
		terminal = true
		result = nil
	default:
	}

	if !healthySince.IsZero() {
		*healthyFor = time.Since(healthySince)
	}
	// A response error may race socket loss. A failed initialized session is
	// terminal even if the transport notification won that race.
	select {
	case <-s.terminal:
		terminal = true
		result = ErrProtocol
	default:
	}
	// Close bounds cleanup even if factory code ignores its construction context.
	s.cancel()
	if e = s.close(); e != nil {
		result = e
		terminal = true
	}
	s.mu.Lock()
	initializing := s.initialized
	constructed := s.provider != nil
	shutdownDeadline := s.shutdownDeadline
	s.mu.Unlock()
	if initializing && !constructed {
		terminal = true
	}
	if initializing {
		select {
		case <-s.factoryDone:
		default:
			result = ErrCleanup
			terminal = true
		}
	}
	if orderly {
		done := make(chan struct{})
		go func() { gs.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Until(shutdownDeadline)):
			// gRPC GracefulStop can hold its mutex while waiting for a handler
			// that ignores cancellation. Close our socket first; do not let a
			// concurrent Stop waiting for that mutex extend the lifecycle budget.
			_ = socket.Close()
			go gs.Stop()
			result = ErrCleanup
		}
	} else {
		gs.Stop()
	}
	_ = adopted.Close()
	return terminal, result
}
func deadlineOf(ctx context.Context) time.Time { d, _ := ctx.Deadline(); return d }

func terminalTLSError(e error) bool {
	var verify *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var record tls.RecordHeaderError
	var op *net.OpError
	if errors.As(e, &verify) || errors.As(e, &unknown) || errors.As(e, &record) {
		return true
	}
	if errors.As(e, &op) {
		return op.Op == "remote error"
	}
	return !errors.Is(e, io.EOF) && !errors.Is(e, io.ErrUnexpectedEOF) && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded)
}
