package plugin

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
)

// A private single-socket host fixture, never a public SDK dial override.
func reversePeer(t *testing.T, tr *reverseTrust, f func(*tls.Conn)) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		raw, e := l.Accept()
		if e != nil {
			return
		}
		defer raw.Close()
		tc := tls.Server(raw, tr.host)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if tc.HandshakeContext(ctx) != nil {
			return
		}
		f(tc)
	}()
	return l.Addr().String()
}
func TestReverseAdmissionPrefixes(t *testing.T) {
	for _, kind := range []string{"denied", "duplicate", "full", "generation_exhausted", "unknown", "badmagic", "empty", "oversized", "control", "mismatch", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			tr := testTrust(t)
			var constructs atomic.Int32
			address := reversePeer(t, tr, func(tc *tls.Conn) {
				h := make([]byte, 7)
				copy(h, "PPR3")
				switch kind {
				case "denied":
					h[4] = 1
				case "duplicate":
					h[4] = 2
				case "full":
					h[4] = 3
				case "generation_exhausted":
					h[4] = 4
				case "unknown":
					h[4] = 5
				case "badmagic":
					copy(h, "????")
				case "oversized":
					binary.BigEndian.PutUint16(h[5:], 257)
				case "control":
					binary.BigEndian.PutUint16(h[5:], 1)
					h = append(h, '\n')
				case "mismatch":
					binary.BigEndian.PutUint16(h[5:], 4)
					h = append(h, []byte("beta")...)
				case "truncated":
					h = h[:4]
				}
				_, _ = tc.Write(h)
			})
			o := RunOptions{Address: address, TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}, StartupTimeout: 200 * time.Millisecond}
			if kind != "truncated" {
				o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
			}
			_, done := launchRun(t, func(ctx context.Context, in Init) (api.Provider, Cleanup, error) {
				constructs.Add(1)
				return &reverseProvider{}, nil, nil
			}, o)
			if e := receiveRun(t, done); e == nil || constructs.Load() != 0 {
				t.Fatal(e, constructs.Load())
			}
		})
	}
}
func TestReverseSocketAdoption(t *testing.T) {
	a, b := net.Pipe()
	socket := newSessionSocket(a)
	defer b.Close()
	defer socket.Close()
	l := newSocketListener(socket)
	got, e := l.Accept()
	if e != nil || got != socket {
		t.Fatal(e)
	}
	waiting := make(chan error, 1)
	go func() { _, e := l.Accept(); waiting <- e }()
	l.Close()
	if e = <-waiting; !errors.Is(e, net.ErrClosed) {
		t.Fatal(e)
	}
	// Closing the listener after adoption must not close the active response socket.
	select {
	case <-socket.done:
		t.Fatal("listener closed the adopted socket")
	default:
	}
}

func TestReverseAdmissionStatusBoundaries(t *testing.T) {
	for _, kind := range []string{"stopped", "generation_exhausted"} {
		t.Run(kind, func(t *testing.T) {
			tr := testTrust(t)
			o := collectorOptions(tr)
			entered, release := make(chan struct{}), make(chan struct{})
			normal := o.Authorize
			o.Authorize = func(ctx context.Context, state tls.ConnectionState) (string, error) {
				close(entered)
				<-release
				return normal(ctx, state)
			}
			c := testCollector(t, o)
			peer, e := tls.Dial("tcp", c.listener.Addr().String(), tr.client)
			if e != nil {
				t.Fatal(e)
			}
			defer peer.Close()
			<-entered
			c.mu.Lock()
			if kind == "stopped" {
				c.stopped = true
			} else {
				c.generation = math.MaxUint64
			}
			c.mu.Unlock()
			close(release)
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			h := make([]byte, 7)
			if _, e = io.ReadFull(peer, h); e != nil {
				t.Fatal(e)
			}
			want := byte(1)
			if kind == "generation_exhausted" {
				want = 4
			}
			if string(h[:4]) != "PPR3" || h[4] != want || binary.BigEndian.Uint16(h[5:]) != 0 {
				t.Fatal(h)
			}
			if kind == "generation_exhausted" && !errors.Is(c.Wait(), ErrUnavailable) {
				t.Fatal("generation exhaustion did not stop collector")
			}
		})
	}
}

func TestReverseRejectsHTTP1Framing(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	peer, e := tls.Dial("tcp", c.listener.Addr().String(), tr.client)
	if e != nil {
		t.Fatal(e)
	}
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	if id, e := readAdmission(peer); e != nil || id != "alpha" {
		t.Fatal(id, e)
	}
	_, _ = peer.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	_ = peer.Close()
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.reserved) == 0 })
	if s, e := c.Snapshot(); e != nil || s.Revision != 1 || len(s.Providers) != 0 {
		t.Fatal(s, e)
	}
}

type reverseCountingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *reverseCountingListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e == nil {
		l.accepts.Add(1)
	}
	return c, e
}
func TestReverseOneConnectionCarriesGRPCAndCallbacks(t *testing.T) {
	tr := testTrust(t)
	raw, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	l := &reverseCountingListener{Listener: raw}
	c, e := StartCollector(context.Background(), l, collectorOptions(tr))
	if e != nil {
		raw.Close()
		t.Fatal(e)
	}
	defer c.Close()
	log := make(chan ProviderChange, 4)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { log <- v })
	callbackEvent(t, log)
	_, done := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	p := nextSnapshot(t, c, 1).Providers[0].Provider
	if _, e = p.GetCapabilities(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = p.List(context.Background(), api.ListRequest{}); e != nil {
		t.Fatal(e)
	}
	if ss, _, e := p.Submit(context.Background(), launches("one-connection"), api.SubmitOptions{}); e != nil || ss[0].Status != api.Accepted {
		t.Fatal(ss, e)
	}
	if v := callbackEvent(t, log); len(v.Joined) != 1 || l.accepts.Load() != 1 {
		t.Fatal(v, l.accepts.Load())
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	receiveRun(t, done)
	if s.Wait() != nil {
		t.Fatal(s.Wait())
	}
}
func TestReversePendingLimitsAndAuthorizationTimeout(t *testing.T) {
	tr := testTrust(t, "alpha", "beta")
	o := collectorOptions(tr)
	o.MaxPendingConnections = 2
	o.StartupTimeout = 100 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	var authCalls atomic.Int32
	normal := o.Authorize
	o.Authorize = func(ctx context.Context, s tls.ConnectionState) (string, error) {
		if s.PeerCertificates[0].Subject.CommonName == "beta" {
			authCalls.Add(1)
			close(entered)
			<-release
		}
		return normal(ctx, s)
	}
	c := testCollector(t, o)
	_, ready := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	nextSnapshot(t, c, 1)
	_, pending := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "beta"))
	<-entered
	// Occupy the remaining pending slot with a stalled TLS socket.
	stall, e := net.Dial("tcp", c.listener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer stall.Close()
	eventually(t, func() bool { return len(c.pending) == 2 })
	overflow, e := net.Dial("tcp", c.listener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer overflow.Close()
	overflow.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, e = overflow.Read(b[:]); e == nil {
		t.Fatal("overflow stayed open")
	}
	receiveRun(t, pending)
	// An uncooperative callback keeps its slot even after its transport times out.
	if authCalls.Load() != 1 {
		t.Fatal(authCalls.Load())
	}
	if s, _ := c.Snapshot(); len(s.Providers) != 1 {
		t.Fatal(s)
	}
	begin := time.Now()
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	if time.Since(begin) > time.Second {
		t.Fatal("unbounded close")
	}
	close(release)
	receiveRun(t, ready)
	eventually(t, func() bool { return len(c.pending) == 0 })
}
func TestReverseInitializationOverridesAndNegotiation(t *testing.T) {
	for _, kind := range []string{"config", "base", "env", "name", "semantics", "protocol"} {
		t.Run(kind, func(t *testing.T) {
			tr := testTrust(t)
			var constructed atomic.Int32
			addr := reversePeer(t, tr, func(tc *tls.Conn) {
				if writeAdmission(tc, 0, "alpha") != nil {
					return
				}
				socket := newSessionSocket(tc)
				conn, e := adoptSocket(socket, DefaultMessageBytes, 16<<10)
				if e != nil {
					t.Error(e)
					return
				}
				defer conn.Close()
				rpc := wire.NewProviderClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				required := semantics
				if kind == "semantics" {
					required = []string{"wrong"}
				}
				baseline := uint32(Protocol)
				if kind == "protocol" {
					baseline = 2
				}
				_, e = rpc.Negotiate(ctx, &wire.Negotiation{Protocol: baseline, Required: required, MaxMessageBytes: DefaultMessageBytes, ShutdownNanos: int64(time.Second)})
				if kind == "semantics" || kind == "protocol" {
					if e == nil {
						t.Error("negotiation succeeded")
					}
					return
				}
				if e != nil {
					t.Error(e)
					return
				}
				in := &wire.Initialization{Name: []byte("alpha")}
				switch kind {
				case "config":
					in.ConfigYaml = []byte("secret")
				case "base":
					in.BaseDir = []byte("/host")
				case "env":
					in.ExecutorEnv = []*wire.Env{{Key: []byte("HOST"), Value: []byte("secret")}}
				case "name":
					in.Name = []byte("beta")
				}
				r, e := rpc.Initialize(ctx, in, grpc.WaitForReady(false))
				if e == nil && r.Error == nil {
					t.Error("override succeeded")
				}
			})
			o := RunOptions{Address: addr, TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}, StartupTimeout: time.Second, Reconnect: RestartPolicy{MaxAttempts: 2}}
			_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
				constructed.Add(1)
				return &reverseProvider{}, nil, nil
			}, o)
			if e := receiveRun(t, done); e == nil || constructed.Load() != 0 {
				t.Fatal(e, constructed.Load())
			}
		})
	}
}
