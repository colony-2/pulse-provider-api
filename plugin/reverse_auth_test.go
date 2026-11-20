package plugin

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"google.golang.org/grpc/metadata"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

type countListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *countListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e == nil {
		l.accepts.Add(1)
	}
	return c, e
}
func countedCollector(t *testing.T, o CollectorOptions) (*Collector, *countListener) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	cl := &countListener{Listener: l}
	c, e := StartCollector(context.Background(), cl, o)
	if e != nil {
		l.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, cl
}
func TestReverseAuthenticationIsTerminal(t *testing.T) {
	for _, kind := range []string{"wrong-root", "hostname", "no-certificate", "untrusted-client", "expired", "unauthorized", "alpn"} {
		t.Run(kind, func(t *testing.T) {
			tr := testTrust(t)
			co := collectorOptions(tr)
			if kind == "unauthorized" {
				co.Authorize = func(context.Context, tls.ConnectionState) (string, error) {
					return "", errors.New("private authorization secret")
				}
			}
			c, l := countedCollector(t, co)
			o := runOptions(t, c, tr, "alpha")
			o.Reconnect = RestartPolicy{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
			switch kind {
			case "wrong-root":
				o.TLSConfig.RootCAs = testTrust(t).client.RootCAs
			case "hostname":
				o.TLSConfig.ServerName = "wrong.test"
			case "no-certificate":
				o.TLSConfig.Certificates = nil
				o.TLSConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &tls.Certificate{}, nil }
			case "untrusted-client":
				o.TLSConfig.Certificates = testTrust(t).client.Certificates
			case "expired":
				parsed, e := x509.ParseCertificate(o.TLSConfig.Certificates[0].Certificate[0])
				if e != nil {
					t.Fatal(e)
				}
				parsed.NotBefore = time.Now().Add(-2 * time.Hour)
				parsed.NotAfter = time.Now().Add(-time.Hour)
				key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if e != nil {
					t.Fatal(e)
				}
				der, e := x509.CreateCertificate(rand.Reader, parsed, tr.caCert, &key.PublicKey, tr.caKey)
				if e != nil {
					t.Fatal(e)
				}
				o.TLSConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}
			case "alpn":
				// The SDK validates local ALPN. This malicious host offers a different one.
				tr.host.NextProtos = []string{"h2"}
				o.Address = reversePeer(t, tr, func(net.Conn) { t.Error("wrong ALPN authenticated") })
			}
			var factories atomic.Int32
			_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
				factories.Add(1)
				return &reverseProvider{}, nil, nil
			}, o)
			started := time.Now()
			e := receiveRun(t, done)
			expectedAccepts := int32(1)
			if kind == "alpn" {
				expectedAccepts = 0
				if time.Since(started) > 500*time.Millisecond {
					t.Fatal("ALPN rejection retried")
				}
			}
			if e == nil || strings.Contains(e.Error(), "private") || factories.Load() != 0 || l.accepts.Load() != expectedAccepts {
				t.Fatal(e, factories.Load(), l.accepts.Load())
			}
			s, _ := c.Snapshot()
			if len(s.Providers) != 0 || s.Revision != 1 {
				t.Fatal(s)
			}
		})
	}
}
func TestReverseAuthorizationRevocation(t *testing.T) {
	tr := testTrust(t, "alpha", "beta")
	o := collectorOptions(tr)
	normal := o.Authorize
	var revoked atomic.Bool
	o.Authorize = func(ctx context.Context, s tls.ConnectionState) (string, error) {
		id, e := normal(ctx, s)
		if id == "alpha" && revoked.Load() {
			return "", ErrUnavailable
		}
		return id, e
	}
	c := testCollector(t, o)
	_, a := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	_, b := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "beta"))
	s := nextSnapshot(t, c, 2)
	revoked.Store(true)
	_ = s.Providers[0].Provider.(*Connection).Close()
	receiveRun(t, a)
	nextSnapshot(t, c, 1)
	_, again := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	if e := receiveRun(t, again); !errors.Is(e, ErrProtocol) {
		t.Fatal(e)
	}
	s, _ = c.Snapshot()
	if len(s.Providers) != 1 || s.Providers[0].ID != "beta" {
		t.Fatal(s)
	}
	if _, e := s.Providers[0].Provider.GetCapabilities(context.Background()); e != nil {
		t.Fatal(e)
	}
	_ = c.Close()
	receiveRun(t, b)
}

func TestReverseMutualKeyAdmission(t *testing.T) {
	for _, mode := range reverseModes[1:] {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode, "alpha", "beta")
			c, l := countedCollector(t, collectorOptions(tr))
			var constructed atomic.Int32
			factory := func(_ context.Context, in Init) (api.Provider, Cleanup, error) {
				constructed.Add(1)
				if strings.Contains(string(in.ConfigYAML), "disposable") || strings.Contains(fmt.Sprint(in.ExecutorEnv), "disposable") {
					t.Error("secret in initialization")
				}
				return &reverseProvider{caps: func(ctx context.Context) (api.Capabilities, error) {
					md, _ := metadata.FromIncomingContext(ctx)
					if strings.Contains(fmt.Sprint(md), "disposable") {
						t.Error("secret in metadata")
					}
					return api.Capabilities{}, nil
				}}, nil, nil
			}
			for _, kind := range []string{"wrong", "unknown", "impersonate", "case", "key-case", "spaces", "short", "empty"} {
				t.Run(kind, func(t *testing.T) {
					o := runOptions(t, c, tr, "alpha")
					switch kind {
					case "wrong":
						o.MutualKey = []byte("wrong disposable key")
					case "unknown":
						o.Init.Name = "unknown"
					case "impersonate":
						o.Init.Name = "beta"
					case "case":
						o.Init.Name = "Alpha"
					case "key-case":
						o.MutualKey = []byte(strings.ToUpper(string(o.MutualKey)))
					case "spaces":
						o.MutualKey = append(o.MutualKey, ' ')
					case "short":
						o.MutualKey = o.MutualKey[:len(o.MutualKey)-1]
					case "empty":
						o.MutualKey = nil
					}
					before := l.accepts.Load()
					o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
					_, done := launchRun(t, factory, o)
					e := receiveRun(t, done)
					want := int32(1)
					if kind == "empty" {
						want = 0
					}
					expectedError := ErrProtocol
					if kind == "empty" {
						expectedError = ErrInvalid
					}
					if !errors.Is(e, expectedError) || strings.Contains(e.Error(), "disposable") || constructed.Load() != 0 || l.accepts.Load() != before+want {
						t.Fatal(e, constructed.Load(), l.accepts.Load())
					}
					eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.reserved) == 0 && len(c.pending) == 0 })
					s, _ := c.Snapshot()
					if s.Revision != 1 || len(s.Providers) != 0 {
						t.Fatal(s)
					}
				})
			}
			changes := make(chan ProviderChange, 8)
			callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { changes <- v })
			callbackEvent(t, changes)
			for _, id := range []string{"alpha", "beta"} {
				_, done := launchRun(t, factory, runOptions(t, c, tr, id))
				defer receiveAfterClose(t, c, done)
			}
			s := nextSnapshot(t, c, 2)
			if constructed.Load() != 2 {
				t.Fatal(constructed.Load())
			}
			for _, p := range s.Providers {
				if _, e := p.Provider.GetCapabilities(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			for i := 0; i < 2; i++ {
				v := callbackEvent(t, changes)
				if strings.Contains(fmt.Sprint(v), "disposable") {
					t.Fatal("secret in callback")
				}
			}
		})
	}
}
func receiveAfterClose(t *testing.T, c *Collector, done <-chan error) {
	t.Helper()
	_ = c.Close()
	receiveRun(t, done)
}

func dialKeyPeer(t *testing.T, c *Collector, tr *reverseTrust) net.Conn {
	t.Helper()
	var conn net.Conn
	var e error
	if tr.mode == "key-http" {
		conn, e = net.Dial("tcp", c.listener.Addr().String())
	} else {
		conn, e = tls.Dial("tcp", c.listener.Addr().String(), tr.client)
	}
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	return conn
}
func TestReverseMutualKeyGenericDenial(t *testing.T) {
	for _, mode := range reverseModes[1:] {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			c := testCollector(t, collectorOptions(tr))
			var previous []byte
			for _, id := range []string{"alpha", "unknown"} {
				conn := dialKeyPeer(t, c, tr)
				if e := writeKeyRequest(conn, id, []byte("wrong secret")); e != nil {
					t.Fatal(e)
				}
				b, e := io.ReadAll(conn)
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(b, []byte{'P', 'P', 'R', '3', 1, 0, 0}) || (previous != nil && !bytes.Equal(previous, b)) {
					t.Fatal(b)
				}
				previous = b
			}
		})
	}
}

func TestReverseMutualKeyTrustIsTerminal(t *testing.T) {
	for _, kind := range []string{"wrong-root", "wrong-name", "expired", "alpn"} {
		t.Run(kind, func(t *testing.T) {
			tr := modeTrust(t, "key-tls")
			if kind == "expired" {
				pair := &tr.host.Certificates[0]
				cert, e := x509.ParseCertificate(pair.Certificate[0])
				if e != nil {
					t.Fatal(e)
				}
				cert.NotBefore = time.Now().Add(-2 * time.Hour)
				cert.NotAfter = time.Now().Add(-time.Hour)
				key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				der, e := x509.CreateCertificate(rand.Reader, cert, tr.caCert, &key.PublicKey, tr.caKey)
				if e != nil {
					t.Fatal(e)
				}
				*pair = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
			}
			c, l := countedCollector(t, collectorOptions(tr))
			o := runOptions(t, c, tr, "alpha")
			switch kind {
			case "wrong-root":
				o.TLSConfig.RootCAs = testTrust(t).client.RootCAs
			case "wrong-name":
				o.TLSConfig.ServerName = "wrong.test"
			case "alpn":
				tr.host.NextProtos = []string{ReverseALPN}
				o.Address = reversePeer(t, tr, func(net.Conn) { t.Error("wrong ALPN admitted") })
			}
			o.Reconnect = RestartPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}
			var constructed atomic.Int32
			_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
				constructed.Add(1)
				return &reverseProvider{}, nil, nil
			}, o)
			if e := receiveRun(t, done); e == nil || constructed.Load() != 0 {
				t.Fatal(e, constructed.Load())
			}
			want := int32(1)
			if kind == "alpn" {
				want = 0
			}
			if l.accepts.Load() != want {
				t.Fatal("trust rejection retried", l.accepts.Load())
			}
		})
	}
}

func TestReverseMutualKeyConfigurationCopyAndRevocation(t *testing.T) {
	for _, mode := range reverseModes[1:] {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode, "alpha", "beta")
			co := collectorOptions(tr)
			original := co.MutualKeys
			c := testCollector(t, co)
			original["alpha"][0] = 'X'
			original["beta"] = []byte("replacement")
			delete(original, "alpha")
			o := runOptions(t, c, tr, "alpha")
			o.Reconnect = RestartPolicy{MaxAttempts: 1, InitialDelay: 20 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
			var constructed atomic.Int32
			_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
				constructed.Add(1)
				return &reverseProvider{}, nil, nil
			}, o)
			first := nextSnapshot(t, c, 1).Providers[0]
			o.MutualKey[0] = 'X'
			_ = first.Provider.(*Connection).socket.Close()
			eventually(t, func() bool {
				s, _ := c.Snapshot()
				return len(s.Providers) == 1 && s.Providers[0].Generation > first.Generation
			})
			if constructed.Load() != 2 {
				t.Fatal(constructed.Load())
			}
			_ = c.Close()
			receiveRun(t, done)
			// Replacement lifecycles revoke alpha's old credential while beta keeps its binding.
			co = collectorOptions(tr)
			co.MutualKeys["alpha"] = []byte("rotated alpha credential")
			replacement := testCollector(t, co)
			_, denied := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, replacement, tr, "alpha"))
			if e := receiveRun(t, denied); !errors.Is(e, ErrProtocol) {
				t.Fatal(e)
			}
			_, beta := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, replacement, tr, "beta"))
			s := nextSnapshot(t, replacement, 1)
			if s.Providers[0].ID != "beta" {
				t.Fatal(s)
			}
			if _, e := s.Providers[0].Provider.GetCapabilities(context.Background()); e != nil {
				t.Fatal(e)
			}
			_ = replacement.Close()
			receiveRun(t, beta)
		})
	}
}
