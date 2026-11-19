package plugin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
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
				o.Address = reversePeer(t, tr, func(*tls.Conn) { t.Error("wrong ALPN authenticated") })
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
