package plugin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

type reverseTrust struct {
	caKey  *ecdsa.PrivateKey
	caCert *x509.Certificate

	host, client *tls.Config
	ca           []byte
	certs        map[string][]byte
	keys         map[string][]byte
}

func testTrust(t *testing.T, names ...string) *reverseTrust {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	ca, e = x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	trust := &reverseTrust{caKey: key, caCert: ca, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), certs: map[string][]byte{}, keys: map[string][]byte{}}
	makeCert := func(name string, serial int64, host bool) tls.Certificate {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		c := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if host {
			c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		b, err := x509.CreateCertificate(rand.Reader, c, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		pk, _ := x509.MarshalPKCS8PrivateKey(k)
		trust.certs[name] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b})
		trust.keys[name] = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})
		pair, err := tls.X509KeyPair(trust.certs[name], trust.keys[name])
		if err != nil {
			t.Fatal(err)
		}
		return pair
	}
	host := makeCert("collector.test", 2, true)
	trust.host = &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{reverseALPN}, Certificates: []tls.Certificate{host}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	if len(names) == 0 {
		names = []string{"alpha"}
	}
	for i, name := range names {
		c := makeCert(name, int64(i+3), false)
		if i == 0 {
			trust.client = &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{reverseALPN}, Certificates: []tls.Certificate{c}, RootCAs: pool, ServerName: "collector.test"}
		}
	}
	return trust
}
func (tr *reverseTrust) clientFor(t *testing.T, name string) *tls.Config {
	t.Helper()
	c := tr.client.Clone()
	pair, e := tls.X509KeyPair(tr.certs[name], tr.keys[name])
	if e != nil {
		t.Fatal(e)
	}
	c.Certificates = []tls.Certificate{pair}
	return c
}
func (tr *reverseTrust) files(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for n, b := range tr.certs {
		if e := os.WriteFile(filepath.Join(dir, n+".crt"), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	for n, b := range tr.keys {
		if e := os.WriteFile(filepath.Join(dir, n+".key"), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(dir, "ca.crt"), tr.ca, 0600); e != nil {
		t.Fatal(e)
	}
	return dir
}
func collectorOptions(tr *reverseTrust) CollectorOptions {
	return CollectorOptions{TLSConfig: tr.host, Authorize: func(_ context.Context, s tls.ConnectionState) (string, error) {
		return s.PeerCertificates[0].Subject.CommonName, nil
	}, StartupTimeout: time.Second, ShutdownTimeout: 500 * time.Millisecond, CallTimeout: time.Second, ProbeInterval: 20 * time.Millisecond, ProbeTimeout: 100 * time.Millisecond}
}
func testCollector(t *testing.T, o CollectorOptions) *Collector {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	c, e := StartCollector(context.Background(), l, o)
	if e != nil {
		l.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func runOptions(t *testing.T, c *Collector, tr *reverseTrust, name string) RunOptions {
	t.Helper()
	return RunOptions{Address: c.listener.Addr().String(), TLSConfig: tr.clientFor(t, name), Init: Initialization{Name: name, BaseDir: t.TempDir(), ConfigYAML: []byte("base: test\n"), ExecutorEnv: map[string]string{"EXECUTOR_ONLY": "local"}}, StartupTimeout: time.Second, Serve: ServeOptions{ShutdownTimeout: 200 * time.Millisecond, FactoryTimeout: 500 * time.Millisecond}}
}
func launchRun(t *testing.T, f Factory, o RunOptions) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f, o) }()
	t.Cleanup(cancel)
	return cancel, done
}
func nextSnapshot(t *testing.T, c *Collector, n int) ProviderSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, e := c.Snapshot()
	for e == nil && len(s.Providers) != n {
		s, e = c.WaitForChange(ctx, s.Revision)
	}
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func receiveRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case e := <-done:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
		return nil
	}
}

type reverseProvider struct {
	caps           func(context.Context) (api.Capabilities, error)
	submitWithLogs func(context.Context, []api.Launch, api.SubmitOptions) ([]api.Submission, api.LogSubscription, error)
	submit         func(context.Context, []api.Launch) ([]api.Submission, error)
	list           func(context.Context, api.ListRequest) (api.ListResponse, error)
	calls          atomic.Int32
}

func (p *reverseProvider) GetCapabilities(ctx context.Context) (api.Capabilities, error) {
	if p.caps != nil {
		return p.caps(ctx)
	}
	return api.Capabilities{BasesComplete: true, Bases: []api.BaseCapabilities{{Name: "test"}}}, nil
}
func (p *reverseProvider) Submit(ctx context.Context, ls []api.Launch, options api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	p.calls.Add(1)
	if p.submitWithLogs != nil {
		return p.submitWithLogs(ctx, ls, options)
	}
	if p.submit != nil {
		r, e := p.submit(ctx, ls)
		return r, nil, e
	}
	out := make([]api.Submission, len(ls))
	for i, l := range ls {
		out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Accepted}
	}
	return out, nil, nil
}
func (p *reverseProvider) List(ctx context.Context, q api.ListRequest) (api.ListResponse, error) {
	if p.list != nil {
		return p.list(ctx, q)
	}
	return api.ListResponse{}, nil
}
func factoryFor(p api.Provider) Factory {
	return func(context.Context, Init) (api.Provider, Cleanup, error) { return p, nil, nil }
}
