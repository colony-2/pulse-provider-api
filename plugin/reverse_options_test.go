package plugin

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestReverseOptions(t *testing.T) {
	tr := testTrust(t)
	for _, mutate := range []func(*tls.Config){func(c *tls.Config) { c.InsecureSkipVerify = true }, func(c *tls.Config) { c.MinVersion = tls.VersionTLS12 }, func(c *tls.Config) { c.NextProtos = []string{"h2"} }, func(c *tls.Config) {
		c.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return c, nil }
	}, func(c *tls.Config) { c.RootCAs = nil }, func(c *tls.Config) { c.ServerName = "" }} {
		c := tr.client.Clone()
		mutate(c)
		o := RunOptions{Address: "localhost:1234", TLSConfig: c, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}}
		if e := Run(context.Background(), factoryFor(&reverseProvider{}), o); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	for _, mutate := range []func(*RunOptions){func(o *RunOptions) { o.Address = "bad" }, func(o *RunOptions) { o.Init.Name = "bad\n" }, func(o *RunOptions) { o.Init.BaseDir = "relative" }, func(o *RunOptions) { o.Serve.Concurrency = -1 }, func(o *RunOptions) { o.StartupTimeout = -time.Second }, func(o *RunOptions) {
		o.Reconnect = RestartPolicy{MaxAttempts: 1, InitialDelay: time.Second, MaxDelay: time.Millisecond}
	}, func(o *RunOptions) { o.Reconnect.Multiplier = 0.5 }} {
		o := RunOptions{Address: "localhost:1234", TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}}
		mutate(&o)
		if e := Run(context.Background(), factoryFor(&reverseProvider{}), o); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	host := tr.host.Clone()
	host.ClientAuth = tls.RequireAnyClientCert
	o := collectorOptions(tr)
	o.TLSConfig = host
	if _, e := StartCollector(context.Background(), nil, o); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	for _, b := range []string{"", "\x00", "\xff", string(make([]byte, 257))} {
		if validIdentity(b) {
			t.Fatal("bad identity")
		}
	}
}
