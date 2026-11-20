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

func TestReverseModeOptions(t *testing.T) {
	for _, mode := range reverseModes {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			co := collectorOptions(tr)
			ro := RunOptions{Address: "localhost:1234", TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}}
			normalizeRunMode(&ro, mode)
			if e := co.defaults(); e != nil {
				t.Fatal(e)
			}
			if e := ro.defaults(); e != nil {
				t.Fatal(e)
			}
			if mode == "mtls" && (co.AuthMode != ReverseAuthMTLS || co.Transport != ReverseTransportTLS || co.TLSConfig.NextProtos[0] != ReverseALPN) {
				t.Fatal("legacy defaults changed")
			}
			if mode == "key-tls" && (co.TLSConfig.NextProtos[0] != ReverseKeyALPN || ro.TLSConfig.NextProtos[0] != ReverseKeyALPN) {
				t.Fatal("key ALPN")
			}
		})
	}
	collectorCases := map[string]func(*CollectorOptions){
		"unknown-auth":      func(o *CollectorOptions) { o.AuthMode = 42 },
		"unknown-transport": func(o *CollectorOptions) { o.Transport = 42 },
		"missing-map":       func(o *CollectorOptions) { o.MutualKeys = nil },
		"empty-map":         func(o *CollectorOptions) { o.MutualKeys = map[string][]byte{} },
		"bad-unused-id":     func(o *CollectorOptions) { o.MutualKeys["bad\n"] = []byte("secret") },
		"bad-unused-key":    func(o *CollectorOptions) { o.MutualKeys["unused"] = []byte("\r") },
		"authorizer": func(o *CollectorOptions) {
			o.Authorize = func(context.Context, tls.ConnectionState) (string, error) { return "alpha", nil }
		},
		"nil-tls":            func(o *CollectorOptions) { o.TLSConfig = nil },
		"legacy-alpn":        func(o *CollectorOptions) { o.TLSConfig.NextProtos = []string{ReverseALPN} },
		"extra-alpn":         func(o *CollectorOptions) { o.TLSConfig.NextProtos = append(o.TLSConfig.NextProtos, "h2") },
		"client-cert-policy": func(o *CollectorOptions) { o.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert },
		"no-server-cert":     func(o *CollectorOptions) { o.TLSConfig.Certificates = nil },
		"old-tls":            func(o *CollectorOptions) { o.TLSConfig.MinVersion = tls.VersionTLS12 },
		"insecure":           func(o *CollectorOptions) { o.TLSConfig.InsecureSkipVerify = true },
		"dynamic": func(o *CollectorOptions) {
			o.TLSConfig.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil }
		},
		"http-with-tls": func(o *CollectorOptions) { o.Transport = ReverseTransportHTTP },
		"mtls-with-key": func(o *CollectorOptions) {
			*o = collectorOptions(testTrust(t))
			o.MutualKeys = map[string][]byte{"alpha": testMutualKey("alpha")}
		},
		"mtls-no-authorizer": func(o *CollectorOptions) { *o = collectorOptions(testTrust(t)); o.Authorize = nil },
		"mtls-http": func(o *CollectorOptions) {
			*o = collectorOptions(testTrust(t))
			o.AuthMode = ReverseAuthMTLS
			o.Transport = ReverseTransportHTTP
			o.TLSConfig = nil
			o.MutualKeys = nil
		},
	}
	for name, mutate := range collectorCases {
		t.Run("collector/"+name, func(t *testing.T) {
			o := collectorOptions(modeTrust(t, "key-tls"))
			mutate(&o)
			if e := o.defaults(); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
		})
	}
	runnerCases := map[string]func(*RunOptions){
		"unknown-auth":      func(o *RunOptions) { o.AuthMode = 42 },
		"unknown-transport": func(o *RunOptions) { o.Transport = 42 },
		"empty-key":         func(o *RunOptions) { o.MutualKey = nil },
		"multiline-key":     func(o *RunOptions) { o.MutualKey = []byte("secret\n") },
		"oversized-key":     func(o *RunOptions) { o.MutualKey = make([]byte, 4097) },
		"mtls-key":          func(o *RunOptions) { o.AuthMode = ReverseAuthMTLS; o.TLSConfig = testTrust(t).client },
		"mtls-no-certificate": func(o *RunOptions) {
			o.AuthMode = ReverseAuthMTLS
			o.MutualKey = nil
			o.TLSConfig = testTrust(t).client
			o.TLSConfig.Certificates = nil
		},
		"http-tls": func(o *RunOptions) { o.Transport = ReverseTransportHTTP },
		"mtls-http": func(o *RunOptions) {
			o.AuthMode = ReverseAuthMTLS
			o.Transport = ReverseTransportHTTP
			o.TLSConfig = nil
			o.MutualKey = nil
		},
		"no-tls":      func(o *RunOptions) { o.TLSConfig = nil },
		"insecure":    func(o *RunOptions) { o.TLSConfig.InsecureSkipVerify = true },
		"no-root":     func(o *RunOptions) { o.TLSConfig.RootCAs = nil },
		"no-name":     func(o *RunOptions) { o.TLSConfig.ServerName = "" },
		"legacy-alpn": func(o *RunOptions) { o.TLSConfig.NextProtos = []string{ReverseALPN} },
	}
	for name, mutate := range runnerCases {
		t.Run("runner/"+name, func(t *testing.T) {
			tr := modeTrust(t, "key-tls")
			o := RunOptions{Address: "localhost:1234", TLSConfig: tr.client, Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}}
			normalizeRunMode(&o, "key-tls")
			mutate(&o)
			if e := Run(context.Background(), factoryFor(&reverseProvider{}), o); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
		})
	}
}

func TestReverseKeyTrustPoolsAreCopied(t *testing.T) {
	tr := modeTrust(t, "key-tls")
	co := collectorOptions(tr)
	ro := RunOptions{Address: "localhost:1234", TLSConfig: tr.client, AuthMode: ReverseAuthMutualKey, MutualKey: testMutualKey("alpha"), Init: Initialization{Name: "alpha", BaseDir: t.TempDir()}}
	before := tr.client.RootCAs.Clone()
	if e := co.defaults(); e != nil {
		t.Fatal(e)
	}
	if e := ro.defaults(); e != nil {
		t.Fatal(e)
	}
	tr.client.RootCAs.AddCert(testTrust(t).caCert)
	if !ro.TLSConfig.RootCAs.Equal(before) || !co.TLSConfig.ClientCAs.Equal(before) || ro.TLSConfig.RootCAs.Equal(tr.client.RootCAs) {
		t.Fatal("caller mutation changed trusted roots")
	}
}
