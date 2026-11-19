// Command provider is a minimal independently usable plugin entry point.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

type config struct {
	Base string `yaml:"base"`
}
type provider struct{ base string }

// Supply explicit certificate, key, CA and configuration files. This command
// opens no listening socket; all SDK traffic uses its outgoing TLS connection.
func main() {
	address := flag.String("address", "", "collector host:port")
	name := flag.String("name", "", "authorized provider ID")
	serverName := flag.String("server-name", "", "verified collector DNS name")
	ca := flag.String("ca", "", "trusted host CA file")
	cert := flag.String("cert", "", "client certificate file")
	key := flag.String("key", "", "client private key file")
	configFile := flag.String("config", "", "local YAML file")
	flag.Parse()
	pair, e := tls.LoadX509KeyPair(*cert, *key)
	if e != nil {
		os.Exit(1)
	}
	b, e := os.ReadFile(*ca)
	if e != nil {
		os.Exit(1)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		os.Exit(1)
	}
	configuration, e := os.ReadFile(*configFile)
	if e != nil {
		os.Exit(1)
	}
	base, e := filepath.Abs(filepath.Dir(*configFile))
	if e != nil {
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := plugin.Run(ctx, func(ctx context.Context, in plugin.Init) (api.Provider, plugin.Cleanup, error) {
		var cfg config
		if err := plugin.DecodeConfig(in.ConfigYAML, &cfg); err != nil {
			return nil, nil, err
		}
		// This demonstration cannot apply executor defaults or launch native work.
		// A real provider explicitly applies defaults to its own detached executor.
		if len(in.ExecutorEnv) != 0 {
			return nil, nil, errors.New("executor defaults unsupported")
		}
		if cfg.Base == "" {
			return nil, nil, errors.New("base required")
		}
		return &provider{base: cfg.Base}, func(context.Context) error { return nil }, nil
	}, plugin.RunOptions{Address: *address, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{plugin.ReverseALPN}, RootCAs: pool, ServerName: *serverName, Certificates: []tls.Certificate{pair}}, Init: plugin.Initialization{Name: *name, BaseDir: base, ConfigYAML: configuration}, Serve: plugin.ServeOptions{Implementation: "reverse-example", Version: "1.0.0"}})
	if err != nil && !errors.Is(err, context.Canceled) {
		os.Exit(1)
	}
}
func (p *provider) GetCapabilities(context.Context) (api.Capabilities, error) {
	return api.Capabilities{Bases: []api.BaseCapabilities{{Name: p.base, Capabilities: []string{}}}, BasesComplete: true}, nil
}
func (p *provider) Submit(_ context.Context, ls []api.Launch) ([]api.Submission, error) {
	out := make([]api.Submission, len(ls))
	for i, l := range ls {
		out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Unsupported, Reason: "example has no native executor"}
	}
	return out, nil
}
func (p *provider) List(context.Context, api.ListRequest) (api.ListResponse, error) {
	return api.ListResponse{Items: []api.Instance{}}, nil
}
