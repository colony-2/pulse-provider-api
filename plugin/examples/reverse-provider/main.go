// Command provider is a minimal independently usable plugin entry point.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
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

// The provider opens no listening socket. Credentials come only from the selected source.
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// Own a pollable stdin pipe on Unix; unsupported inputs fail with guidance.
	input := os.Stdin
	if owned, e := os.Open("/dev/stdin"); e == nil {
		input = owned
		defer input.Close()
	}
	if e := runProvider(ctx, os.Args[1:], input); e != nil && !errors.Is(e, context.Canceled) {
		fmt.Fprintln(os.Stderr, "reverse provider failed:", e)
		os.Exit(1)
	}
}
func providerOptions(ctx context.Context, args []string, input *os.File) (plugin.RunOptions, error) {
	var opts plugin.RunOptions
	flags := flag.NewFlagSet("reverse-provider", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("address", "", "collector host:port")
	name := flags.String("name", "", "authorized provider ID")
	serverName := flags.String("server-name", "", "verified collector DNS name")
	ca := flags.String("ca", "", "trusted host CA file")
	cert := flags.String("cert", "", "client certificate file")
	key := flags.String("key", "", "client certificate private key file")
	configFile := flags.String("config", "", "local YAML file")
	auth := flags.String("auth", "mtls", "mtls or mutual-key")
	transport := flags.String("transport", "tls", "tls or http (plaintext tests only)")
	keyStdin := flags.Bool("auth-key-stdin", false, "read shared secret from first stdin line")
	keyEnv := flags.String("auth-key-env", "", "environment variable containing shared secret")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return opts, plugin.ErrInvalid
	}
	a, t, e := plugin.ParseReverseModes(*auth, *transport)
	if e != nil {
		return opts, e
	}
	opts.AuthMode, opts.Transport = a, t
	if a == plugin.ReverseAuthMutualKey {
		if *keyStdin == (*keyEnv != "") {
			return opts, plugin.ErrInvalid
		}
	} else if *keyStdin || *keyEnv != "" {
		return opts, plugin.ErrInvalid
	}
	if t == plugin.ReverseTransportTLS {
		b, e := os.ReadFile(*ca)
		if e != nil {
			return opts, plugin.ErrInvalid
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return opts, plugin.ErrInvalid
		}
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{plugin.ReverseKeyALPN}, RootCAs: pool, ServerName: *serverName}
		if a == plugin.ReverseAuthMTLS {
			pair, e := tls.LoadX509KeyPair(*cert, *key)
			if e != nil {
				return opts, plugin.ErrInvalid
			}
			opts.TLSConfig.Certificates, opts.TLSConfig.NextProtos = []tls.Certificate{pair}, []string{plugin.ReverseALPN}
		}
	} else if *ca != "" || *cert != "" || *key != "" || *serverName != "" {
		return opts, plugin.ErrInvalid
	}
	configuration, e := os.ReadFile(*configFile)
	if e != nil {
		return opts, plugin.ErrInvalid
	}
	base, e := filepath.Abs(filepath.Dir(*configFile))
	if e != nil {
		return opts, plugin.ErrInvalid
	}
	opts.Address = *address
	opts.Init = plugin.Initialization{Name: *name, BaseDir: base, ConfigYAML: configuration}
	opts.Serve = plugin.ServeOptions{Implementation: "reverse-example", Version: "1.0.0"}
	if a == plugin.ReverseAuthMutualKey {
		if *keyStdin {
			opts.MutualKey, e = plugin.LoadMutualKeyInput(ctx, input, bufio.NewReader(input))
		} else {
			opts.MutualKey, e = plugin.LoadMutualKey(plugin.MutualKeySource{EnvName: *keyEnv, LookupEnv: os.LookupEnv})
		}
		if e != nil {
			return opts, e
		}
	}
	return opts, nil
}
func runProvider(ctx context.Context, args []string, input *os.File) error {
	opts, e := providerOptions(ctx, args, input)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return plugin.Run(ctx, func(ctx context.Context, in plugin.Init) (api.Provider, plugin.Cleanup, error) {
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
	}, opts)
}
func (p *provider) GetCapabilities(context.Context) (api.Capabilities, error) {
	return api.Capabilities{Bases: []api.BaseCapabilities{{Name: p.base, Capabilities: []string{}}}, BasesComplete: true}, nil
}
func (p *provider) Submit(_ context.Context, ls []api.Launch, options api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	if options.Listener != nil && (options.Listener.Context == nil || options.Listener.OnLog == nil || options.Listener.Duration < 0) {
		return nil, nil, plugin.ErrInvalid
	}
	out := make([]api.Submission, len(ls))
	for i, l := range ls {
		out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Unsupported, Reason: "example has no native executor"}
	}
	return out, unavailableLogs(ls, options), nil
}
func (p *provider) List(context.Context, api.ListRequest) (api.ListResponse, error) {
	return api.ListResponse{Items: []api.Instance{}}, nil
}

// This example has no executor or output sources. Report that explicitly.
type unavailableSubscription struct {
	coverage []api.SourceStatus
	done     chan struct{}
	reason   api.ListenerReason
	cause    error
}

func unavailableLogs(ls []api.Launch, o api.SubmitOptions) api.LogSubscription {
	if o.Listener == nil {
		return nil
	}
	s := &unavailableSubscription{done: make(chan struct{}), reason: api.ListenerSourceUnavailable, cause: api.ErrLogSource}
	if e := o.Listener.Context.Err(); e != nil {
		s.cause = e
		s.reason = api.ListenerContextCanceled
		if errors.Is(e, context.DeadlineExceeded) {
			s.reason = api.ListenerContextDeadline
		}
	}
	for _, l := range ls {
		for _, src := range []api.LogSource{api.LogSourceLaunch, api.LogSourceStdout, api.LogSourceStderr} {
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: l.LaunchID, Source: src, State: api.SourceUnavailable, Cause: api.ErrLogSource})
		}
	}
	close(s.done)
	return s
}
func (s *unavailableSubscription) Stop()                 {}
func (s *unavailableSubscription) Done() <-chan struct{} { return s.done }
func (s *unavailableSubscription) Coverage() []api.SourceStatus {
	return append([]api.SourceStatus(nil), s.coverage...)
}
func (s *unavailableSubscription) Wait(context.Context) (api.ListenerResult, error) {
	return api.ListenerResult{Reason: s.reason, Cause: s.cause, Coverage: s.Coverage()}, s.cause
}
