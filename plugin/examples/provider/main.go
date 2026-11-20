// Command provider is a minimal independently usable plugin entry point.
package main

import (
	"context"
	"errors"
	"os"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

type config struct {
	Base string `yaml:"base"`
}
type provider struct{ base string }

func main() {
	err := plugin.Serve(func(ctx context.Context, in plugin.Init) (api.Provider, plugin.Cleanup, error) {
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
	}, plugin.ServeOptions{Implementation: "example", Version: "1.0.0"})
	if err != nil {
		os.Exit(1)
	}
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
