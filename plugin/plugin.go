// Package plugin serves and hosts out-of-process providers over authenticated
// baseline-2 gRPC. It has no dependency on Pulse controller or configuration code.
// Accepted native work and its runtime enforcement remain provider responsibilities.
package plugin

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	provider "github.com/colony-2/pulse-provider-api"
)

const (
	Protocol              = 2
	DefaultMessageBytes   = 16 << 20
	MaxMessageBytes       = 128 << 20
	MaxConfigBytes        = 1 << 20
	MaxObservationBytes   = 64 << 10
	MaxObservationEntries = 256
	FailureProcessExit    = "process_exit"
	FailureProbe          = "probe_failed"
	FailureStartup        = "startup_failed"
)

var (
	ErrInvalid     = errors.New("plugin: invalid input")
	ErrUnavailable = errors.New("plugin: unavailable")
	ErrSaturated   = errors.New("plugin: concurrency limit")
	ErrTransport   = errors.New("plugin: transport failure; execution may have started")
	ErrProtocol    = errors.New("plugin: incompatible protocol or execution semantics")
	ErrTooLarge    = errors.New("plugin: message limit exceeded")
	ErrCleanup     = errors.New("plugin: cleanup failed or exceeded shutdown bound")
	ErrCallback    = errors.New("plugin: callback failed or exceeded delivery bound")
)

// Initialization is copied across the process boundary. ExecutorEnv supplies
// native executor defaults, never plugin ambient variables. The factory must
// explicitly apply these defaults or reject them.
type Initialization struct {
	Name        string
	ConfigYAML  []byte
	BaseDir     string
	ExecutorEnv map[string]string
}
type Init struct {
	Initialization
	// Lifetime bounds provider maintenance, independently of factory/RPC contexts.
	// Detached accepted execution must not derive its authority from this context.
	Lifetime context.Context
}
type Cleanup func(context.Context) error
type Factory func(context.Context, Init) (provider.Provider, Cleanup, error)

type ServeOptions struct {
	Implementation  string
	Version         string
	MaxMessageBytes int
	FactoryTimeout  time.Duration
	ShutdownTimeout time.Duration
	Concurrency     int
}

type RestartPolicy struct {
	MaxAttempts          int
	InitialDelay         time.Duration
	MaxDelay             time.Duration
	Multiplier           float64
	JitterFraction       float64
	HealthyResetInterval time.Duration
}

type StartOptions struct {
	Executable string
	Args       []string
	// Env is the complete selected child environment, not additions to os.Environ.
	Env             map[string]string
	SHA256          string
	Init            Initialization
	StartupTimeout  time.Duration
	CallTimeout     time.Duration
	ShutdownTimeout time.Duration
	Concurrency     int
	MaxMessageBytes int
	ProbeInterval   time.Duration
	ProbeTimeout    time.Duration
	Restart         RestartPolicy
}

// Health contains SDK codes only. Raw errors and private submission observations
// must not be added to public health output.
type Health struct {
	Name           string `json:"name"`
	Implementation string `json:"implementation"`
	Version        string `json:"version"`
	Protocol       int    `json:"protocol"`
	State          string `json:"state"`
	RestartCount   int    `json:"restart_count"`
	Generation     uint64 `json:"generation"`
	LastFailure    string `json:"last_failure,omitempty"`
}

type ErrorObservation struct {
	Present   bool
	Class     string
	Text      string
	Truncated bool
}
type OutcomeObservation struct {
	LaunchID  string
	Status    string
	Reason    string
	Truncated bool
}

// SubmissionObservations is private diagnostic output from this invocation only.
// Provider authors must keep secrets out of their returned reasons and errors.
type SubmissionObservations struct {
	Invocation uint64
	Generation uint64
	// Capture is complete, truncated, or not_captured.
	Capture     string
	Outcomes    []OutcomeObservation
	MethodError ErrorObservation
}

func duration(v *time.Duration, d time.Duration) error {
	if *v < 0 {
		return ErrInvalid
	}
	if *v == 0 {
		*v = d
	}
	return nil
}
func limits(m, c *int) error {
	if *m == 0 {
		*m = DefaultMessageBytes
	}
	if *c == 0 {
		*c = 4
	}
	if *m < 1024 || *m > MaxMessageBytes || *c < 1 || *c > 65536 {
		return ErrInvalid
	}
	return nil
}
func (o *ServeOptions) defaults() error {
	if err := limits(&o.MaxMessageBytes, &o.Concurrency); err != nil {
		return err
	}
	if err := duration(&o.FactoryTimeout, 30*time.Second); err != nil {
		return err
	}
	return duration(&o.ShutdownTimeout, 5*time.Second)
}
func (o *StartOptions) defaults() error {
	if !filepath.IsAbs(o.Executable) || o.Init.Name == "" || !filepath.IsAbs(o.Init.BaseDir) || len(o.Init.ConfigYAML) > MaxConfigBytes {
		return ErrInvalid
	}
	if err := limits(&o.MaxMessageBytes, &o.Concurrency); err != nil {
		return err
	}
	for _, d := range []struct {
		p *time.Duration
		v time.Duration
	}{
		{&o.StartupTimeout, 30 * time.Second}, {&o.CallTimeout, 30 * time.Second}, {&o.ShutdownTimeout, 5 * time.Second}, {&o.ProbeInterval, 10 * time.Second}, {&o.ProbeTimeout, 5 * time.Second},
	} {
		if err := duration(d.p, d.v); err != nil {
			return err
		}
	}
	for k, v := range o.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) || strings.HasPrefix(k, "PLUGIN_") || strings.HasPrefix(k, "PULSE_PLUGIN_") || k == "TMPDIR" {
			return ErrInvalid
		}
	}
	return o.Restart.defaults()
}

func (r *RestartPolicy) defaults() error {
	if r.MaxAttempts < 0 || r.InitialDelay < 0 || r.MaxDelay < 0 || r.HealthyResetInterval < 0 || math.IsNaN(r.Multiplier) || math.IsInf(r.Multiplier, 0) || math.IsNaN(r.JitterFraction) || r.JitterFraction < 0 || r.JitterFraction > 1 {
		return ErrInvalid
	}
	if r.InitialDelay == 0 {
		r.InitialDelay = time.Second
	}
	if r.MaxDelay == 0 {
		r.MaxDelay = 30 * time.Second
	}
	if r.Multiplier == 0 {
		r.Multiplier = 2
	}
	if r.MaxDelay < r.InitialDelay || r.Multiplier < 1 {
		return ErrInvalid
	}
	return nil
}

// ResolvePath resolves against an explicit base; it is not a containment sandbox.
func ResolvePath(baseDir, path string) (string, error) {
	if !filepath.IsAbs(baseDir) {
		return "", ErrInvalid
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	return filepath.Join(baseDir, path), nil
}

func safeError(code error, cause error) error {
	// Avoid exposing provider output, argv, configuration, leases or transport text.
	if errors.Is(cause, context.Canceled) {
		return fmt.Errorf("%w: %w", code, context.Canceled)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", code, context.DeadlineExceeded)
	}
	return code
}
