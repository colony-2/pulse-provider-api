package plugin

import (
	"context"
	"crypto/tls"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// CollectorOptions configures authenticated provider collection. Authorize maps
// an ordinarily verified certificate to a configured, case-sensitive identity.
type CollectorOptions struct {
	TLSConfig                                                         *tls.Config
	Authorize                                                         func(context.Context, tls.ConnectionState) (string, error)
	StartupTimeout, CallTimeout, ShutdownTimeout                      time.Duration
	ProbeInterval, ProbeTimeout                                       time.Duration
	Concurrency, MaxMessageBytes                                      int
	MaxProviders, MaxPendingConnections, MaxWaiters, MaxMetadataBytes int
	MaxSubscriptions, CallbackQueue                                   int
	CallbackTimeout                                                   time.Duration
}

// RunOptions constructs providers using local configuration, never host paths.
// Reconnect is disabled by default and applies only to transient transport loss.
type RunOptions struct {
	Address        string
	TLSConfig      *tls.Config
	Init           Initialization
	Serve          ServeOptions
	StartupTimeout time.Duration
	Reconnect      RestartPolicy
}

func (o *CollectorOptions) defaults() error {
	var e error
	if o.Authorize == nil {
		return ErrInvalid
	}
	if o.TLSConfig, e = reverseTLS(o.TLSConfig, true); e != nil {
		return e
	}
	if e = limits(&o.MaxMessageBytes, &o.Concurrency); e != nil {
		return e
	}
	for _, d := range []struct {
		p *time.Duration
		v time.Duration
	}{
		{&o.StartupTimeout, 30 * time.Second}, {&o.CallTimeout, 30 * time.Second}, {&o.ShutdownTimeout, 5 * time.Second}, {&o.ProbeInterval, 10 * time.Second}, {&o.ProbeTimeout, 5 * time.Second}, {&o.CallbackTimeout, 5 * time.Second},
	} {
		if e = duration(d.p, d.v); e != nil {
			return e
		}
	}

	for _, b := range []struct {
		v        *int
		def, max int
	}{{&o.MaxProviders, 128, 65536}, {&o.MaxPendingConnections, 32, 65536}, {&o.MaxWaiters, 1024, 1000000}, {&o.MaxMetadataBytes, 16 << 10, 1 << 20}, {&o.MaxSubscriptions, 32, 1024}, {&o.CallbackQueue, 64, 4096}} {
		if *b.v == 0 {
			*b.v = b.def
		}
		if *b.v < 1 || *b.v > b.max {
			return ErrInvalid
		}
	}
	return nil
}
func copyInitialization(in Initialization) Initialization {
	in.ConfigYAML = append([]byte(nil), in.ConfigYAML...)
	in.ExecutorEnv = copyMap(in.ExecutorEnv)
	return in
}
func (o *RunOptions) defaults() error {
	h, p, e := net.SplitHostPort(o.Address)
	port, pe := strconv.Atoi(p)
	if e != nil || h == "" || pe != nil || port < 1 || port > 65535 {
		return ErrInvalid
	}
	if !validIdentity(o.Init.Name) || !filepath.IsAbs(o.Init.BaseDir) || len(o.Init.ConfigYAML) > MaxConfigBytes {
		return ErrInvalid
	}
	for k, v := range o.Init.ExecutorEnv {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return ErrInvalid
		}
	}
	if o.TLSConfig, e = reverseTLS(o.TLSConfig, false); e != nil {
		return e
	}
	if e = o.Serve.defaults(); e != nil {
		return e
	}
	if e = duration(&o.StartupTimeout, 30*time.Second); e != nil {
		return e
	}
	if e = o.Reconnect.defaults(); e != nil {
		return e
	}

	o.Init = copyInitialization(o.Init)
	return nil
}
