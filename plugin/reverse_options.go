package plugin

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ReverseAuth selects how the collector authenticates a provider.
type ReverseAuth uint8

const (
	ReverseAuthMTLS ReverseAuth = iota
	ReverseAuthMutualKey
)

// ReverseTransport selects encryption for the reverse HTTP/2 socket.
type ReverseTransport uint8

const (
	ReverseTransportTLS ReverseTransport = iota
	// ReverseTransportHTTP exposes credentials and traffic; use only for tests.
	ReverseTransportHTTP
)

func reverseMode(auth ReverseAuth, transport ReverseTransport) error {
	if auth > ReverseAuthMutualKey || transport > ReverseTransportHTTP || (auth == ReverseAuthMTLS && transport == ReverseTransportHTTP) {
		return ErrInvalid
	}
	return nil
}
func reverseProtocol(auth ReverseAuth) string {
	if auth == ReverseAuthMutualKey {
		return ReverseKeyALPN
	}
	return ReverseALPN
}

// CollectorOptions configures authenticated provider collection. Authorize maps
// an ordinarily verified certificate to a configured, case-sensitive identity.
type CollectorOptions struct {
	LogOptions LogOptions
	AuthMode   ReverseAuth
	Transport  ReverseTransport
	// MutualKeys binds each allowed identity to an exact shared secret. Copied and
	// hashed at startup; rotation requires replacing the collector and sessions.
	MutualKeys                                                        map[string][]byte
	keyDigests                                                        map[string][sha256.Size]byte
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
	AuthMode  ReverseAuth
	Transport ReverseTransport
	// MutualKey is copied before the first connection. Run never reads process input.
	MutualKey      []byte
	Address        string
	TLSConfig      *tls.Config
	Init           Initialization
	Serve          ServeOptions
	StartupTimeout time.Duration
	Reconnect      RestartPolicy
}

func (o *CollectorOptions) defaults() error {
	if e := o.LogOptions.defaults(); e != nil {
		return e
	}
	var e error
	if e = reverseMode(o.AuthMode, o.Transport); e != nil {
		return e
	}
	if o.AuthMode == ReverseAuthMTLS {
		if o.Authorize == nil || len(o.MutualKeys) != 0 {
			return ErrInvalid
		}
	} else {
		if o.Authorize != nil || len(o.MutualKeys) == 0 {
			return ErrInvalid
		}
		o.keyDigests = make(map[string][sha256.Size]byte, len(o.MutualKeys))
		for id, key := range o.MutualKeys {
			if !validIdentity(id) || !validMutualKey(key) {
				return ErrInvalid
			}
			o.keyDigests[id] = sha256.Sum256(key)
		}
		o.MutualKeys = nil
	}
	if o.TLSConfig, e = reverseTLS(o.TLSConfig, true, o.AuthMode, o.Transport); e != nil {
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
	if e = reverseMode(o.AuthMode, o.Transport); e != nil {
		return e
	}
	if o.AuthMode == ReverseAuthMTLS {
		if len(o.MutualKey) != 0 {
			return ErrInvalid
		}
	} else {
		if !validMutualKey(o.MutualKey) {
			return ErrInvalid
		}
		o.MutualKey = append([]byte(nil), o.MutualKey...)
	}
	if o.TLSConfig, e = reverseTLS(o.TLSConfig, false, o.AuthMode, o.Transport); e != nil {
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

// ParseReverseModes parses the example CLI selectors without accessing input.
func ParseReverseModes(auth, transport string) (ReverseAuth, ReverseTransport, error) {
	var a ReverseAuth
	var t ReverseTransport
	switch auth {
	case "mtls":
		a = ReverseAuthMTLS
	case "mutual-key":
		a = ReverseAuthMutualKey
	default:
		return 0, 0, ErrInvalid
	}
	switch transport {
	case "tls":
		t = ReverseTransportTLS
	case "http":
		t = ReverseTransportHTTP
	default:
		return 0, 0, ErrInvalid
	}
	return a, t, reverseMode(a, t)
}
