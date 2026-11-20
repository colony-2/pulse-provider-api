// Command reverse-host starts one collector and reads a fresh provider snapshot
// for each "invoke" line on stdin. Real hosts inject ProviderCollector into their
// own placement logic and preserve submission uncertainty on every invocation.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/colony-2/pulse-provider-api/plugin"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "provider collection failed:", e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// Reopening inherited Unix stdin registers pipes with Go's poller, including
	// Go 1.22 where os.Stdin itself does not support read deadlines.
	input := os.Stdin
	if owned, e := os.Open("/dev/stdin"); e == nil {
		input = owned
		defer input.Close()
	}
	return runHost(ctx, os.Args[1:], input, os.Stdout)
}

func hostOptions(ctx context.Context, args []string, input *os.File, reader *bufio.Reader) (string, plugin.CollectorOptions, error) {
	var opts plugin.CollectorOptions
	flags := flag.NewFlagSet("reverse-host", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("listen", "127.0.0.1:0", "reachable TCP address")
	ca := flags.String("ca", "", "trusted provider CA file")
	cert := flags.String("cert", "", "host certificate file")
	key := flags.String("key", "", "host certificate private key file")
	identities := flags.String("identities", "", "comma-separated authorized certificate DNS names")
	auth := flags.String("auth", "mtls", "mtls or mutual-key")
	transport := flags.String("transport", "tls", "tls or http (plaintext tests only)")
	keyStdin := flags.Bool("auth-key-stdin", false, "read shared secret from first stdin line")
	keyEnv := flags.String("auth-key-env", "", "environment variable containing shared secret")
	keyID := flags.String("auth-key-id", "", "expected provider identity in key mode")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return "", opts, plugin.ErrInvalid
	}
	a, t, e := plugin.ParseReverseModes(*auth, *transport)
	if e != nil {
		return "", opts, e
	}
	opts.AuthMode, opts.Transport = a, t
	identityFlag := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "identities" {
			identityFlag = true
		}
	})
	if a == plugin.ReverseAuthMutualKey {
		if identityFlag || *keyID == "" || (*keyStdin == (*keyEnv != "")) {
			return "", opts, plugin.ErrInvalid
		}
	} else if *keyStdin || *keyEnv != "" || *keyID != "" {
		return "", opts, plugin.ErrInvalid
	}
	if t == plugin.ReverseTransportTLS {
		pair, e := tls.LoadX509KeyPair(*cert, *key)
		if e != nil {
			return "", opts, plugin.ErrInvalid
		}
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, NextProtos: []string{plugin.ReverseKeyALPN}}
		if a == plugin.ReverseAuthMTLS {
			b, e := os.ReadFile(*ca)
			if e != nil {
				return "", opts, plugin.ErrInvalid
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				return "", opts, plugin.ErrInvalid
			}
			opts.TLSConfig.NextProtos = []string{plugin.ReverseALPN}
			opts.TLSConfig.ClientCAs, opts.TLSConfig.ClientAuth = pool, tls.RequireAndVerifyClientCert
			allowed := strings.Split(*identities, ",")
			opts.Authorize = func(_ context.Context, s tls.ConnectionState) (string, error) {
				for _, id := range allowed {
					if id != "" && s.PeerCertificates[0].VerifyHostname(id) == nil {
						return id, nil
					}
				}
				return "", plugin.ErrUnavailable
			}
		}
	} else if *ca != "" || *cert != "" || *key != "" {
		return "", opts, plugin.ErrInvalid
	}
	if a == plugin.ReverseAuthMutualKey {
		var secret []byte
		if *keyStdin {
			secret, e = plugin.LoadMutualKeyInput(ctx, input, reader)
		} else {
			secret, e = plugin.LoadMutualKey(plugin.MutualKeySource{EnvName: *keyEnv, LookupEnv: os.LookupEnv})
		}
		if e != nil {
			return "", opts, e
		}
		opts.MutualKeys = map[string][]byte{*keyID: secret}
	}
	return *address, opts, nil
}

func runHost(ctx context.Context, args []string, input *os.File, stdout io.Writer) error {
	reader := bufio.NewReader(input)
	address, opts, e := hostOptions(ctx, args, input, reader)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	listener, e := net.Listen("tcp", address)
	if e != nil {
		return e
	}
	collector, e := plugin.StartCollector(ctx, listener, opts)
	if e != nil {
		listener.Close()
		return e
	}
	defer collector.Close()
	output := &lockedWriter{Writer: stdout}
	fmt.Fprintln(output, "listening="+listener.Addr().String())
	observerContext, cancelObserver := context.WithCancel(context.Background())
	defer cancelObserver()
	stopped := make(chan error, 3)
	subscription, e := collector.OnProvidersChanged(observerContext, func(_ context.Context, change plugin.ProviderChange) {
		if _, e := fmt.Fprintln(output, changeLine(change)); e != nil {
			select {
			case stopped <- e:
			default:
			}
		}
	})
	if e != nil {
		return e
	}
	defer subscription.Cancel()
	// Monitor delivery independently. Do not silently resubscribe across a gap.
	go func() {
		if e := subscription.Wait(); e != nil {
			stopped <- e
		}
	}()
	// The application supervisor must also observe fatal collection errors.
	commandContext, cancelCommands := context.WithCancel(ctx)
	defer cancelCommands()
	defer input.Close()
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-commandContext.Done():
				return
			}
		}
	}()
	go func() { stopped <- collector.Wait() }()
	if e = serveInvocations(collector, lines, stopped, output); e != nil {
		return e
	}
	// Keep the observer context live through final delivery and transport cleanup.
	if e = collector.Close(); e != nil {
		return e
	}
	return subscription.Wait()
}

type lockedWriter struct {
	sync.Mutex
	io.Writer
}

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.Lock()
	defer w.Unlock()
	return w.Writer.Write(b)
}

func changeLine(v plugin.ProviderChange) string {
	entries := func(ps []plugin.AvailableProvider) string {
		out := make([]string, 0, len(ps))
		for _, p := range ps {
			out = append(out, fmt.Sprintf("%s/%d", p.ID, p.Generation))
		}
		return strings.Join(out, ",")
	}
	return fmt.Sprintf("change kind=%s revision=%d reason=%s joined=%s left=%s", v.Kind, v.Revision, v.Reason, entries(v.Joined), entries(v.Left))
}

func serveInvocations(source plugin.ProviderCollector, lines <-chan string, stopped <-chan error, output io.Writer) error {
	for {
		select {
		case e := <-stopped:
			return e
		case line, ok := <-lines:
			if !ok || line == "quit" {
				return nil
			}
			if line != "invoke" {
				return plugin.ErrInvalid
			}
			snapshot, e := source.Snapshot()
			if e != nil {
				return e
			}
			result, e := invocation(context.Background(), snapshot)
			if e != nil {
				// A snapshot is not a reservation: a provider can leave before
				// discovery. Fail this invocation and keep collecting providers.
				if _, e = fmt.Fprintln(output, "invocation failed"); e != nil {
					return e
				}
				continue
			}
			if _, e = fmt.Fprintln(output, result); e != nil {
				return e
			}
		}
	}
}

func invocation(ctx context.Context, snapshot plugin.ProviderSnapshot) (string, error) {
	entries := make([]string, 0, len(snapshot.Providers))
	for _, entry := range snapshot.Providers {
		// Discovery errors are failed reads, never proof of incompatibility
		// or empty membership. Publish a result only if every read succeeds.
		if _, e := entry.Provider.GetCapabilities(ctx); e != nil {
			return "", e
		}
		entries = append(entries, fmt.Sprintf("%s/%d", entry.ID, entry.Generation))
	}
	return fmt.Sprintf("revision=%d providers=%s", snapshot.Revision, strings.Join(entries, ",")), nil
}
