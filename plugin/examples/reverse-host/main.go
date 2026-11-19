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
	"strings"
	"sync"

	"github.com/colony-2/pulse-provider-api/plugin"
)

func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "provider collection failed")
		os.Exit(1)
	}
}
func run() error {
	address := flag.String("listen", "127.0.0.1:0", "reachable TCP address")
	ca := flag.String("ca", "", "trusted provider CA file")
	cert := flag.String("cert", "", "host certificate file")
	key := flag.String("key", "", "host private key file")
	identities := flag.String("identities", "", "comma-separated authorized certificate DNS names")
	flag.Parse()
	pair, e := tls.LoadX509KeyPair(*cert, *key)
	if e != nil {
		return e
	}
	b, e := os.ReadFile(*ca)
	if e != nil {
		return e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return plugin.ErrInvalid
	}
	allowed := strings.Split(*identities, ",")
	listener, e := net.Listen("tcp", *address)
	if e != nil {
		return e
	}
	collector, e := plugin.StartCollector(context.Background(), listener, plugin.CollectorOptions{
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{plugin.ReverseALPN}, Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert},
		Authorize: func(_ context.Context, s tls.ConnectionState) (string, error) {
			for _, id := range allowed {
				if id != "" && s.PeerCertificates[0].VerifyHostname(id) == nil {
					return id, nil
				}
			}
			return "", plugin.ErrUnavailable
		},
	})
	if e != nil {
		listener.Close()
		return e
	}
	defer collector.Close()
	output := &lockedWriter{Writer: os.Stdout}
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
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
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
