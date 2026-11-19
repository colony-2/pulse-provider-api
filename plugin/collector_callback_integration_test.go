package plugin

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func TestCollectorCallbackRealJoinLeaveReconnect(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	log := make(chan ProviderChange, 10)
	s := callbackSubscribe(t, c, func(ctx context.Context, v ProviderChange) {
		if v.Kind == ProviderChangeInitial {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		log <- v
	})
	<-entered
	fast := make(chan ProviderChange, 10)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { fast <- v })
	callbackEvent(t, fast)
	var discoveries atomic.Int32
	p := &reverseProvider{caps: func(context.Context) (api.Capabilities, error) { discoveries.Add(1); return api.Capabilities{}, nil }}
	_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
	old := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
	callbackEvent(t, fast)
	if discoveries.Load() != 0 {
		t.Fatal("callback performed discovery")
	}
	if _, e := old.GetCapabilities(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := old.Close(); e != nil {
		t.Fatal(e)
	}
	receiveRun(t, done)
	callbackEvent(t, fast)
	_, next := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
	newer := nextSnapshot(t, c, 1).Providers[0]
	callbackEvent(t, fast)
	close(release)
	initial, joined, left, rejoined := callbackEvent(t, log), callbackEvent(t, log), callbackEvent(t, log), callbackEvent(t, log)
	if initial.Revision != 1 || joined.Revision != 2 || left.Revision != 3 || rejoined.Revision != 4 {
		t.Fatal(initial, joined, left, rejoined)
	}
	if left.Reason != ProviderReasonAdministrativeClose || left.Left[0].Generation != old.g.id || rejoined.Joined[0].Generation != newer.Generation || newer.Generation <= old.g.id {
		t.Fatal(left, rejoined)
	}
	if _, e := joined.Joined[0].Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal("historical join revived", e)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	receiveRun(t, next)
	final := callbackEvent(t, log)
	if final.Kind != ProviderChangeStopped || final.Revision != 5 || final.Left[0].Generation != newer.Generation {
		t.Fatal(final)
	}
	if e := callbackWait(t, s); e != nil {
		t.Fatal(e)
	}
}

func TestCollectorCallbackNoRejectedJoins(t *testing.T) {
	tr := testTrust(t, "alpha", "beta", "denied")
	o := collectorOptions(tr)
	authorize := o.Authorize
	o.Authorize = func(ctx context.Context, s tls.ConnectionState) (string, error) {
		if s.PeerCertificates[0].Subject.CommonName == "denied" {
			return "", ErrUnavailable
		}
		return authorize(ctx, s)
	}
	c := testCollector(t, o)
	log := make(chan ProviderChange, 10)
	callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { log <- v })
	callbackEvent(t, log)
	_, good := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	nextSnapshot(t, c, 1)
	callbackEvent(t, log)
	for _, kind := range []string{"duplicate", "authorization", "initialization", "certificate"} {
		name := "alpha"
		factory := factoryFor(&reverseProvider{})
		if kind == "authorization" {
			name = "denied"
		}
		if kind == "initialization" {
			name = "beta"
			factory = func(context.Context, Init) (api.Provider, Cleanup, error) {
				return nil, nil, errors.New("private initialization error")
			}
		}
		run := runOptions(t, c, tr, name)
		if kind == "certificate" {
			run.TLSConfig = run.TLSConfig.Clone()
			run.TLSConfig.Certificates = nil
		}
		_, done := launchRun(t, factory, run)
		if e := receiveRun(t, done); e == nil {
			t.Fatal(kind, "accepted")
		}
	}
	if snapshot, e := c.Snapshot(); e != nil || snapshot.Revision != 2 || len(log) != 0 {
		t.Fatal(snapshot, e, len(log))
	}
	_ = c.Close()
	receiveRun(t, good)
}

func TestCollectorCallbackCleanupFailureAfterFinal(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	cleanupEntered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
		return &reverseProvider{}, func(context.Context) error { close(cleanupEntered); <-release; return errors.New("private cleanup") }, nil
	}, runOptions(t, c, tr, "alpha"))
	nextSnapshot(t, c, 1)
	final := make(chan struct{})
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) {
		if v.Kind == ProviderChangeStopped {
			close(final)
		}
	})
	// A previously canceled observer must retain cancellation despite cleanup failure.
	prior := callbackSubscribe(t, c, func(context.Context, ProviderChange) {})
	prior.Cancel()
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	<-final
	<-cleanupEntered
	select {
	case <-s.done:
		t.Fatal("final delivery hid incomplete transport cleanup")
	default:
	}
	if e := <-closed; !errors.Is(e, ErrCleanup) {
		t.Fatal(e)
	}
	if e := callbackWait(t, s); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if prior.Wait() != context.Canceled {
		t.Fatal(prior.Wait())
	}
	// Run returns within its cleanup budget even while the fixture callback waits.
	if e := receiveRun(t, done); !errors.Is(e, ErrCleanup) {
		t.Fatal(e)
	}
}

func TestCollectorCallbackShutdownOverflow(t *testing.T) {
	tr := testTrust(t)
	o := collectorOptions(tr)
	o.CallbackQueue = 1
	c := testCollector(t, o)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s := callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { close(entered); <-release })
	<-entered
	_, done := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
	p := nextSnapshot(t, c, 1).Providers[0].Provider
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	receiveRun(t, done)
	if e := callbackWait(t, s); !errors.Is(e, ErrSaturated) {
		t.Fatal(e)
	}
	if _, e := p.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestCollectorCallbackUncooperativeWorkerBound(t *testing.T) {
	tr := testTrust(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, fixtureBinary, "callback-hang", tr.files(t))
	cmd.Env = append(os.Environ(), "MODE=")
	out, e := cmd.CombinedOutput()
	if e != nil || !strings.Contains(string(out), "callback worker bound and shutdown verified") {
		t.Fatal(e, string(out))
	}
}
