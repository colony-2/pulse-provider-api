package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc/metadata"
)

func TestReversePostdispatchUncertainty(t *testing.T) {
	for _, fault := range []string{"cancel", "timeout", "oversized"} {
		t.Run(fault, func(t *testing.T) {
			tr := testTrust(t)
			o := collectorOptions(tr)
			o.MaxMessageBytes = 1024
			o.CallTimeout = 100 * time.Millisecond
			c := testCollector(t, o)
			entered := make(chan struct{})
			release := make(chan struct{})
			p := &reverseProvider{submit: func(ctx context.Context, ls []api.Launch) ([]api.Submission, error) {
				close(entered)
				if fault != "oversized" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				<-release
				return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Rejected, Reason: strings.Repeat("private", 1024)}}, nil
			}}
			_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			handle := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			completed := make(chan struct{})
			var ss []api.Submission
			var e error
			go func() { ss, _, e = handle.Submit(ctx, launches("uncertain"), api.SubmitOptions{}); close(completed) }()
			<-entered
			if fault == "cancel" {
				cancel()
			}
			if fault == "oversized" {
				close(release)
			}
			<-completed
			if e == nil || ss[0].Status != api.Unknown || p.calls.Load() != 1 {
				t.Fatal(ss, e, p.calls.Load())
			}
			page, e := handle.List(context.Background(), api.ListRequest{})
			if e != nil || len(page.Items) != 0 || ss[0].Status != api.Unknown {
				t.Fatal(page, e)
			}
			_ = handle.Close()
			nextSnapshot(t, c, 0)
			if ss[0].Status != api.Unknown || p.calls.Load() != 1 {
				t.Fatal("later emptiness resolved uncertainty")
			}
			receiveRun(t, done)
		})
	}
}
func TestReverseMetadataAndObservationBounds(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	p := &reverseProvider{submit: func(_ context.Context, ls []api.Launch) ([]api.Submission, error) {
		return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Rejected, Reason: strings.Repeat("x", 100000)}}, errors.New(strings.Repeat("y", 100000))
	}}
	_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
	handle := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
	// Ordinary method data stay complete; diagnostic observations remain bounded.
	ss, obs, _, e := handle.SubmitDetailed(context.Background(), launches("bounds"), api.SubmitOptions{})
	if e == nil || len(ss[0].Reason) != 100000 || obs.Capture != "truncated" || len(obs.MethodError.Text) > MaxObservationBytes {
		t.Fatal(len(ss[0].Reason), obs.Capture, e)
	}
	// Oversized request metadata must not enter a business handler.
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("test-bound", strings.Repeat("z", 32<<10)))
	l, _ := toLaunch(launches("metadata")[0])
	_, e = handle.g.rpc.Submit(ctx, &wire.SubmitRequest{Invocation: 100, Launches: []*wire.Launch{l}})
	if e == nil || p.calls.Load() != 1 {
		t.Fatal(e, p.calls.Load())
	}
	_ = c.Close()
	receiveRun(t, done)
}
func TestCollectorSharedShutdownBudget(t *testing.T) {
	tr := testTrust(t, "alpha", "beta", "gamma")
	o := collectorOptions(tr)
	o.ShutdownTimeout = 150 * time.Millisecond
	c := testCollector(t, o)
	release := make(chan struct{})
	defer close(release)
	var completions []<-chan error
	for _, name := range []string{"alpha", "beta", "gamma"} {
		run := runOptions(t, c, tr, name)
		run.Serve.ShutdownTimeout = time.Second
		_, done := launchRun(t, func(context.Context, Init) (api.Provider, Cleanup, error) {
			return &reverseProvider{}, func(context.Context) error { <-release; return nil }, nil
		}, run)
		completions = append(completions, done)
	}
	s := nextSnapshot(t, c, 3)
	var wg sync.WaitGroup
	results := make(chan error, 5)
	start := time.Now()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- c.Close() }()
	}
	wg.Wait()
	close(results)
	for e := range results {
		if !errors.Is(e, ErrCleanup) {
			t.Fatal(e)
		}
	}
	if time.Since(start) > 350*time.Millisecond {
		t.Fatal("shutdown multiplied by peer count", time.Since(start))
	}
	for _, entry := range s.Providers {
		if _, e := entry.Provider.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
	}
	for _, done := range completions {
		if e := receiveRun(t, done); !errors.Is(e, ErrCleanup) {
			t.Fatal(e)
		}
	}
}
func TestCollectorSnapshotJoinLeaveRaces(t *testing.T) {
	tr := testTrust(t, "alpha", "beta")
	c := testCollector(t, collectorOptions(tr))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := uint64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				s, e := c.Snapshot()
				if e != nil {
					t.Error(e)
					return
				}
				if s.Revision < last {
					t.Error("revision regressed")
				}
				last = s.Revision
				for j := 1; j < len(s.Providers); j++ {
					if s.Providers[j-1].ID >= s.Providers[j].ID {
						t.Error("unsorted duplicate membership")
					}
				}
			}
		}()
	}
	for i := 0; i < 3; i++ {
		cancel, a := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "alpha"))
		cancel2, b := launchRun(t, factoryFor(&reverseProvider{}), runOptions(t, c, tr, "beta"))
		nextSnapshot(t, c, 2)
		cancel()
		receiveRun(t, a)
		nextSnapshot(t, c, 1)
		cancel2()
		receiveRun(t, b)
		nextSnapshot(t, c, 0)
	}
	close(stop)
	wg.Wait()
}
