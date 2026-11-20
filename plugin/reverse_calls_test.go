package plugin

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
)

func TestReverseCallsFidelityAndMethodErrors(t *testing.T) {
	for _, mode := range reverseModes {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			c := testCollector(t, collectorOptions(tr))
			ls := launches("a", "b", "c", "d")
			var got []api.Launch
			caps := api.Capabilities{BasesComplete: true, Bases: []api.BaseCapabilities{{Name: "unrelated", Capabilities: []string{"gpu", "net"}}}}
			created := time.Date(1, 1, 1, 0, 0, 0, 123, time.UTC)
			started := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
			listed := api.ListResponse{NextPageToken: "\xff\x00cursor", Items: []api.Instance{{ID: "native/opaque", LaunchID: "a", State: "busy", Job: ls[0].Job, Refs: []string{"\xffref", "second"}, CreatedAt: &created, StartedAt: &started}}}
			var methodError error
			p := &reverseProvider{caps: func(context.Context) (api.Capabilities, error) { return caps, methodError }, list: func(_ context.Context, q api.ListRequest) (api.ListResponse, error) {
				if q.PageToken != "\xffcursor" || q.PageSize != 100 {
					t.Error(q)
				}
				return listed, methodError
			}, submit: func(_ context.Context, in []api.Launch) ([]api.Submission, error) {
				got = in
				return []api.Submission{{LaunchID: "a", Status: api.Accepted}, {LaunchID: "b", Status: api.NoCapacity, Reason: "busy"}, {LaunchID: "c", Status: api.Status("new"), Reason: "future"}, {LaunchID: "d", Status: api.Rejected, Reason: "one"}, {LaunchID: "d", Status: api.Rejected, Reason: "two"}}, errors.New("private-method-error")
			}}
			_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			connection := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			out, obs, _, e := connection.SubmitDetailed(context.Background(), ls, api.SubmitOptions{})
			if e == nil || out[0].Status != api.Accepted || out[1].Status != api.NoCapacity || out[2].Status != api.Unknown || out[3].Status != api.Unknown || !reflect.DeepEqual(got, ls) || !obs.MethodError.Present || len(obs.Outcomes) != 5 || obs.Generation != connection.Health().Generation {
				t.Fatal(out, obs, e)
			}
			for _, err := range []error{nil, errors.New(""), context.Canceled, context.DeadlineExceeded} {
				// Calls complete serially; no provider callback reads this while it changes.
				methodError = err
				actual, e := connection.GetCapabilities(context.Background())
				if !reflect.DeepEqual(actual, caps) || (e != nil) != (err != nil) {
					t.Fatal(actual, e)
				}
				if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && !errors.Is(e, err) {
					t.Fatal(e)
				}
				page, e := connection.List(context.Background(), api.ListRequest{PageToken: "\xffcursor"})
				if !reflect.DeepEqual(page, listed) || (e != nil) != (err != nil) {
					t.Fatal(page, e)
				}
			}
			if strings.Contains(connection.Health().LastFailure, "private") {
				t.Fatal(connection.Health())
			}
			_ = c.Close()
			receiveRun(t, done)
		})
	}
}
func TestReverseDisconnectAfterDispatchIsUnknown(t *testing.T) {
	for _, mode := range reverseModes {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			c := testCollector(t, collectorOptions(tr))
			entered := make(chan struct{})
			p := &reverseProvider{submit: func(ctx context.Context, _ []api.Launch) ([]api.Submission, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			connection := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			type result struct {
				ss []api.Submission
				e  error
			}
			returned := make(chan result, 1)
			go func() {
				ss, _, e := connection.Submit(context.Background(), launches("uncertain"), api.SubmitOptions{})
				returned <- result{ss, e}
			}()
			<-entered
			observer := callbackSubscribe(t, c, func(context.Context, ProviderChange) { panic("private observer failure") })
			if e := callbackWait(t, observer); !errors.Is(e, ErrCallback) {
				t.Fatal(e)
			}
			_ = connection.socket.Close()
			r := <-returned
			if r.e == nil || len(r.ss) != 1 || r.ss[0].Status != api.Unknown || p.calls.Load() != 1 {
				t.Fatal(r, p.calls.Load())
			}
			nextSnapshot(t, c, 0)
			receiveRun(t, done)
			_, newDone := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			nextSnapshot(t, c, 1)
			fresh := make(chan ProviderChange, 2)
			callbackSubscribe(t, c, func(_ context.Context, v ProviderChange) { fresh <- v })
			if v := callbackEvent(t, fresh); len(v.Joined) != 1 {
				t.Fatal(v)
			}
			ss, _, e := connection.Submit(context.Background(), launches("uncertain"), api.SubmitOptions{})
			if !errors.Is(e, ErrUnavailable) || ss[0].Status != api.Unavailable || p.calls.Load() != 1 {
				t.Fatal(ss, e)
			}
			_ = c.Close()
			receiveRun(t, newDone)
		})
	}
}
func TestReversePredispatchAndControlIsolation(t *testing.T) {
	for _, mode := range reverseModes {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			o := collectorOptions(tr)
			o.Concurrency = 1
			o.MaxMessageBytes = 1024
			c := testCollector(t, o)
			entered := make(chan struct{})
			release := make(chan struct{})
			p := &reverseProvider{submit: func(ctx context.Context, ls []api.Launch) ([]api.Submission, error) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
				return []api.Submission{{LaunchID: ls[0].LaunchID, Status: api.Accepted}}, nil
			}}
			_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			connection := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			ss, _, e := connection.Submit(ctx, launches("cancel"), api.SubmitOptions{})
			if !errors.Is(e, context.Canceled) || ss[0].Status != api.Unavailable {
				t.Fatal(ss, e)
			}
			invalid := launches("bad")
			invalid[0].Lease = ""
			if _, _, e = connection.Submit(context.Background(), invalid, api.SubmitOptions{}); e == nil {
				t.Fatal("invalid allowed")
			}
			big := launches("big")
			big[0].Lease = api.Lease(strings.Repeat("x", 3000))
			if _, _, e = connection.Submit(context.Background(), big, api.SubmitOptions{}); !errors.Is(e, ErrTooLarge) {
				t.Fatal(e)
			}
			if p.calls.Load() != 0 {
				t.Fatal("predispatch handler entry")
			}
			first := make(chan error, 1)
			go func() {
				_, _, e := connection.Submit(context.Background(), launches("one"), api.SubmitOptions{})
				first <- e
			}()
			<-entered
			if _, _, e = connection.Submit(context.Background(), launches("two"), api.SubmitOptions{}); !errors.Is(e, ErrSaturated) {
				t.Fatal(e)
			}
			// Explicit control RPCs bypass the saturated business permit.
			for i := 0; i < 5; i++ {
				ctx, cc := context.WithTimeout(context.Background(), time.Second)
				_, e = connection.g.rpc.Control(ctx, &wire.ControlRequest{})
				cc()
				if e != nil {
					t.Fatal(e)
				}
			}
			if s, _ := c.Snapshot(); len(s.Providers) != 1 || p.calls.Load() != 1 {
				t.Fatal(s, p.calls.Load())
			}
			close(release)
			if e = <-first; e != nil {
				t.Fatal(e)
			}
			_ = c.Close()
			receiveRun(t, done)
		})
	}
}
func TestReverseInvocationFenceAndObservations(t *testing.T) {
	for _, mode := range reverseModes {
		t.Run(mode, func(t *testing.T) {
			tr := modeTrust(t, mode)
			c := testCollector(t, collectorOptions(tr))
			p := &reverseProvider{}
			_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			connection := nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			l, _ := toLaunch(launches("fenced")[0])
			req := &wire.SubmitRequest{Invocation: 5000, Launches: []*wire.Launch{l}}
			if _, e := connection.g.rpc.Submit(context.Background(), req); e != nil {
				t.Fatal(e)
			}
			for _, token := range []uint64{5000, 1} {
				req.Invocation = token
				if _, e := connection.g.rpc.Submit(context.Background(), req); e == nil {
					t.Fatal("duplicate or retired accepted")
				}
			}
			if p.calls.Load() != 1 {
				t.Fatal(p.calls.Load())
			}
			// Separate session so the raw fence tokens do not retire caller sequence IDs.
			_ = connection.Close()
			receiveRun(t, done)
			_, next := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
			connection = nextSnapshot(t, c, 1).Providers[0].Provider.(*Connection)
			var wg sync.WaitGroup
			seen := sync.Map{}
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ss, obs, _, e := connection.SubmitDetailed(context.Background(), launches("parallel"), api.SubmitOptions{})
					if e != nil || ss[0].Status != api.Accepted || obs.Capture != "complete" || obs.Generation != connection.g.id {
						t.Error(ss, obs, e)
					}
					if _, loaded := seen.LoadOrStore(obs.Invocation, true); loaded {
						t.Error("reused invocation")
					}
				}()
			}
			wg.Wait()
			_ = c.Close()
			receiveRun(t, next)
		})
	}
}
