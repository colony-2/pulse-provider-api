package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/protobuf/proto"
)

func wireTestSub(t *testing.T) (*logSubscription, <-chan api.LaunchLog) {
	t.Helper()
	o := logLimits(t)
	got := make(chan api.LaunchLog, 8)
	s := newLogSubscription(o, newLogPool(o), launches("one"), api.ListenOptions{Context: context.Background(), OnLog: func(_ context.Context, v api.LaunchLog) error { got <- v; return nil }})
	t.Cleanup(s.Stop)
	return s, got
}
func TestLogWireFragmentRoundTrip(t *testing.T) {
	s, got := wireTestSub(t)
	r := frameReader{}
	if e := r.accept(&wire.ObserveEvent{Kind: &wire.ObserveEvent_Ready{Ready: &wire.Empty{}}}, s); e != nil {
		t.Fatal(e)
	}
	v := logRecord(1)
	v.Data = bytes.Repeat([]byte{0xff, 0, 27, '\n'}, 1024)
	frames, e := recordFrames(v, 1024)
	if e != nil || len(frames) < 2 {
		t.Fatal(frames, e)
	}
	for _, f := range frames {
		data, e := proto.Marshal(f)
		if e != nil || len(data) > 1024 {
			t.Fatal(e, len(data))
		}
		var decoded wire.ObserveEvent
		if e = proto.Unmarshal(data, &decoded); e != nil {
			t.Fatal(e)
		}
		if e = r.accept(&decoded, s); e != nil {
			t.Fatal(e)
		}
	}
	select {
	case actual := <-got:
		if !bytes.Equal(actual.Data, v.Data) || actual.LaunchID != v.LaunchID || actual.Source != v.Source || actual.SubscriptionID != v.SubscriptionID || actual.Sequence != v.Sequence || !actual.Timestamp.Equal(v.Timestamp) {
			t.Fatal(actual)
		}
	case <-time.After(time.Second):
		t.Fatal("record missing")
	}
	for _, n := range []time.Duration{0, 1, time.Duration(math.MaxInt64)} {
		a := &wire.SubmitRequest{DurationNanos: int64(n)}
		data, _ := proto.Marshal(a)
		var b wire.SubmitRequest
		_ = proto.Unmarshal(data, &b)
		if time.Duration(b.DurationNanos) != n {
			t.Fatal(n, b.DurationNanos)
		}
	}
}
func TestLogWireMalformedFrames(t *testing.T) {
	for _, mutation := range []string{"pre-ready", "duplicate-ready", "duplicate-fragment", "missing-fragment", "interleaved", "metadata", "over-limit", "bad-source", "bad-time", "sequence-gap", "post-terminal", "unknown", "incomplete-coverage"} {
		t.Run(mutation, func(t *testing.T) {
			s, _ := wireTestSub(t)
			r := frameReader{}
			ready := &wire.ObserveEvent{Kind: &wire.ObserveEvent_Ready{Ready: &wire.Empty{}}}
			if mutation != "pre-ready" {
				_ = r.accept(ready, s)
			}
			v := logRecord(1)
			v.Data = bytes.Repeat([]byte("x"), 4096)
			frames, e := recordFrames(v, 1024)
			if e != nil {
				t.Fatal(e)
			}
			var bad *wire.ObserveEvent
			switch mutation {
			case "pre-ready":
				bad = frames[0]
			case "duplicate-ready":
				bad = ready
			case "duplicate-fragment":
				_ = r.accept(frames[0], s)
				bad = frames[0]
			case "missing-fragment":
				_ = r.accept(frames[0], s)
				bad = frames[2]
			case "interleaved":
				_ = r.accept(frames[0], s)
				bad = proto.Clone(frames[1]).(*wire.ObserveEvent)
				bad.GetRecord().Sequence = 2
			case "metadata":
				_ = r.accept(frames[0], s)
				bad = proto.Clone(frames[1]).(*wire.ObserveEvent)
				bad.GetRecord().Source = "stderr"
			case "over-limit":
				bad = frames[0]
				bad.GetRecord().Data = make([]byte, 4097)
			case "bad-source":
				bad = frames[0]
				bad.GetRecord().Source = "unknown"
			case "bad-time":
				bad = frames[0]
				bad.GetRecord().Timestamp.Seconds = math.MaxInt64
			case "sequence-gap":
				v.Sequence = 2
				v.Data = nil
				a, _ := recordFrames(v, 1024)
				bad = a[0]
			case "post-terminal":
				_ = r.accept(&wire.ObserveEvent{Kind: &wire.ObserveEvent_Terminal{Terminal: &wire.LogTerminal{Reason: string(api.ListenerStopped)}}}, s)
				bad = frames[0]
			case "unknown":
				bad = &wire.ObserveEvent{}
			case "incomplete-coverage":
				cov, _ := coverageFrames(s.Coverage(), 1, 1024)
				_ = r.accept(cov[0], s)
				bad = frames[0]
			}
			if e := r.accept(bad, s); e == nil {
				t.Fatal("malformed frame accepted")
			}
		})
	}
}
func TestLogWireCoverageAtomicAndBounded(t *testing.T) {
	s, _ := wireTestSub(t)
	r := frameReader{}
	_ = r.accept(&wire.ObserveEvent{Kind: &wire.ObserveEvent_Ready{Ready: &wire.Empty{}}}, s)
	before := s.Coverage()
	changed := s.Coverage()
	for i := range changed {
		changed[i].State = api.SourceClosed
		changed[i].Closed = true
		changed[i].LastSequence = 9999
	}
	frames, e := coverageFrames(changed, 1, 1024)
	if e != nil {
		t.Fatal(e)
	}
	for i, f := range frames {
		if e = r.accept(f, s); e != nil {
			t.Fatal(e)
		}
		if i < len(frames)-1 && s.Coverage()[0].State != before[0].State {
			t.Fatal("partial snapshot published")
		}
	}
	if s.Coverage()[0].State != api.SourceClosed || s.Coverage()[0].LastSequence != 0 {
		t.Fatal(s.Coverage())
	}
	if e = r.accept(frames[0], s); e == nil {
		t.Fatal("old revision accepted")
	}
}
func TestLogWireTerminalCategories(t *testing.T) {
	for _, reason := range []api.ListenerReason{api.ListenerStopped, api.ListenerDurationElapsed, api.ListenerSourcesClosed, api.ListenerContextCanceled, api.ListenerContextDeadline, api.ListenerSourceUnavailable, api.ListenerSourceError, api.ListenerTransportLost, api.ListenerProviderClosed, api.ListenerError, api.ListenerSlow, api.ListenerOverflow, api.ListenerCapacityExceeded, api.ListenerProtocolError} {
		t.Run(string(reason), func(t *testing.T) {
			s, _ := wireTestSub(t)
			r := frameReader{ready: true}
			cause, _ := terminalCause(reason)
			e := r.accept(&wire.ObserveEvent{Kind: &wire.ObserveEvent_Terminal{Terminal: &wire.LogTerminal{Reason: string(reason), Cause: logCauseCode(cause)}}}, s)
			if e != nil {
				t.Fatal(e)
			}
			if result := logWait(t, s); result.Reason != reason {
				t.Fatal(result)
			}
		})
	}
}

func TestLogWireRejectsInvalidPartialCoverageBeforeRetention(t *testing.T) {
	for _, mutation := range []string{"oversized-id", "invalid-id", "foreign-id", "source", "state", "cause", "duplicate"} {
		for _, partial := range []bool{false, true} {
			if mutation == "duplicate" && !partial {
				continue // A duplicate requires an earlier fragment.
			}
			t.Run(fmt.Sprintf("%s/partial=%v", mutation, partial), func(t *testing.T) {
				s, _ := wireTestSub(t)
				r := frameReader{ready: true}
				before := s.Coverage()
				frames, err := coverageFrames(before, 1, 1024)
				if err != nil {
					t.Fatal(err)
				}
				index := 0
				if partial {
					if err := r.accept(frames[0], s); err != nil {
						t.Fatal(err)
					}
					index = 1
				}
				bad := proto.Clone(frames[index]).(*wire.ObserveEvent)
				q := bad.GetCoverage().Status
				switch mutation {
				case "oversized-id":
					q.LaunchId = bytes.Repeat([]byte("x"), 100000)
				case "invalid-id":
					q.LaunchId = []byte("invalid/id")
				case "foreign-id":
					q.LaunchId = []byte("other")
				case "source":
					q.Source = string(bytes.Repeat([]byte("x"), 100000))
				case "state":
					q.State = string(bytes.Repeat([]byte("x"), 100000))
				case "cause":
					q.Cause = "unknown"
				case "duplicate":
					q.Source = frames[0].GetCoverage().Status.Source
				}
				retained := append([]api.SourceStatus(nil), r.coverage...)
				if err := r.accept(bad, s); !errors.Is(err, api.ErrLogProtocol) {
					t.Fatalf("invalid fragment accepted: %v", err)
				}
				if !reflect.DeepEqual(r.coverage, retained) {
					t.Fatal("invalid metadata retained")
				}
				if !reflect.DeepEqual(s.Coverage(), before) {
					t.Fatal("invalid partial coverage published")
				}
			})
		}
	}
}
