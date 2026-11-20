package plugin

import (
	"context"
	"errors"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validLogID(s string) bool      { return launchID.MatchString(s) }
func validLogTime(t time.Time) bool { return timestamppb.New(t).CheckValid() == nil }
func logCauseCode(e error) string {
	if e == nil {
		return ""
	}
	for _, v := range []struct {
		code string
		e    error
	}{{"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded}, {"source", api.ErrLogSource}, {"transport", api.ErrLogTransport}, {"closed", api.ErrProviderClosed}, {"delivery", api.ErrLogDelivery}, {"capacity", api.ErrLogCapacity}, {"protocol", api.ErrLogProtocol}} {
		if errors.Is(e, v.e) {
			return v.code
		}
	}
	return "source"
}
func logCause(code string) (error, bool) {
	switch code {
	case "":
		return nil, true
	case "canceled":
		return context.Canceled, true
	case "deadline":
		return context.DeadlineExceeded, true
	case "source":
		return api.ErrLogSource, true
	case "transport":
		return api.ErrLogTransport, true
	case "closed":
		return api.ErrProviderClosed, true
	case "delivery":
		return api.ErrLogDelivery, true
	case "capacity":
		return api.ErrLogCapacity, true
	case "protocol":
		return api.ErrLogProtocol, true
	}
	return nil, false
}
func terminalCause(reason api.ListenerReason) (error, bool) {
	switch reason {
	case api.ListenerStopped, api.ListenerDurationElapsed, api.ListenerSourcesClosed:
		return nil, true
	case api.ListenerContextCanceled:
		return context.Canceled, true
	case api.ListenerContextDeadline:
		return context.DeadlineExceeded, true
	case api.ListenerSourceUnavailable, api.ListenerSourceError:
		return api.ErrLogSource, true
	case api.ListenerTransportLost:
		return api.ErrLogTransport, true
	case api.ListenerProviderClosed:
		return api.ErrProviderClosed, true
	case api.ListenerError:
		return api.ErrLogDelivery, true
	case api.ListenerSlow:
		return errors.Join(api.ErrLogDelivery, context.DeadlineExceeded), true
	case api.ListenerOverflow, api.ListenerCapacityExceeded:
		return api.ErrLogCapacity, true
	case api.ListenerProtocolError:
		return api.ErrLogProtocol, true
	}
	return nil, false
}
func recordFrames(v api.LaunchLog, limit int) ([]*wire.ObserveEvent, error) {
	var ts *timestamppb.Timestamp
	if !v.Timestamp.IsZero() {
		ts = timestamppb.New(v.Timestamp)
		if ts.CheckValid() != nil {
			return nil, api.ErrLogProtocol
		}
	}
	if len(v.Data) > api.MaxLogRecordBytes || v.Source.Prefix() == "" || !validLogID(v.LaunchID) || !validLogID(v.SubscriptionID) || v.Sequence == 0 {
		return nil, api.ErrLogProtocol
	}
	out := []*wire.ObserveEvent{}
	data := v.Data
	for i := uint32(0); ; i++ {
		r := &wire.LogRecord{LaunchId: []byte(v.LaunchID), SubscriptionId: []byte(v.SubscriptionID), Sequence: v.Sequence, Timestamp: ts, Source: string(v.Source), Index: i}
		f := &wire.ObserveEvent{Kind: &wire.ObserveEvent_Record{Record: r}}
		// Reserve final/index overhead; never trust a peer-advertised total length.
		room := limit - proto.Size(f) - 16
		if room < 1 {
			return nil, api.ErrLogProtocol
		}
		n := min(room, len(data))
		r.Data = append([]byte(nil), data[:n]...)
		data = data[n:]
		r.Final = len(data) == 0
		if proto.Size(f) > limit {
			return nil, api.ErrLogProtocol
		}
		out = append(out, f)
		if r.Final {
			return out, nil
		}
	}
}
func coverageFrames(v []api.SourceStatus, revision uint64, limit int) ([]*wire.ObserveEvent, error) {
	if len(v) > api.MaxBatch*4 || len(v) == 0 || revision == 0 {
		return nil, api.ErrLogProtocol
	}
	out := make([]*wire.ObserveEvent, 0, len(v))
	for i, q := range v {
		if !validLogID(q.LaunchID) || q.Source.Prefix() == "" || !validSourceState(q.State) {
			return nil, api.ErrLogProtocol
		}
		f := &wire.ObserveEvent{Kind: &wire.ObserveEvent_Coverage{Coverage: &wire.LogCoverage{Revision: revision, Index: uint32(i), Final: i == len(v)-1, Status: &wire.LogSourceStatus{LaunchId: []byte(q.LaunchID), Source: string(q.Source), State: string(q.State), Closed: q.Closed, Cause: logCauseCode(q.Cause)}}}}
		if proto.Size(f) > limit {
			return nil, api.ErrLogProtocol
		}
		out = append(out, f)
	}
	return out, nil
}

// frameReader retains at most one record and one bounded coverage revision.
type frameReader struct {
	ready        bool
	terminal     bool
	record       *wire.LogRecord
	data         []byte
	recordIndex  uint32
	revision     uint64
	nextRevision uint64
	coverage     []api.SourceStatus
}

func (r *frameReader) accept(f *wire.ObserveEvent, s *logSubscription) error {
	if r.terminal || f == nil {
		return api.ErrLogProtocol
	}
	switch k := f.Kind.(type) {
	case *wire.ObserveEvent_Ready:
		if r.ready || k.Ready == nil {
			return api.ErrLogProtocol
		}
		r.ready = true
		return nil
	case *wire.ObserveEvent_Record:
		if !r.ready || k.Record == nil {
			return api.ErrLogProtocol
		}
		v := k.Record
		if len(v.Data) > api.MaxLogRecordBytes {
			return api.ErrLogProtocol
		}
		if len(r.coverage) > 0 {
			return api.ErrLogProtocol
		}
		if r.record == nil {
			if v.Index != 0 || !validLogID(string(v.LaunchId)) || !validLogID(string(v.SubscriptionId)) || api.LogSource(v.Source).Prefix() == "" || v.Sequence == 0 {
				return api.ErrLogProtocol
			}
			if v.Timestamp != nil && v.Timestamp.CheckValid() != nil {
				return api.ErrLogProtocol
			}
			r.record = proto.Clone(v).(*wire.LogRecord)
			r.record.Data = nil
			r.recordIndex = 0
		}
		a := r.record
		if v.Index != r.recordIndex || string(v.LaunchId) != string(a.LaunchId) || string(v.SubscriptionId) != string(a.SubscriptionId) || v.Sequence != a.Sequence || v.Source != a.Source || !proto.Equal(v.Timestamp, a.Timestamp) || len(r.data)+len(v.Data) > api.MaxLogRecordBytes || (!v.Final && len(v.Data) == 0) {
			return api.ErrLogProtocol
		}
		r.data = append(r.data, v.Data...)
		r.recordIndex++
		if v.Final {
			t := time.Time{}
			if a.Timestamp != nil {
				t = a.Timestamp.AsTime()
			}
			e := s.enqueue(api.LaunchLog{LaunchID: string(a.LaunchId), SubscriptionID: string(a.SubscriptionId), Sequence: a.Sequence, Timestamp: t, Source: api.LogSource(a.Source), Data: r.data})
			r.record = nil
			r.data = nil
			r.recordIndex = 0
			return e
		}
		return nil
	case *wire.ObserveEvent_Coverage:
		if !r.ready || k.Coverage == nil || r.record != nil {
			return api.ErrLogProtocol
		}
		v := k.Coverage
		q := v.Status
		// Validate each fragment before retaining metadata, even when a peer
		// never completes the revision. Bound IDs before converting their bytes.
		if q == nil || len(q.LaunchId) > 128 || !validLogID(string(q.LaunchId)) || api.LogSource(q.Source).Prefix() == "" || !validSourceState(api.SourceState(q.State)) {
			return api.ErrLogProtocol
		}
		c, ok := logCause(q.Cause)
		if !ok {
			return api.ErrLogProtocol
		}
		id := string(q.LaunchId)
		s.mu.Lock()
		_, known := s.launches[id]
		maxCoverage := len(s.launches) * 4
		s.mu.Unlock()
		if !known || len(r.coverage) >= maxCoverage {
			return api.ErrLogProtocol
		}
		for _, old := range r.coverage {
			if old.LaunchID == id && old.Source == api.LogSource(q.Source) {
				return api.ErrLogProtocol
			}
		}
		if len(r.coverage) == 0 {
			if v.Index != 0 || v.Revision <= r.revision {
				return api.ErrLogProtocol
			}
			r.nextRevision = v.Revision
		}
		if v.Revision != r.nextRevision || int(v.Index) != len(r.coverage) {
			return api.ErrLogProtocol
		}
		r.coverage = append(r.coverage, api.SourceStatus{LaunchID: id, Source: api.LogSource(q.Source), State: api.SourceState(q.State), Closed: q.Closed, Cause: c})
		if v.Final {
			if e := s.setCoverage(r.coverage); e != nil {
				return e
			}
			r.revision = v.Revision
			r.coverage = nil
		}
		return nil
	case *wire.ObserveEvent_Terminal:
		if !r.ready || k.Terminal == nil || r.record != nil || len(r.coverage) > 0 {
			return api.ErrLogProtocol
		}
		reason := api.ListenerReason(k.Terminal.Reason)
		cause, ok := terminalCause(reason)
		if !ok || k.Terminal.Cause != logCauseCode(cause) {
			return api.ErrLogProtocol
		}
		r.terminal = true
		s.complete(api.ListenerResult{Reason: reason, Cause: cause})
		return nil
	default:
		return api.ErrLogProtocol
	}
}
