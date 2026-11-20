package provider

import (
	"context"
	"errors"
	"time"
)

// SubmitOptions selects optional, independent observation of this batch.
type SubmitOptions struct{ Listener *ListenOptions }

// ListenOptions governs logs only, never admission or accepted work. Duration
// starts at registration: zero listens forever; negative durations are invalid.
// Providers must detach or drain readers when listening ends.
type ListenOptions struct {
	Context  context.Context
	Duration time.Duration
	OnLog    func(context.Context, LaunchLog) error
}

// LogSubscription observes a batch, not workload completion. Stop is idempotent
// and nonblocking. Done and Wait do not wait for an uncooperative callback.
// Wait's context bounds only that wait. Coverage and results own their slices.
type LogSubscription interface {
	Stop()
	Done() <-chan struct{}
	Wait(context.Context) (ListenerResult, error)
	Coverage() []SourceStatus
}

type LogSource string

const (
	LogSourceLaunch   LogSource = "launch"
	LogSourceStdout   LogSource = "stdout"
	LogSourceStderr   LogSource = "stderr"
	LogSourceCombined LogSource = "combined"
	MaxLogRecordBytes           = 4096
)

// Prefix is display metadata; Data remains unmodified, including binary bytes.
func (s LogSource) Prefix() string {
	switch s {
	case LogSourceLaunch:
		return "[launch]"
	case LogSourceStdout:
		return "[out]"
	case LogSourceStderr:
		return "[err]"
	case LogSourceCombined:
		return "[out+err]"
	}
	return ""
}

// LaunchLog is a bounded fragment. Sequence starts at one, increasing per launch
// across all sources. Split native reads before sequencing; do not buffer lines.
// SubscriptionID is an opaque identity, never an authorization credential.
type LaunchLog struct {
	LaunchID       string
	SubscriptionID string
	Sequence       uint64
	Timestamp      time.Time
	Source         LogSource
	Data           []byte
}

type SourceState string

const (
	SourcePending     SourceState = "pending"
	SourceActive      SourceState = "active"
	SourceClosed      SourceState = "closed"
	SourceUnavailable SourceState = "unavailable"
	SourceInterrupted SourceState = "interrupted"
)

// Closed records known native closure even when delivery was interrupted.
// LastSequence counts callback dispatch, not processing or remote claims.
type SourceStatus struct {
	LaunchID     string
	Source       LogSource
	State        SourceState
	Closed       bool
	LastSequence uint64
	Cause        error
}

type ListenerReason string

const (
	ListenerStopped           ListenerReason = "listener_stopped"
	ListenerDurationElapsed   ListenerReason = "duration_elapsed"
	ListenerSourcesClosed     ListenerReason = "sources_closed"
	ListenerContextCanceled   ListenerReason = "context_canceled"
	ListenerContextDeadline   ListenerReason = "context_deadline"
	ListenerSourceUnavailable ListenerReason = "source_unavailable"
	ListenerSourceError       ListenerReason = "source_error"
	ListenerTransportLost     ListenerReason = "transport_lost"
	ListenerProviderClosed    ListenerReason = "provider_closed"
	ListenerError             ListenerReason = "listener_error"
	ListenerSlow              ListenerReason = "slow_listener"
	ListenerOverflow          ListenerReason = "overflow"
	ListenerCapacityExceeded  ListenerReason = "capacity_exceeded"
	ListenerProtocolError     ListenerReason = "protocol_error"
)

type ListenerResult struct {
	Reason   ListenerReason
	Cause    error
	Coverage []SourceStatus
}

var (
	ErrStopListening  = errors.New("provider: stop listening")
	ErrLogSource      = errors.New("provider: log source failure")
	ErrLogTransport   = errors.New("provider: log transport lost")
	ErrProviderClosed = errors.New("provider: provider closed")
	ErrLogDelivery    = errors.New("provider: log delivery failed")
	ErrLogCapacity    = errors.New("provider: log capacity exceeded")
	ErrLogProtocol    = errors.New("provider: invalid log protocol")
)
