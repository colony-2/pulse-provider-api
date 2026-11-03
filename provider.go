// Package provider defines the in-process contract for Pulse compute providers.
// Implementations can live in independent Go modules and be linked into a host.
// Configuration, construction, validation, and registration belong to the host
// and implementation, not this package.
package provider

import (
	"context"
	"time"
)

const (
	MaxBatch          = 100
	MaxQuantity int64 = 9007199254740991
)

// Provider submits complete execution launches and lists active compute.
// Methods must support concurrent calls and respect context cancellation.
// Neither side may mutate arguments or returned data shared with the other;
// implementations retaining inputs must copy mutable slices and pointers.
type Provider interface {
	// Submit accepts 1..MaxBatch launches with distinct LaunchIDs. Return one
	// Submission per launch, correlated by LaunchID rather than slice position.
	// Batches are not atomic. Valid explicit results remain authoritative even
	// with a non-nil error; missing, duplicate, or invalid results are uncertain.
	// An error or canceled context alone never proves that execution cannot start.
	// Callers must not retry uncertain submissions or immediately fall back.
	Submit(context.Context, []Launch) ([]Submission, error)

	// List returns active instances within this provider's configured scope.
	// It must not launch, cancel, restart, or reconcile work. A failed lookup
	// returns an error, not an empty successful page; discard results on error.
	List(context.Context, ListRequest) (ListResponse, error)
}

// Status describes admission, not execution completion.
type Status string

const (
	Accepted    Status = "accepted"    // Admitted or handed off; startup is not guaranteed.
	NoCapacity  Status = "no_capacity" // Definitely not accepted; fallback is safe.
	Unsupported Status = "unsupported" // Cannot honor a constraint; fallback is safe.
	Unavailable Status = "unavailable" // Definitely not accepted; fallback is safe.
	Rejected    Status = "rejected"    // Invalid request/configuration; stop this attempt.
	Unknown     Status = "unknown"     // Acceptance uncertain; no immediate fallback.
)

// Submission is one launch's admission result. Reason must be empty for Accepted
// and nonempty otherwise. Reasons and errors must not contain lease credentials.
// Definite declines guarantee this submission cannot later start execution.
type Submission struct {
	LaunchID string `json:"launch_id"`
	Status   Status `json:"status"`
	Reason   string `json:"reason,omitempty"`
}

// Allocation describes simultaneous minimum usable resources. Zero means
// unspecified; positive quantities must be at most MaxQuantity. CPU is measured
// in millicores (1000 = one CPU); memory and scratch are bytes. Providers may
// round up internally. Report only guaranteed allocation facts to c2j.
type Allocation struct {
	CPUMillis    int64 `json:"cpu_millis" yaml:"cpu_millis"`
	MemoryBytes  int64 `json:"memory_bytes" yaml:"memory_bytes"`
	ScratchBytes int64 `json:"scratch_bytes" yaml:"scratch_bytes"`
	// Base is a nonempty execution-environment selector. Match case-sensitively
	// against a concrete base name or an explicitly declared family alias.
	// Without '*', require equality. Each '*' anywhere in the selector matches
	// zero or more characters, including '/'; repeated '*' characters are allowed.
	// '*' alone matches any base. Every other character is literal: there are no
	// character classes, escapes, regex, version-range, or hierarchy semantics.
	// For example, ubuntu-24.04-arm64 can declare the family aliases ubuntu and
	// linux, matching ubuntu-24*arm64, ubuntu*, and linux*. Aliases must describe
	// the execution environment and follow the host's/provider's agreed contract.
	// Any matching base satisfying all other requirements is acceptable; no
	// newest/best preference is implied. Preserve the selector across fallback,
	// but report the resolved environment, not the pattern, as facts to c2j.
	// Base resolution, alias catalogs, and matching implementation live outside
	// this package. There are no separate OS, architecture, or version fields.
	Base string `json:"base" yaml:"base"`
	// Capabilities is a set of required names with no fixed vocabulary. All must
	// be supplied; order and duplicates have no meaning. Match names exactly,
	// without wildcard or implied capabilities. Empty means no requirements.
	// Providers must establish these capabilities before reporting them to the
	// executor as allocation facts; the request alone is not proof of availability.
	Capabilities []string `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
}

// JobRef identifies the assigned job without carrying credentials. All fields
// are required. Preserve this reference on provider-owned resources and listings.
// The host must ensure it identifies the same job as the supplied Lease; c2j
// validates the lease against JobDB before executing.
type JobRef struct {
	// JobDB is an executor-reachable endpoint, including any tenant path. It must
	// not embed credentials; the exported lease provides execution authority.
	JobDB string `json:"jobdb"`
	// InstanceID is the configured JobDB identity, independent of endpoint
	// rewrites needed to reach JobDB from different execution environments.
	InstanceID string `json:"instance_id"`
	TenantID   string `json:"tenant_id"`
	JobID      string `json:"job_id"`
}

// Lease contains a nonempty, opaque exported JobDB lease, forwarded unchanged
// to c2j. It is a bearer credential, not a controller discovery/claim credential.
// Formatting redacts it; string conversion and JSON encoding deliberately expose
// its contents for delivery. Never log serialized launches or raw lease data.
type Lease string

func (Lease) String() string     { return "[REDACTED]" }
func (l Lease) GoString() string { return l.String() }

// Launch assigns one c2j execution attempt, not a job's lifetime. LaunchID is
// 1..128 characters from [A-Za-z0-9._-]. The complete assignment is immutable
// and reused unchanged on fallback. Honor all constraints or decline before
// side effects. Make one native launch attempt; never retry an ambiguous start.
// Durable journals, replay support, and terminal history are not required.
type Launch struct {
	LaunchID string `json:"launch_id"`
	Allocation
	Job JobRef `json:"job"`
	// Start c2j with this exact lease using a protected channel (for example,
	// stdin with --lease-file -). Never expose it in child argv/environment,
	// diagnostics, or instance listings. Do not claim replacement work.
	Lease Lease `json:"lease"`
	// Zero delegates execution timeouts to the executor. Positive values are an
	// infrastructure runtime cap in seconds, at most 31536000 (one year).
	TimeoutSeconds int64 `json:"timeout_seconds,omitempty"`
	// If present, actual startup must occur strictly before this timestamp.
	// Decline unsupported constraints before side effects. Provider-owned runner
	// queues require this deadline; ordinary native scheduling delays do not.
	StartBefore *time.Time `json:"start_before,omitempty"`
}

// Instance describes provider-owned active compute, not executor/job status.
// ID is stable and unique within a provider; a launch may have multiple instances.
// State is queued, starting, running, paused, or stopping. Exclude terminal work.
// Preserve the original JobRef and LaunchID; never include lease credentials.
// Use an empty non-nil Refs slice when no native references exist (for example,
// before runner assignment).
type Instance struct {
	ID        string     `json:"id"`
	LaunchID  string     `json:"launch_id"`
	State     string     `json:"state"`
	Job       JobRef     `json:"job"`
	Refs      []string   `json:"refs"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
}

// ListRequest selects a page of active instances. Keep LaunchID unchanged across
// pages. PageToken is opaque and provider-owned, at most 16384 bytes.
type ListRequest struct {
	PageSize  int    `json:"page_size"` // 1..100; zero defaults to 100.
	PageToken string `json:"page_token,omitempty"`
	LaunchID  string `json:"launch_id,omitempty"` // Optional exact-match filter.
}

// ListResponse is a changing view, not an atomic snapshot or historical ledger.
// Items should be an empty non-nil slice for empty pages. Continue while
// NextPageToken is nonempty, even if Items is empty. Absence does not establish
// non-acceptance, completion, or free capacity. Native inventory may lag handoff.
type ListResponse struct {
	Items         []Instance `json:"items"`
	NextPageToken string     `json:"next_page_token,omitempty"`
}
