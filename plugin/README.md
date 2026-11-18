# Provider plugin SDK

`github.com/colony-2/pulse-provider-api/plugin` is an optional, independently
versioned nested module. The root API remains Go 1.22 with only standard-library
dependencies. This module also declares Go 1.22 and pins the published root API
`v0.0.0-20261105011626-401f11e595a8`. It imports no Pulse implementation code.

A provider command calls `Serve(factory, options)` and handles its returned error.
A host calls `Start(ctx, options)`, uses the returned `*Process` directly as
`provider.Provider`, and eventually calls `Close`. See the [provider command](examples/provider/main.go)
and [independent host](examples/host/main.go). The example advertises one base and
explicitly declines submissions because it does not implement native execution.

```sh
go build -o /tmp/example-provider ./examples/provider
go run ./examples/host /tmp/example-provider
```

## Construction and ownership

`Factory` receives a bounded construction context and an `Init` containing the
provider name, raw YAML, absolute base directory, explicit executor defaults and a
separate `Lifetime` context. It returns the API provider, a cleanup callback, and
an error. Cleanup returned alongside an error is registered and run once. A
factory that never returns cannot register a callback; the host forcibly reaps it.
Cleanup cannot be guaranteed after a crash.

`Lifetime` bounds maintenance. Accepted native execution must be detached from
factory, RPC, plugin, host and cleanup lifetimes, with its lease authority and
runtime cap enforced independently by the provider. The SDK never creates c2j
commands, acquires leases, supplies native resource enforcement or removes native
execution state. SDK cleanup targets the SDK process and private transport files,
not process groups or arbitrary descendants. Put durable native state outside the
SDK's private working directory. Live fixtures verify this boundary with a
provider-owned detached runner that progresses after faults and stops at cap expiry.

`Start`'s context governs startup only. A ready process remains alive after that
context expires. All later calls have independent caller and SDK bounds. Health
probes use a control RPC and never discover, reconcile, provision, submit or list.

Pulse supplies configuration precedence, executable/path resolution, credentials,
executor defaults, registration, health translation, restart policy and controller
lease/fallback decisions. Other hosts supply their own values. The SDK does not
import Pulse or choose its controller policy.

## Options and safeguards

Zero-valued durations and limits select these finite defaults:

| Option | Default |
| --- | --- |
| Startup/factory timeout | 30 seconds |
| Business call timeout | 30 seconds |
| Total shutdown timeout | 5 seconds |
| Concurrency | 4 |
| Message limit | 16 MiB |
| Probe interval / timeout | 10 seconds / 5 seconds |
| Restart replacement attempts | 0 (disabled) |
| Enabled restart initial / maximum delay, multiplier | 1 second / 30 seconds, 2 |
| Restart jitter / healthy reset | 0 / disabled |

Message limits range from 1 KiB to 128 MiB; host and server negotiate the smaller
limit, including protobuf envelopes. Configuration has a separate 1 MiB bound.
Negative durations/limits, contradictory restart bounds, invalid checksums and
reserved environment names fail validation. Set an absolute executable path and
absolute initialization base directory. `Env` is the whole selected child
environment: no ambient inheritance. Reserved `PLUGIN_*`/`PULSE_PLUGIN_*` values
and TMPDIR cannot be overridden. Executor defaults travel in initialization, not
the ambient plugin environment. A factory explicitly applies or rejects them.
Selected environment is not a sandbox for untrusted provider code.

The transport is HashiCorp go-plugin v1.6.2 with mutual TLS, a private Unix socket,
and baseline 2 only. A cookie alone is not authentication. Negotiation checks all
required semantics before construction; future execution semantics need explicit
negotiation support. Optional trusted SHA-256 verification precedes execution.
Framework logging is disabled; public health contains only identity, state,
generation, restart count and SDK failure codes. Provider authors must keep secrets
out of returned reasons/errors; private method output is not automatically redacted.

Process safeguards are currently implemented and live-tested on **Linux** only.
`Start` and `Serve` explicitly fail on other operating systems. The child inherits
a private parent-loss pipe, marked close-on-exec before provider initialization.
Parent loss cancels maintenance, bounds cleanup and stops the server; a watchdog
kills a child that ignores shutdown. No helper process or process-group kill is used.

Restart policy exposes maximum replacement attempts, initial/maximum delay,
multiplier, jitter fraction and a continuous healthy reset interval. Replacement
initialization failures count toward the budget. A supervisor disposes the previous
generation before launching another. Exhaustion leaves the handle unavailable
until Close. Calls during restart fail immediately; only new calls use a ready
replacement. Close cancels timers and calls, bounds cleanup and reaps the SDK
process within one shutdown budget. Concurrent/repeated Close calls share one
completion and error. Forced termination or failed cleanup returns `ErrCleanup`.

## API results and submission uncertainty

The wire preserves all root API fields, byte-exact leases/cursors (including
invalid UTF-8), signed integer quantities, selector strings, required capabilities,
ordered complete/partial catalogs, all instance fields, and optional timestamps
with nanoseconds in years 1–9999. Page size zero becomes 100; other values must be
1–100. No selector matching or placement policy is implemented by the SDK.

A completed RPC carries both method data and an optional method error. Error
presence survives even with empty text; `errors.Is` preserves canceled/deadline
classification, not arbitrary Go concrete error types. As the root interface
requires, callers discard discovery/list data when an error accompanies it.
Valid explicit submission outcomes remain authoritative alongside method errors.
Missing, duplicate, unknown-status or malformed outcomes affect only those
requested launches and normalize to `unknown` in request order. Extra outcomes
remain diagnostic observations. Accepted requires an empty reason; other statuses
require a reason. No error text determines admission.

`SubmitDetailed` returns results, invocation-correlated `SubmissionObservations`
and error from one handler invocation. `Submit` uses the same operation and
discards observations. Observations include generation/invocation identifiers,
original ordered entries (including duplicates), method error presence/class/text,
and complete/truncated/not_captured markers. Capture is limited to 64 KiB of text
and 256 entries independently of authoritative results. These observations are
private diagnostics, not public health and not admission decisions.

Local request validation, message-size checks, readiness checks and nonblocking
permit acquisition precede possible dispatch. They can prove non-start. After
possible dispatch, transport failure, cancellation, process death or lost/oversized
responses yield unknown unresolved outcomes. Requests and authoritative responses
are never shortened to fit. The SDK disables gRPC retries/wait-for-ready and commits
server headers before entering Submit. A bounded generation-local invocation fence
rejects duplicate and retired tokens without replaying results. It never queues a
submission for another generation, reconnects to resend it, or offers a retry API.
Hosts must not immediately fall back or release unused leases for unknown results.

## Configuration helpers

`DecodeConfig` accepts exactly one strict YAML mapping. It rejects aliases,
anchors, merges, duplicate/non-string keys, unknown typed fields, scalar coercions,
custom decoders, inline fields, multiple documents and excessive depth/node counts.
Use concrete struct/map/slice types; interfaces are intentionally rejected.
There is no interpolation or secret discovery. `ResolvePath` requires an absolute
base and resolves relative paths; it does not enforce containment.

## Verification and independent release

From the repository root, `make check` explicitly tests/vets both modules, runs
hosting race checks, checks nested tidiness and reproduces generated bindings.
`make generate` requires protoc 36.2 and uses pinned protoc-gen-go v1.35.2 and
protoc-gen-go-grpc v1.5.1 in a temporary directory. It fails if generated content
changes. Consumer tests compile the published root API with Go 1.22.12 and build/run
copied author/host commands in an isolated module with a temporary candidate
replacement. No replacement is committed to either module.

Release this directory through the repository publication route with a
`plugin/vX.Y.Z` tag; the nested module's version is `vX.Y.Z`. A root-only tag does
not version this module. After publication, run `make release-check VERSION=vX.Y.Z`.
That check downloads the exact SDK version into a fresh module cache with no
replacements, reports the resolved API pin and SDK version, and builds/runs the
independent author and host. A successful local replacement check does not prove
published availability. Publication and this check do not depend on Pulse adoption.

The repository's build verify hook runs `make check`. After the managed recipe
publishes its reviewed merge result, the publish hook reads `plugin/VERSION`,
checks that its workspace and published branch identify that result, and publishes
the corresponding `plugin/` tag. An existing tag is reusable only for identical
module contents; changed contents require a version bump. It then runs the fresh
independent release check. Tagging or resolution failures fail the publication
hook and retain diagnostics; they must not be reported as a successful release.
Implementation work itself does not merge, push, or invoke this hook.
