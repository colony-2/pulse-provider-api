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
and baseline 3 only. A cookie alone is not authentication. Negotiation checks all
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

## Outbound providers and the central collector

A provider behind NAT can dial a central server instead of exposing a public
endpoint. The central server starts one `Collector`, then reads a fresh snapshot
for each invocation. It supplies the returned ordinary `provider.Provider`
handles to its own placement logic. Local `Start`/`Serve` remains available.

```go
collector, err := plugin.StartCollector(serverContext, listener, plugin.CollectorOptions{
    TLSConfig: hostTLS,
    Authorize: authorizeProvider,
})
if err != nil { return err } // The caller still owns listener on failure.
defer collector.Close()

// Inject this interface into invocation handling; do not cache startup membership.
var providers plugin.ProviderCollector = collector
snapshot, err := providers.Snapshot()
if err != nil { return err }
// place(ctx, snapshot.Providers, launches) is your existing placement policy.
```

`StartCollector` returns promptly with zero providers and takes ownership of the
listener only on success. Its context bounds the entire collector lifetime.
`Wait()` reports fatal collection failure; an application supervisor must observe
it. An empty running collector returns an empty successful snapshot. A stopped
or failed collector returns `ErrUnavailable` and wakes change waiters.

Snapshots contain copied, ID-sorted entries with `ID`, `Generation`, and
`Provider`, plus a membership revision beginning at one. They perform no RPCs or
reservations. Each join/departure advances the revision. Optional
`WaitForChange(ctx, revision)` returns current membership immediately if already
newer, otherwise waits independently of other observers. Future revisions are
invalid, cancellation releases the waiter slot, and excess waits return
`ErrSaturated`. Slow observers see the newest snapshot, without an event backlog.

For ordered individual transitions, `*Collector` additionally implements the
optional `ProviderChangeSource` interface. `ProviderCollector` itself still has
only `Snapshot` and `WaitForChange`, so existing implementations need no new method.

```go
subscription, err := collector.OnProvidersChanged(observerContext,
    func(ctx context.Context, change plugin.ProviderChange) {
        for _, p := range change.Joined {
            fmt.Printf("joined=%s/%d revision=%d\n", p.ID, p.Generation, change.Revision)
        }
        for _, p := range change.Left {
            fmt.Printf("left=%s/%d revision=%d\n", p.ID, p.Generation, change.Revision)
        }
    })
if err != nil { return err }
defer subscription.Cancel()
// A supervisor monitors subscription.Wait() independently of collector.Wait().
// Continue obtaining a fresh Snapshot() for each invocation.
```

Registration atomically queues an `initial` event at the current revision, with
all ready providers sorted by ID in `Joined` and an empty `Left`. Even an empty
baseline is delivered; registration does not advance membership or wake snapshot
waiters. Later `membership` events contain exact joins or departures in revision
order, including providers that leave before their join callback executes.
`ProviderChange` contains `Kind`, `Revision`, `Reason`, `Joined`, and `Left`.
Typed constants expose the three kinds (`initial`, `membership`, `stopped`) and
fixed reasons (`initial`, `ready`, `connection_lost`, `probe_failed`,
`administrative_close`, `collector_closed`, `collector_failed`). Events contain no
provider error text, leases or configuration, and do no capability RPCs. The first
ready removal fixes its reason; these are SDK observations, not network diagnoses.

Each subscriber owns its payload slices. Handles remain session-bound and may
already be unavailable at delivery. Apply departures by both ID and generation;
callbacks are observations, not capacity reservations. Failed admission or
initialization emits no join. Healthy subscribers receive every delta once,
serially; different subscribers have independent workers. User code runs outside
collector locks and can read snapshots or cancel its subscription.

| Callback option | Zero-value default | Nonzero bound |
| --- | --- | --- |
| `MaxSubscriptions` | 32 | 1–1024 |
| `CallbackQueue` | 64 waiting events per subscriber | 1–4096 |
| `CallbackTimeout` | 5 seconds per dispatch | Positive duration |

Use keyed `CollectorOptions` literals: adding these optional fields changes the
positional struct layout. Subscription limits are independent of snapshot waiters.
The initial event counts against the queue until dispatched; one in-flight event
is additional. The callback context carries observer values and a deadline starting
at dispatch, shortened by an earlier observer or shared shutdown deadline.

`Cancel()` is nonblocking and idempotent. It clears pending events and cancels an
in-flight callback; already dispatched code may still start or continue. Observer
context cancellation affects only that subscription. `Wait()` is concurrent-safe
and reports one stable first terminal outcome:

| Outcome | `Wait()` result |
| --- | --- |
| Explicit cancel / observer cancellation | `context.Canceled` / observer context error |
| Queue overflow | `ErrSaturated` |
| Callback panic | `ErrCallback`, without panic text |
| Callback or shutdown delivery timeout | Matches both `ErrCallback` and `context.DeadlineExceeded` |
| Failed collector or transport cleanup | `ErrUnavailable`, unless already terminated |
| Final callback returned and orderly transport cleanup succeeded | nil |

Delivery failure never fails the collector. It ends that subscription rather than
silently dropping or coalescing events. Explicit re-registration starts a new
baseline; it cannot replay history lost during the gap. A callback that ignores
cancellation keeps its worker slot until it actually returns, bounding abandoned
goroutines across repeated registrations. The SDK cannot forcibly stop user code.
Keep callbacks short and cancellation-aware; do not call their own `Wait`, or
blocking collector shutdown, from inside them.

Shutdown immediately retires handles, then queues a newer `stopped` event after
pending deltas, with sorted remaining providers in `Left` even when empty. Callback
draining and transport disposal share one deadline and run concurrently. Overflow
or an expired budget means final delivery is not promised. Revision exhaustion
fails subscriptions without wrapping. To observe final delivery, keep the observer
context live through explicit `collector.Close()` and `subscription.Wait()`, then
cancel it. Canceling a shared collector/observer context instead detaches the
observer with its context error.

Each handle is a `*Connection`, permanently bound to one generation. It supports
`SubmitDetailed`, `Health`, and administrative `Close` as well as ordinary provider
calls. Invocation callers must not close their candidates. A departure invalidates
retained handles; reconnect creates a fresh handle. Duplicate identities are
rejected while reserved, including during initialization, without replacing their
owner. Snapshots describe observed connectivity, never capacity or guaranteed
reachability. Probes use only the control RPC and bypass business-call permits.

Provider entry points reuse `Factory`, `Init`, strict config helpers, and the
existing wire adapters:

```go
err := plugin.Run(providerContext, factory, plugin.RunOptions{
    Address: "central.example:8443",
    TLSConfig: clientTLS,
    Init: plugin.Initialization{
        Name: "worker-alpha", BaseDir: "/etc/my-provider",
        ConfigYAML: localConfig,
    },
    Serve: plugin.ServeOptions{Implementation: "my-provider", Version: "1.0"},
})
```

`Run` opens no listening port. It serves gRPC on its outbound authenticated socket; the
collector adopts that accepted socket as its gRPC client exactly once. Both transports use the
same baseline-3 messages, including independent observation streams. HashiCorp
[go-plugin v1.6.2](https://github.com/hashicorp/go-plugin/tree/v1.6.2) explicitly
excludes real-network plugins; this additive transport uses gRPC's socket
adapters and the SDK's shared call safety instead of adding a tunnel library.

Authentication and transport are separate explicit selectors. Their zero values
retain the existing certificate exchange:

| `AuthMode` | `Transport` | Requirements |
| --- | --- | --- |
| `ReverseAuthMTLS` (default) | `ReverseTransportTLS` (default) | Verified mutual TLS 1.3 and `Authorize` |
| `ReverseAuthMutualKey` | `ReverseTransportTLS` | Verified server TLS 1.3 and an identity-bound shared key |
| `ReverseAuthMutualKey` | `ReverseTransportHTTP` | Plaintext **test environments only**, with an identity-bound key |
| `ReverseAuthMTLS` | `ReverseTransportHTTP` | Invalid |

For key authentication, set `RunOptions.MutualKey` to the resolved bytes and
`CollectorOptions.MutualKeys` to `map[string][]byte{"worker-alpha": expectedKey}`.
Configure a distinct key for each identity. The collector validates every map
entry at startup and authenticates before identity reservation, provider
construction, or readiness. `Authorize` is required for mTLS and rejected in key
mode; key material in mTLS mode is rejected. Unknown selectors fail with
`ErrInvalid` before networking. One collector uses one mode pair, with no fallback.

Both TLS configurations require TLS 1.3, ordinary certificate verification, and
exactly one ALPN: `ReverseALPN` (`pulse-provider-reverse/3`) for mTLS, or
`ReverseKeyALPN` (`pulse-provider-reverse-key/3`) for key mode. mTLS requires host
`ClientAuth: tls.RequireAndVerifyClientCert`, explicit `ClientCAs`, a host
certificate, and provider `RootCAs`, verified `ServerName`, and client certificate.
Key TLS requires a host certificate and `ClientAuth: tls.NoClientCert`, plus
provider `RootCAs` and verified `ServerName`; no provider certificate is needed.
In HTTP mode both `TLSConfig` fields must be nil. Skip verification, weakened TLS,
and `GetConfigForClient` overrides are rejected. Certificate/key callbacks remain
caller-owned and must be safe for concurrent use.

The protocol layers use exactly one outbound provider TCP connection:

1. When selected, TLS verifies the server (and the provider in mTLS mode).
2. In key mode only, the provider writes `PPK1`, one key-type byte (`1`), two
   unsigned big-endian 16-bit lengths (identity then key), followed by identity
   and key bytes. Identities are case-sensitive UTF-8, 1–256 bytes, without control
   characters. Keys are exact nonempty byte strings up to 4096 bytes without CR
   or LF; spaces are significant. Other key types are reserved and rejected.
   The collector hashes configured and presented keys with SHA-256 and compares
   fixed-size digests using `crypto/subtle`, including a dummy comparison for
   unknown identities. This is not password hardening or constant-time ID lookup.
3. The collector writes the existing `PPR3`, one status byte, a two-byte big-endian
   identity length, and the configured identity. Status 0 admits; 1 generically
   denies; 2 means duplicate reserved identity; 3 means provider capacity; 4 means
   generation exhaustion. Rejections have zero identity length and reflect no
   secrets. The provider checks the admitted identity against its local name.
4. The collector adopts that socket as its HTTP/2/gRPC client; the provider serves
   the existing protobuf service on its dialed socket. `Negotiate` and successful
   `Initialize` precede ready publication. Callbacks remain local notifications.

`http` means plaintext reverse HTTP/2 **after this custom admission exchange**.
It exposes the key and all provider traffic, provides no server authentication,
and offers no replay protection. Use disposable keys in isolated test environments.
It is not an ordinary HTTP/1 endpoint or standard h2c URL. Use direct TCP or TCP
passthrough: ordinary HTTP proxies and HTTPS ingress cannot transparently carry
this protocol. No provider listener, second tunnel, new service, or protobuf
change is involved.

Existing mTLS users need no changes. Key users must upgrade both ends and select
matching auth and transport modes. Mixed fleets need separate collectors or a
coordinated cutover. Options copy initialization, key bytes, key maps (as private
digests), and trust pools at startup; callers must not mutate them concurrently
with startup. Later mutations do not rotate credentials. Replace the collector
and provider lifecycles and close existing sessions to revoke a key. Scope
credentials to their intended tenant/environment and prefer randomly generated
secrets. Other key types, reciprocal challenges, expiration, and live rotation
are deferred.

Initialization belongs to the provider machine: the host supplies only its
authorized identity. Host config, paths, executor-default overrides, and identity
mismatches are rejected before construction. Every new session constructs a fresh
provider. The provider must apply or reject its local executor defaults.

Collector defaults retain the local SDK's startup, business call, shutdown,
message, concurrency, and probe bounds. Additional finite defaults are 128
providers, 32 pending connections/authorization callbacks, 1024 change waiters,
and 16 KiB incoming HTTP/2 metadata (provider side also 16 KiB). The provider
reserves business concurrency, log subscription capacity, and two control streams. Excess admissions
do not displace ready providers. TLS, key exchange, admission, and initialization share the startup bound;
a callback ignoring cancellation keeps its work slot until it returns.

`Run` reconnects only when explicitly enabled through `Reconnect RestartPolicy`.
It reuses bounded delay, multiplier, jitter, and healthy-reset conventions, with
no replacement until the old session's cleanup succeeds. Certificate, key, identity,
admission, negotiation, and initialization rejection are terminal. Reconnecting
never queues, resends, or resolves a previous submission. A received shutdown
returns nil unless cleanup failed; context cancellation returns its context error.

Collector shutdown retires handles immediately and disposes peers concurrently
under one shutdown budget. Run cancels maintenance and bounds cleanup; accepted
native work remains detached and enforces its own runtime cap. An uncooperative
factory or failed cleanup ends Run with `ErrCleanup` and prevents reconnect.
Late cleanup registration runs once without publishing readiness. The SDK cannot
kill caller-owned goroutines or remote processes; use external supervision when
forced termination is needed. These network checks run on Linux; additional
platform certification and production Pulse listener/handler wiring are separate.

The independent commands in `examples/reverse-host` and
`examples/reverse-provider` accept `-auth mtls|mutual-key` and
`-transport tls|http`, retaining mTLS/TLS defaults. `-key` still names a certificate
private-key file. For shared secrets choose exactly one of `-auth-key-stdin` or
`-auth-key-env NAME`; the host also requires `-auth-key-id ID` for its single
expected identity. Embedded collectors can bind several identities to distinct
keys. Certificate `-identities` is rejected in key mode.

For example, with a key already supplied through each process's environment:

```sh
# Server-verified TLS; the provider does not need a client certificate.
go run ./examples/reverse-host -listen 127.0.0.1:8443 \
  -auth mutual-key -auth-key-id worker-alpha -auth-key-env PROVIDER_KEY \
  -cert collector.crt -key collector.key
go run ./examples/reverse-provider -address 127.0.0.1:8443 -name worker-alpha \
  -auth mutual-key -auth-key-env PROVIDER_KEY \
  -ca ca.crt -server-name collector.test -config provider.yaml

# Explicit plaintext testing; these example values are disposable.
{ printf 'disposable-test-key\n'; cat; } | go run ./examples/reverse-host \
  -listen 127.0.0.1:8443 -auth mutual-key -transport http -auth-key-id worker-alpha -auth-key-stdin
printf 'disposable-test-key\n' | go run ./examples/reverse-provider \
  -address 127.0.0.1:8443 -name worker-alpha -config provider.yaml \
  -auth mutual-key -transport http -auth-key-stdin
```

Run the host and provider in separate terminals. The host's `cat` keeps the pipe
open for `invoke` and `quit` commands after its first key line. Neither source
puts a real key on the command line.
`LoadMutualKey(MutualKeySource{Stdin: reader})` reads the first bounded line,
removing only its terminal LF or CRLF; nonempty EOF is accepted. Environment
loading uses explicit `EnvName` and injected `LookupEnv`. Missing, empty,
multiline, oversized, or conflicting sources fail without exposing values or
underlying reader errors. The SDK never implicitly reads globals; `Run` accepts
only resolved bytes. It never injects a key into initialization, YAML, RPC
metadata, executor defaults, or callbacks. Original process environment and
native executor inheritance remain the embedding application's responsibility.

The host uses the same buffered reader for its key and subsequent `invoke` /
`quit` commands, preserving prefetched lines. The provider ignores later lines.
The examples bound owned stdin acquisition to 30 seconds and honor interruption
before opening sockets. On Unix they reopen `/dev/stdin` so inherited pipes
support deadlines under Go 1.22. Inputs or platforms without interruptible read
support fail with guidance to use the environment source. The synchronous SDK
loader itself cannot cancel an arbitrary reader; `LoadMutualKeyInput` handles an
explicitly owned deadline-capable file and clears its deadline afterward.

The standard-library `scripts/reverse-consumer.go` helper generates temporary
trust and runs the copied commands under Go 1.22.12. It requires the legacy mTLS
two-provider join/departure and ordered callback scenario, plus separate alpha
and beta key scenarios on TLS and plaintext using both sources. Child output is
captured and checked for accidental key disclosure. `TestIndependentConsumersAndExamples`
asserts every scenario, including local subprocess compatibility; the published
consumer check runs the same helper without a module replacement.

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

`SubmitDetailed(ctx, launches, options)` returns results, invocation-correlated
`SubmissionObservations`, a log subscription, and the admission error from one
handler invocation. `Submit` uses the same operation and
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
changes. Consumer tests compile both candidate modules with Go 1.22.12 and build/run
copied local and reverse author/host commands in an isolated module with temporary
candidate replacements. The reverse pair exercises invocation snapshots and callbacks
as alpha and beta join and leave. No replacement is committed to either module.

Release this directory through the repository publication route with a
`plugin/vX.Y.Z` tag; the nested module's version is `vX.Y.Z`. A root-only tag does
not version this module. After publication, run `make release-check VERSION=vX.Y.Z`.
That check downloads the exact SDK version into a fresh module cache with no
replacements, reports the resolved API pin and SDK version, and builds/runs the
independent local and reverse example pairs, including collector membership changes.
A successful local replacement check does not prove
published availability. Publication and this check do not depend on Pulse adoption.

The repository's build verify hook runs `make check`. After the managed recipe
publishes its reviewed merge result, the publish hook checks that its workspace
and published branch identify that result. It does not tag or release the SDK.
Publish the root contract first, then pin this module to that published root
revision and separately request a versioned SDK release and fresh independent
release check. The current `plugin/go.mod` pin predates the launch-log contract;
local candidate checks do not claim a published SDK release. Implementation work
itself does not merge, push, tag, or invoke publication hooks.


## Launch-log listeners

This is a breaking migration of the required root `Provider.Submit` method:

```go
results, subscription, err := p.Submit(ctx, launches, api.SubmitOptions{})
// subscription == nil: no observation stream, queue, or callback worker.
results, observations, subscription, err := p.SubmitDetailed(ctx, launches, options)
```

A present `options.Listener` supplies a separate context, callback, and duration.
Duration zero means forever; every positive representable `time.Duration` is
preserved exactly in nanoseconds, without a startup cutoff or cumulative output
quota. It starts before registration setup and native preparation. Logs may
arrive before Submit returns. The admission context and `CallTimeout` govern
only admission. Logging failures never trigger replay or change item outcomes.

Inspect `subscription.Coverage()` during listening. `Stop`, context cancellation,
a wrapped `api.ErrStopListening`, or elapsed duration closes `Done` independently
of native work. `Wait(ctx)` returns a stable `ListenerResult` and its cause;
context expiry on a wait leaves listening active. Snapshots own their slices.
Normal source closure drains preceding records. Other terminal causes discard
queued records; a committed in-flight callback receives cancellation and may
return later. Callbacks can safely call Stop. Panic text stays private.

`LogOptions` on StartOptions, ServeOptions, and CollectorOptions bounds resources
separately from membership callbacks. Run uses its ServeOptions. Defaults are
32 subscriptions, 64 queued records per subscription, a five-second callback
deadline, and a five-second setup deadline. Positive resource overrides are
accepted up to 1024 subscriptions and 4096 queued records; negative values are
invalid. These delivery bounds never replace listener duration zero. Stuck
callbacks retain occupied capacity until return, including across collector
reconnections, with no replacement-worker loop. Capacity and overflow failures
are explicit subscription results while otherwise valid admission proceeds.

Both transports use one authenticated bidirectional Observe stream per listener
alongside the single unary Submit. Register binds an invocation, exact launch set,
identity, and duration before native dispatch. Missing, stopped, expired, or
failed registration produces canceled native logging while preserving admission.
Generation fences reject duplicates and retired invocations. Active forever
listeners survive invocation-window advancement. Reconnection does not resume
old logs or resubmit work. Shutdown and session loss end existing handles.

Baseline 3 changes protobuf namespace, process handshake, outbound ALPN/admission
magic, and negotiation together. Both peers must rebuild; baseline 2 fails before
factory initialization or native work. Records are bounded to 4096 bytes and
fragment within negotiated envelopes. Coverage revisions publish atomically.
Invalid frames, sequences, fragments, or source metadata terminate logging with
stable `errors.Is` categories. No peer exception text is serialized. Output and
private observations remain outside public health.

Run `make check` at the repository root to validate both unpublished modules in
an isolated candidate tree. It temporarily replaces the SDK's root dependency
there, runs tests/vet/races/tidiness and pinned wire generation, and checks Go
1.22.12 independent consumers against both candidates. No replacement is
committed. These checks do not claim that a published SDK resolves this API.

Release the root contract first. Once its commit is published, pin plugin/go.mod
to that root revision, choose the next SDK version, and run the release checks
before publishing the SDK. The build publication hook verifies Git publication;
it does not tag this intermediate SDK against the older root dependency. Actual
module release is separate from this API implementation. Real-provider capture
and downstream adoption are also separate work.
