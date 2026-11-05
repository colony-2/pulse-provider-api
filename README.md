# Pulse provider API

A small, standard-library-only Go contract for implementing Pulse c2j execution
providers in independent repositories. Pulse and external providers consume this
module. The required `GetCapabilities` method is a new breaking interface change;
providers must adopt it before Pulse updates its dependency.

```go
import (
    "context"
    provider "github.com/colony-2/pulse-provider-api"
)

// The entire required interface:
type Provider interface {
    GetCapabilities(context.Context) (provider.Capabilities, error)
    Submit(context.Context, []provider.Launch) ([]provider.Submission, error)
    List(context.Context, provider.ListRequest) (provider.ListResponse, error)
}
```

The module has one package (`provider`), one source file, and no external module
dependencies. Its only imports are `context` and `time`. Go 1.22 is the minimum. JSON/YAML struct tags are passive metadata and require no codec dependency.

## Allocation and assignment

[provider.go](provider.go) defines allocation requirements separately from job
identity and execution authority. A launch looks like this:

```go
launch := provider.Launch{
    LaunchID: "attempt-001",
    Allocation: provider.Allocation{
        Base:         "ubuntu-24.04-arm64",
        CPUMillis:    2000,
        MemoryBytes:  4 << 30,
        Capabilities: []string{"docker", "docker.workspace-mounts"},
    },
    Job: provider.JobRef{
        JobDB:      "https://jobdb.example/acme",
        InstanceID: "production",
        TenantID:   "acme",
        JobID:      "job-123",
    },
    Lease: provider.Lease(exportedLease), // Supplied by the host; never log it.
}
```

`Base` selects an execution environment by concrete name with optional `*`
wildcards. It has no mandatory OCI syntax and no separate OS, architecture, or
version fields. Providers resolve it to a runner base, VM template, container
image, or another environment satisfying the request. There are no aliases.

| Selector | Matches |
| --- | --- |
| `ubuntu-24.04-arm64` | That concrete base. |
| `ubuntu-24*arm64` | Names starting with `ubuntu-24` and ending with `arm64`. |
| `ubuntu-24.04*` | Names starting with `ubuntu-24.04`. |
| `ubuntu*` | Names starting with `ubuntu`. |
| `linux*` | Names starting with `linux`; never an Ubuntu label. |
| `*` | Any base satisfying the other requirements. |

Matching rules:

- Selectors and concrete names must be nonempty valid UTF-8, at most 4096 bytes,
  without NUL, spaces, tabs, CR, or LF. Concrete names cannot contain `*`.
- Without `*`, require a case-sensitive exact match against the concrete name.
- Each `*` anywhere matches zero or more characters, including `/`. Repeated
  stars are allowed. All other characters are literal: no character classes,
  escapes, regex, version ranges, or implicit hierarchy semantics.
- Names have agreed meanings across providers and describe the job's execution
  environment. A base promises only the environment defined by its contract;
  tool and service requirements should be explicit capabilities where appropriate.
- Any match satisfying all constraints is valid. `GetCapabilities` lists bases
  in provider preference order; the selector itself implies no newest/best order.

Preserve the requested selector across fallback. Before executing, resolve a
concrete environment and report that label and guaranteed allocation to c2j
(using `--execution-base` or `C2J_EXECUTION_BASE`); never advertise the selector
as an allocation fact. Catalogs, validation, matching, and resolution remain
outside this library. Existing constraints must be preserved or declined, never
silently broadened.

`Capabilities` is an open vocabulary represented as `[]string`. Matching is exact
and all-of, with set semantics: no ordering, wildcards, or implied capabilities.
Providers must establish actual availability before advertising allocation facts
to c2j. Requests alone are not proof of availability. c2j's name validation stays
outside this module.

`JobRef` contains the executor-reachable JobDB endpoint and stable instance,
tenant, and job identities. All four fields are required. The endpoint must be
credential-free. The host resolves any endpoint rewrite before submission and
ensures the reference identifies the same job as the supplied lease. Providers
preserve the reference and launch ID on native resources and active listings.
There is no arbitrary metadata map; ownership/version labels are implementation
details. JobDB's own job metadata stays in JobDB.

`Lease` is an opaque exported JobDB bearer credential. The provider starts c2j
with that exact lease and the guaranteed allocation; it does not claim replacement
work. c2j validates the lease with JobDB before executing. Lease formatting is
redacted, but string conversion and JSON serialization expose the original bytes
for delivery. This is not encryption. Never log serialized launches or expose
leases in argv, the child environment, or instance listings. A provider may use
stdin with `c2j run with-lease --lease-file -` internally.

The public API has no generic command, environment, working directory, or stdin
fields. Executor invocation and configuration belong to the provider. `Instance`
contains `JobRef` and `LaunchID` for correlation, and no lease field.

## Submission and listing

- `Submit` is batch-only (1–100), including single launches. Correlate results by
  launch ID. Return explicit per-item outcomes, including definite declines.
- Only `no_capacity`, `unsupported`, and `unavailable` permit fallback. Each
  guarantees this submission cannot start later. `accepted` means admission or
  handoff, not completion. `rejected` stops the attempt; `unknown` is uncertain.
- Preserve valid explicit results even alongside a Go error. Missing, duplicate,
  or invalid results are uncertain. Cancellation or an error alone never proves
  non-acceptance. Do not retry ambiguous launches or immediately fall back.
- Honor resource minima, the base selector, capability requirements, and deadlines.
  Decline unsupported constraints before side effects. The complete assignment
  is immutable and reused unchanged on fallback.
- `List` is required and read-only. Return only managed active compute in the
  provider's scope, with opaque pagination. A failed read returns an error;
  an empty successful list does not resolve an uncertain submission.
- Calls may overlap. Treat shared inputs/results as immutable, honor call
  cancellation, and keep accepted workload lifetime independent of call context.
  Runtime limits must continue to hold after the submitting host exits.

## Capability discovery

`GetCapabilities` is required on every provider. The initial response describes
bases and their optional execution capability names. Keeping it as a structured
response allows later additions without introducing more discovery methods.

```go
func (p *Provider) GetCapabilities(ctx context.Context) (provider.Capabilities, error) {
    if err := ctx.Err(); err != nil {
        return provider.Capabilities{}, err
    }
    return provider.Capabilities{
        BasesComplete: true,
        Bases: []provider.BaseCapabilities{
            {
                Name: "macos-sequoia-15.7.7-arm64",
                Capabilities: []string{"nix", "git", "node", "pnpm", "uv", "python"},
            },
        },
    }, nil
}
```

Advertise configured support, including busy or warming bases. Discovery must not
start VMs, prepare templates, reconcile work, or reserve capacity. Return data
that callers can read safely while other calls run. Do not include credentials,
endpoints with credentials, or arbitrary configuration. On error, the caller
discards the response and treats discovery as failed, not as proof of no support.

`Bases` is ordered, most preferred first, with unique concrete names. A Docker
implementation should list bases for the **daemon's native Linux architecture**
before emulated alternatives: ARM64 on Apple Silicon or ARM Linux; AMD64 on AMD64
Linux. An explicit selector always wins: never choose ARM64 for an AMD64-only
request. Ordering bases within a provider is separate from host policy for
choosing between Docker, Tart, and other providers.

Each listed base supplies a complete capability-name set. Empty means that no
optional capabilities can be supplied for that base. A provider unable to describe
a base's capability set leaves that base unlisted and marks the catalog partial.

| Response | Meaning for placement |
| --- | --- |
| Complete catalog, no matching base | Provider is incompatible with this base request. |
| Complete catalog, every matching base lacks a requested capability | Provider is incompatible with this capability request. |
| Partial catalog, no suitable listed base | Support is unknown; the provider may support an unlisted base. |
| Suitable listed base | Candidate only; resource, deadline, availability, and combined constraints still need admission. |
| Empty complete catalog | No supported bases, regardless of current utilization. |
| Zero-value response | Unknown support; safe fallback for implementations without discovery. |

`BasesComplete` is deliberately explicit. Dynamic container image references
cannot usually be enumerated. Such a provider can advertise its finite named
catalog with `BasesComplete: false`, while keeping dynamic requests eligible for
`Submit`. A complete finite VM catalog can set it to `true`. No caller may infer
incompatibility solely from absence in a partial catalog. For an exact listed
base, an absent capability name proves that particular base is incompatible.

These are support hints, not allocation facts or reservations. Do not remove a
base merely because its slots are occupied. A cached response can become stale;
`Submit` must always revalidate and return its normal outcome. Refresh frequency,
caching, validation, filtering, and matching stay in hosts/implementations.
Resource bounds and deadline support are outside this first discovery response.

## Adoption and placement follow-up

1. External providers, including Tart/Orchard, add the required method and report
   their configured bases and verified capability offerings. This module alone
   does not upgrade their guest c2j binaries or lease handling.
2. After those libraries adopt the interface, Pulse can update its dependencies
   and expose the same discovery data in startup diagnostics.
3. Pulse can then skip provably incompatible providers before submission, using
   the current leased request. Discovery must not replace lease-time validation,
   provider admission, or existing rules forbidding fallback after uncertainty.
4. Default local placement should try Docker before Tart (different priority
   tiers), and Docker should prefer native architecture for broad selectors.
   Explicit provider priorities and explicit base requests remain authoritative.
   A default such as `ubuntu-26*arm64` pins ARM64; native-first defaults on AMD64
   require an architecture-neutral selector or a deliberate new default policy.

The last two policies are **not implemented by this module**. At review time,
Pulse assigns automatic Docker and Tart equal priority and rotates within the
priority tier; its native base catalog tries ARM64 first on every host. Pulse
integration should correct both rather than relying on provider map order or
alphabetically sorted platform names.

## Scope

No dependency on Pulse core, c2j, JobDB, cloud SDKs, OCI parsers, YAML libraries,
gRPC, or the plugin framework. No factory, global registry, config schema,
lifecycle interface, transport, validation helpers, ID generation, or pagination
implementation. Implementations expose their own typed constructor and cleanup;
the host owns wiring and lifetime. Providers must be linked into that host binary;
this module does not add dynamic loading.

## Local checks

```sh
GOWORK=off GOPROXY=off go test ./...
GOWORK=off GOPROXY=off go vet ./...
GOWORK=off GOPROXY=off go list -m all
```

The final command should list only this module.
