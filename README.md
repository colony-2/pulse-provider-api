# Pulse provider API — proposal

A small, standard-library-only Go contract for implementing Pulse c2j execution
providers in independent repositories. This is a review draft; Pulse does not
consume it yet.

```go
import (
    "context"
    provider "github.com/colony-2/pulse-provider-api"
)

// The entire required interface:
type Provider interface {
    Submit(context.Context, []provider.Launch) ([]provider.Submission, error)
    List(context.Context, provider.ListRequest) (provider.ListResponse, error)
}
```

The module has one package (`provider`), one source file, and no external module
dependencies. Its only imports are `context` and `time`. Go 1.22 is the proposed
minimum. JSON/YAML struct tags are passive metadata and require no codec dependency.

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

`Base` selects an execution environment by concrete name or family alias, with
optional wildcard matching. It has no mandatory OCI syntax and no separate OS,
architecture, or version fields. Providers resolve it to a runner base, VM
template, container image, or another environment satisfying the request.

For example, a concrete base named `ubuntu-24.04-arm64` could declare the aliases
`ubuntu` and `linux`:

| Selector | Matches |
| --- | --- |
| `ubuntu-24.04-arm64` | That concrete base. |
| `ubuntu-24*arm64` | Bases with names or aliases starting with `ubuntu-24` and ending with `arm64`. |
| `ubuntu-24.04*` | Bases with names or declared aliases starting with `ubuntu-24.04`. |
| `ubuntu*` | Bases with names or declared aliases starting with `ubuntu`. |
| `linux*` | Bases with names or declared aliases starting with `linux`. |
| `*` | Any base satisfying the other requirements. |

Matching rules:

- The selector must be nonempty. Without `*`, require a case-sensitive exact
  match against the concrete name or a declared alias.
- Each `*` anywhere matches zero or more characters, including `/`. Repeated
  stars are allowed; `*` alone matches any base. All other characters are literal.
  There are no character classes, escapes, regex, version ranges, or implicit
  hierarchy semantics. Match the entire concrete name or alias.
- Family aliases are explicitly declared, not inferred from the concrete name.
  `linux*` matches the example through its `linux` alias. An exact alias such as
  `linux` can also match multiple concrete bases.
- Names and aliases have agreed meanings across providers. For example, `linux`
  describes the job's execution environment, not merely the provider's host.
  A base label promises only the environment defined by its contract; tool and
  service requirements should be explicit capabilities where appropriate.
- Any match satisfying resource, capability, and deadline requirements is valid;
  there is no implied newest/best ordering. Broad selectors may resolve to
  different releases, architectures, or tool sets across attempts or providers.

Preserve the requested selector across fallback. Before executing, resolve a
concrete environment and report its actual identity/platform and guaranteed
allocation to c2j; do not advertise a wildcard selector as an allocation fact.
The alias catalog, validation, matching, and resolution implementations remain
outside this library. Existing OS/architecture/variant constraints must be mapped
to selectors that preserve them or declined, never silently broadened.

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

## Scope and integration after review

No dependency on Pulse core, c2j, JobDB, cloud SDKs, OCI parsers, YAML libraries,
gRPC, or the plugin framework. No factory, global registry, config schema,
capability negotiation, lifecycle interface, transport, validation helpers,
ID generation, or pagination implementation. Each implementation can expose its
own typed constructor and cleanup; the host owns wiring and lifetime.

A host imports a provider library, constructs it, and supplies it to Pulse after
integration. Providers must be linked into that host binary. This draft does not
add dynamic loading to the stock Pulse executable.

Integration will need to:

1. Adopt these shared boundary types and adapt Pulse's allocation/assignment
   construction. Keep validation, base matching/resolution, and alias catalogs in
   core or implementations. Existing validation methods cannot be attached to aliases
   of external types; use core helper functions or explicit internal adapters.
2. Add host wiring for externally constructed `provider.Provider` instances,
   including implementation-specific configuration and cleanup.
3. Move c2j invocation and secure lease delivery into providers or shared adapter
   code outside this module. Pass the resolved environment and verified execution
   allocation facts to c2j rather than the requested selector.
4. Adapt built-in providers and existing transports, including typed job
   correlation, base resolution, and platform conversion. Existing remote/plugin
   contracts need explicit mapping or versioning; this is not a compatible rename.

The initial draft was based on Pulse commit
`1a8c516450289290316302be7fb08908e505f0ad` (`pkg/compute/compute.go` and `list.go`).
This proposal now deliberately changes that contract. No Pulse core edits are
part of this draft.

## Local checks

```sh
GOWORK=off GOPROXY=off go test ./...
GOWORK=off GOPROXY=off go vet ./...
GOWORK=off GOPROXY=off go list -m all
```

The final command should list only this module.
