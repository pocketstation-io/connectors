# PocketStation Connector Registry

Find the first-party packages that connect a PocketStation Session to an
external service.

This repository is the source registry and shared verification workspace for
connectors maintained by the PocketStation project. Each connector is an
independent package with its own protocol scope, compatibility contract,
documentation, tests, and release lifecycle.

The repository root is a catalog. Package-specific setup and implementation
guidance belongs inside each connector directory.

## Available connectors

| Connector | Package | Direction | Connects to | Release | Documentation |
|---|---|---|---|---|---|
| PocketStation Relay | [`pocketstation-relay`](https://crates.io/crates/pocketstation-relay) | outbound audio | [PocketStation Relay](https://github.com/pocketstation-io/relay) over WebRTC | `0.1.1` | [Guide](relay/README.md) · [Rust API](https://docs.rs/pocketstation-relay) |

That is the complete first-party registry today. A connector not listed here
does not inherit PocketStation maintenance, compatibility, or evidence claims.

## Choose a connector

Use `pocketstation-relay` when you need to publish independent, named audio
buses from a Rust PocketStation Session to PocketStation Relay.

There is not currently a first-party LiveKit, generic WHIP, OpenAI, Deepgram,
or arbitrary WebRTC connector in this registry. Those services have different
authentication, negotiation, lifecycle, and outcome contracts. Support requires
a dedicated adapter; changing a URL is not sufficient.

For installation and a complete application-plus-microphone example, go
directly to the [PocketStation Relay connector guide](relay/README.md).

## What “first-party” means

A connector in this registry must have all of the following:

- a named maintainer and an active product or ecosystem requirement;
- a finite, typed configuration contract with secret redaction;
- an explicit provider/protocol compatibility boundary;
- canonical PocketStation Connector and Endpoint lifecycle integration;
- bounded preparation, delivery, cancellation, drain/abort, and shutdown;
- stable provider error classification and observable terminal outcomes;
- executable conformance, saturation, rollback, and failure tests;
- real protocol integration evidence;
- an independently installable package and isolated consumer proof;
- an intentional versioning and compatibility policy.

Passing component tests does not automatically establish remote production
readiness, every network topology, every platform, or competitive superiority.
Those claims require separately identified evidence.

## Architecture boundary

```text
PocketStation Core
  Session + Graph + Endpoint lifecycle
              ↓
        Connector contract
              ↓
independently packaged provider adapter
              ↓
      external service or protocol
```

Responsibilities stay deliberately separated:

| Owner | Responsibility |
|---|---|
| [`pocketstation`](https://github.com/pocketstation-io/pocketstation) | provider-neutral graph, bounded routing, lineage, lifecycle, recording, observations, Connector contract |
| This registry | first-party connector packages, compatibility, conformance, packaging, release ownership |
| Provider/service repository | wire protocol, server behavior, authentication authority, remote delivery |

Connectors are outbound Endpoint specializations. Inbound media remains a
PocketStation `Source`; transformations remain `Operator`s. A larger
bidirectional integration may compose all three without introducing another
Session or runtime.

Core never gains a closed provider enum. Adding a connector must not add its
WebRTC, SDK, authentication, or protocol dependencies to Core.

## Registry policy

This is a curated first-party registry, not a collection of every possible
integration.

A proposed connector moves through these stages:

1. **Scope** — identify a real user workflow and the exact protocol boundary.
2. **Ownership** — assign maintainers, security ownership, and compatibility
   responsibilities.
3. **Contract** — declare inputs, capabilities, configuration, credentials,
   limits, readiness, errors, and outcomes.
4. **Implementation** — use the canonical Core lifecycle without duplicating
   graph, queue, or Session authority.
5. **Conformance** — prove rollback, saturation, discontinuity, cancellation,
   drain/abort, failure containment, and exact destruction.
6. **Integration** — exercise the real external service and record the evidence
   boundary honestly.
7. **Distribution** — package, inspect, install, and run from an isolated
   consumer before release.

An example or experimental adapter is not promoted into this registry merely
because it compiles.

## Repository layout

```text
connectors/
├── README.md          this registry and its policies
├── Cargo.toml         shared verification workspace only
└── relay/
    ├── README.md      package setup, behavior, and operational limits
    ├── Cargo.toml     independently released crate
    ├── src/           Relay-specific implementation
    └── tests/         package and portable-semantics conformance
```

Package directories own their user documentation. The repository README owns
only discovery, support status, shared boundaries, and registry policy.

## Versioning and compatibility

Connector packages version independently from this repository and from
PocketStation Core.

- Published crate versions and Git tags are immutable.
- Each package declares the Core versions it supports.
- Provider protocol compatibility is proved by that package, not inferred from
  Core's trait definitions.
- A breaking provider or public Rust API change requires the package's normal
  semantic-versioning process.
- A repository commit is not a release until its package artifact, tag, and
  isolated consumer agree on the same source.

Current compatibility:

| Package | Connector version | PocketStation Core | Evidence boundary |
|---|---:|---:|---|
| `pocketstation-relay` | `0.1.1` | `1.1.1` | component and same-host integration; remote production breadth not implied |

## Develop and verify the registry

Run the complete workspace gate from the repository root:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

Package and real-service verification requirements remain in each connector's
own guide and release process.

## Community connectors

Third-party connectors can implement the same open Core contract without
living in this repository. Their maintainers own distribution, provider
compatibility, security response, and support claims.

If a community connector is later considered for first-party support, it must
pass the registry policy above. Adoption is an ownership commitment—not only a
directory move.
