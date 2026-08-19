# PocketStation connectors

Connect a PocketStation Session to an external service without teaching Core
about providers, protocols, credentials, or network clients.

This repository contains independently packaged implementations of
PocketStation's open `Connector` contract. The first package is
[`pocketstation-relay`](relay): a bounded WebRTC publisher for
[PocketStation Relay](https://github.com/pocketstation-io/relay).

```text
source-aware PocketStation stems
  → Core Connector / Endpoint lifecycle
      → provider adapter in this repository
          → external service
```

## Start publishing

```bash
cargo add pocketstation pocketstation-relay
```

```rust,no_run
use pocketstation::connector::ConnectorSecret;
use pocketstation::{ApplicationSelector, EdgeContract, Session, Source};
use pocketstation_relay::{RelayConnector, RelayRouteConfiguration};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let session = Session::builder().recording_root("recordings").build();
let application = session.capture(Source::application(
    ApplicationSelector::name("PocketStation Demo"),
))?;

let relay = RelayConnector::new()?;
let registered = relay.register(&session)?;
let endpoint = registered.declare(
    &session,
    RelayRouteConfiguration::new(
        "https://relay.example.com",
        "relay-session-id",
        ConnectorSecret::new("source-token")?,
        "application",
    )?
    .connector_configuration()?,
    EdgeContract::realtime_audio(),
)?;

application.send(endpoint)?;
application.record("application")?;

let mut running = session.start()?;
let stop = running.stop();
assert!(stop.is_success());
# Ok(())
# }
```

The connector does not create a hidden Relay server. The Relay origin, Session
identity, and source credential come from infrastructure you operate or a
control plane you explicitly call.

## One Session model, clear ownership

| Layer | Responsibility |
|---|---|
| PocketStation Core | graph compilation, bounded routes, lineage, lifecycle, recording, observations |
| Connector package | provider configuration, protocol client, readiness, provider errors, transport outcomes |
| Provider service | network protocol, authentication authority, remote delivery |

Core remains provider-neutral. It does not contain WebRTC, LiveKit, WHIP,
OpenAI, Deepgram, or PocketStation Relay behavior. A different provider is a
different connector package using the same lifecycle—not a new Core enum or a
special Session mode.

## What the Relay connector guarantees

`pocketstation-relay` turns one or more PocketStation audio routes into one
grouped Relay publisher:

- independent application and microphone stems remain independent named
  `AudioBus` publications;
- routes with the same Relay origin, Session, credential, publisher group, ICE
  configuration, and deadline share one transactional publisher lifecycle;
- input is bounded 48 kHz interleaved `f32` PCM;
- encoding and network work remain outside realtime capture callbacks;
- one finite deadline covers DNS, signaling, ICE, and DTLS startup;
- source credentials use Core's redacting `ConnectorSecret`;
- preparation, rollback, start gating, drain/abort, join, and finalization use
  the canonical Core Endpoint lifecycle;
- provider failures retain stable codes, stages, and retryability;
- per-route receipts preserve source, stem, route, and bus correlation.

## Publish application and microphone together

Declare both endpoints from the same registered connector and give their route
configurations the same publisher group. The connector groups them into one
publisher while retaining two bus identities:

```text
application stem ──→ application bus ──┐
                                       ├─ one authenticated PeerConnection
microphone stem  ──→ microphone bus  ──┘
```

If any grouped route cannot prepare, Core rolls back the group before the start
gate opens. Shutdown can drain accepted work or abort it explicitly through the
existing Endpoint contract.

See the [crate guide](relay/README.md) for the complete two-stem example and
configuration API.

## Configuration model

The public Relay configuration is typed:

```rust,no_run
# use std::time::Duration;
# use pocketstation::connector::ConnectorSecret;
# use pocketstation_relay::{RelayIceServer, RelayRouteConfiguration};
# fn config() -> Result<(), Box<dyn std::error::Error>> {
let route = RelayRouteConfiguration::new(
    "https://relay.example.com",
    "relay-session-id",
    ConnectorSecret::new("source-token")?,
    "microphone",
)?
.with_publisher_group("demo-publisher")?
.with_ice_servers([
    RelayIceServer::new(["stun:stun.example.com:3478"])?
])?
.with_startup_timeout(Duration::from_secs(20))?
.with_low_latency();
# let _ = route;
# Ok(())
# }
```

Validation happens before Session startup. Secret values are never exposed by
`Debug`. Capacities and deadlines are finite. Invalid URLs, identities, bus
labels, ICE configuration, or timeout values fail as typed configuration
errors.

## What this repository is—and is not

This is a small set of maintained connector packages, not a speculative
provider catalog.

A package belongs here only when it has:

- a real product or ecosystem need;
- an explicit maintainer and compatibility policy;
- executable Connector conformance tests;
- real protocol integration evidence;
- bounded failure and shutdown behavior;
- independent packaging and consumer verification.

The current Relay connector works with PocketStation Relay. It is not a
universal SFU client and cannot be pointed at LiveKit or an arbitrary WHIP
endpoint. Those protocols require their own adapters.

The current package also supports STUN discovery but does not allocate TURN
credentials. Networks that require TURN need a later connector revision or a
different adapter.

## Repository map

```text
relay/
  Cargo.toml
  README.md
  src/
    configuration.rs   typed, secret-aware provider configuration
    connector.rs       Core Connector registration and manifest
    runtime.rs         grouped lifecycle and terminal receipts
    frame_publisher.rs bounded route-to-bus publication
    audio/             PCM packetization, clocking and Opus workers
    rtc/               signaling, ICE/DTLS and WebRTC publication
  tests/
    portable_semantics.rs
```

Related repositories:

- [pocketstation](https://github.com/pocketstation-io/pocketstation) — the
  provider-neutral runtime and Connector contract;
- [relay](https://github.com/pocketstation-io/relay) — the Go Relay service and
  public signaling contract;
- `pocketstation-lab` — cross-repository product proof;
- `pocketstation-bench` — neutral transport measurement.

## Verify the package

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo package -p pocketstation-relay --locked
```

## Compatibility and evidence

The current crate release is `pocketstation-relay 0.1.1` and consumes
PocketStation Core `1.1.1`. Pre-1.0 connector releases may evolve, but published
versions and tags are immutable.

Current evidence proves the component and same-host integration paths. It does
not by itself establish remote production readiness, every NAT topology,
cross-platform distribution, or performance superiority over another
transport. Those claims require explicit Lab and Bench artifacts.

The design goal is simple: provider integrations should feel native to a
PocketStation Session while remaining independently owned, testable, and
replaceable.
