# PocketStation Relay connector

Publish independent PocketStation audio stems to PocketStation Relay through
the `pocketstation::connector` lifecycle.

`pocketstation-relay` owns the client-side Opus, RTP, WebRTC, and Relay
signaling implementation. PocketStation Core continues to own graph
compilation, bounded delivery, lineage, recording, and transactional lifecycle.

```text
application stem ──→ application AudioBus ──┐
                                            ├─ grouped Relay publisher
microphone stem  ──→ microphone AudioBus  ──┘
```

## Install

```bash
cargo add pocketstation pocketstation-relay
```

You also need a reachable
[PocketStation Relay](https://github.com/pocketstation-io/relay) service and a
valid RelaySession source credential. The crate never starts hidden
infrastructure.

Before running the example, create a `RelaySession` in the control plane (or a
standalone Relay), keep its source capability, and confirm that the Relay URL
is reachable. The example uses placeholder values; it does not contact a
PocketStation-operated service by default.

## Declare two bus publications

```rust,no_run
use pocketstation::connector::ConnectorSecret;
use pocketstation::{ApplicationSelector, EdgeContract, Session, Source};
use pocketstation_relay::{RelayConnector, RelayRouteConfiguration};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let application_name = std::env::args()
    .nth(1)
    .ok_or("usage: publish_to_relay <application name or identifier>")?;
let session = Session::builder().recording_root("recordings").build();
let application = session.capture(Source::application(
    ApplicationSelector::name(application_name),
))?;
let microphone = session.capture(Source::microphone_default())?;

let relay = RelayConnector::new()?;
let registered = relay.register(&session)?;
let source_token = ConnectorSecret::new("control-plane-source-token")?;

let application_endpoint = registered.declare(
    &session,
    RelayRouteConfiguration::new(
        "https://relay.example.com",
        "relay-session-id",
        source_token.clone(),
        "application",
    )?
    .connector_configuration()?,
    EdgeContract::realtime_audio(),
)?;

let microphone_endpoint = registered.declare(
    &session,
    RelayRouteConfiguration::new(
        "https://relay.example.com",
        "relay-session-id",
        source_token,
        "microphone",
    )?
    .connector_configuration()?,
    EdgeContract::realtime_audio(),
)?;

application.send(application_endpoint)?;
microphone.send(microphone_endpoint)?;
application.record("application")?;
microphone.record("microphone")?;

let mut running = session.start()?;
let stop = running.stop();
assert!(stop.is_success());
# Ok(())
# }
```

This program demonstrates configuration, grouped preparation, Session start,
and joined shutdown. Replace the placeholder URL, Session ID, and source
capability before running it. A receiver-visible publication also requires the
application and microphone to produce media while the Session remains running;
the snippet stops immediately so it can stay focused on declaration.

The two route configurations share a Relay origin, Session, credential,
publisher group, ICE configuration, and startup deadline. They therefore
prepare and run as one publisher while retaining distinct source, stem, route,
and bus identities.

## Configure the network path

```rust,no_run
# use std::time::Duration;
# use pocketstation::connector::ConnectorSecret;
# use pocketstation_relay::{RelayIceServer, RelayRouteConfiguration};
# fn config() -> Result<(), Box<dyn std::error::Error>> {
let configuration = RelayRouteConfiguration::new(
    "https://relay.example.com",
    "relay-session-id",
    ConnectorSecret::new("source-token")?,
    "application",
)?
.with_publisher_group("desktop-demo")?
.with_ice_servers([
    RelayIceServer::new(["stun:stun.example.com:3478"])?
])?
.with_startup_timeout(Duration::from_secs(20))?
.with_low_latency();
# let _ = configuration;
# Ok(())
# }
```

All validation happens before media publication:

- the Relay URL must be an HTTP(S) origin accepted by the transport;
- Session, bus, and publisher-group identities are finite and non-empty;
- credentials remain inside `ConnectorSecret` and are redacted from `Debug`;
- ICE server and URL counts are bounded;
- the startup deadline is between 1 ms and 120 seconds.

## Runtime contract

| Concern | Behavior |
|---|---|
| Input | bounded 48 kHz interleaved `f32` PCM |
| Encoding | Opus outside the realtime capture callback |
| Publication | named WebRTC streams mapped to Relay `AudioBus` identities |
| Startup | one finite deadline across DNS, signaling, ICE, and DTLS |
| Grouping | compatible routes share one transactional publisher |
| Backlog | finite, latency-profile-specific freshness policy |
| Shutdown | Core-owned drain or abort, followed by joined finalization |
| Errors | stable connector code, provider stage, diagnostic, retryability |
| Outcomes | per-route receipt correlated to source, stem, route, and bus |

Core remains the lifecycle authority:

```text
Session prepare
  → Connector validates and prepares provider state
  → Core opens the start gate atomically
  → connector workers publish bounded route input
  → Core requests drain or abort
  → workers join
  → Core records terminal outcomes
```

The crate does not create a second graph, Session, queue policy, or retry
engine. Provider retry/reconnect behavior must remain finite and explicit.

## Inspect publication results

`RelayConnector` retains bounded publication receipts. A receipt key identifies
the endpoint/route publication, and the final result reports stable outcome
state and unit-bearing statistics. Missing receipts are not interpreted as
success.

Use Core's Session and route observations for route delivery. Use the connector
receipt for Relay-specific publication results.

## Current boundaries

- This crate targets PocketStation Relay only. LiveKit, generic WHIP, and other
  transports require separate connectors.
- The current ICE client configuration supports finite STUN authorities. It
  does not allocate TURN credentials.
- The package publishes audio; it does not implement receiver playback,
  control-plane Session creation, or browser UI.
- Complete PocketStation per-frame lineage is not yet serialized across the
  remote protocol. Named bus and route correlation must not be overstated as
  full `FrameLineage` delivery.
- Current published evidence is component and same-host evidence, not a claim
  of every NAT topology, cross-platform readiness, or production scale.

## Verify

From the repository root:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo package -p pocketstation-relay --locked
```

Published crate versions and tags are immutable. Connector changes must retain
Core conformance, Relay protocol compatibility, secret redaction, bounded
startup/shutdown behavior, and isolated package-consumer proof.
