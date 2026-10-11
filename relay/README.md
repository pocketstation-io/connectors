# PocketStation Relay connector

Publish independent PocketStation audio stems to PocketStation Relay, or turn
one authorized remote `AudioBus` into a Core external `Source`.

`pocketstation-relay` owns the client-side Opus, RTP, WebRTC, WebSocket ingress, and Relay
signaling implementation. PocketStation Core continues to own graph
compilation, route delivery, lineage, recording, and transactional lifecycle.

```text
application stem ──→ application AudioBus ──┐
                                            ├─ grouped Relay publisher
microphone stem  ──→ microphone AudioBus  ──┘

authorized remote AudioBus ──→ Opus decode ──→ Core Source
                                             ├─ fixed-capacity destination
                                             └─ multistem recording
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
use pocketstation::{ApplicationSelector, RouteSettings, Session, Source};
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
    RouteSettings::realtime_audio(),
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
    RouteSettings::realtime_audio(),
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

## Configure the Relay connection

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

To publish through an HTTP or HTTPS CONNECT proxy, select the WebSocket ingress in this
development checkout:

```rust,no_run
# use pocketstation::connector::ConnectorSecret;
# use pocketstation_relay::RelayRouteConfiguration;
# fn configure() -> Result<(), Box<dyn std::error::Error>> {
let configuration = RelayRouteConfiguration::new(
    "https://relay.example.com", "relay-session-id",
    ConnectorSecret::new("source-capability")?, "application",
)?
.with_websocket_ingress()?
.with_http_proxy(ConnectorSecret::new("http://proxy.example.com:8080")?)?;
# let _ = configuration;
# Ok(())
# }
```

Apply the same transport and proxy to both grouped bus configurations. Each bus
uses an authenticated `pks-opus-v1` connection to the existing Relay. Publisher
audio travels over WSS without publisher UDP; remote receivers still use the
existing Relay media plane. This API requires pocketstation-relay 0.1.6 or
later and a Relay service that enables Opus WebSocket ingress. SDK distribution
support is verified separately.

Omit `with_http_proxy` to honor `https_proxy`/`HTTPS_PROXY`, then
`all_proxy`/`ALL_PROXY`. `no_proxy`/`NO_PROXY` supports exact hosts, domain suffixes,
optional ports, IPv6 literals and `*`; CIDR ranges are not supported. Lowercase
variables take precedence. An explicit proxy overrides those exclusions.
HTTP and HTTPS CONNECT proxy origins are accepted; SOCKS is unsupported.
HTTPS verifies the proxy certificate and hostname before sending CONNECT.
Proxy Basic credentials are sent only to that proxy; the source capability is
sent only inside the separate Relay connection. WSS independently verifies
the Relay certificate and hostname. Both TLS connections use the normal
platform trust store. Failure never falls back to a direct connection.

All validation happens before media publication:

- the Relay URL must be an HTTP(S) origin accepted by the transport;
- Session, bus, and publisher-group identities are finite and non-empty;
- credentials remain inside `ConnectorSecret` and are redacted from `Debug`;
- ICE server and URL counts must remain within their configured maxima;
- the startup deadline is between 1 ms and 120 seconds.

## Runtime behavior

| Concern | Behavior |
|---|---|
| Input | 48 kHz interleaved `f32` PCM through a fixed-capacity route queue |
| Encoding | Opus outside the realtime capture callback |
| Publication | named WebRTC streams mapped to Relay `AudioBus` identities |
| Startup | one finite deadline across DNS, signaling, ICE, and DTLS |
| Grouping | compatible routes share one transactional publisher |
| Backlog | finite, latency-profile-specific freshness policy |
| Shutdown | Core-owned drain or abort, followed by joined finalization |
| Errors | stable connector code, provider stage, diagnostic, retryability |
| Outcomes | per-route receipt correlated to source, stem, route, and bus |

Core controls the Connector lifecycle:

```text
Session prepare
  → Connector validates and prepares provider state
  → Core opens the start gate atomically
  → connector workers publish accepted route input
  → Core requests drain or abort
  → workers join
  → Core records terminal outcomes
```

The crate does not create a second graph, Session, queue policy, or retry
engine. Provider retry/reconnect behavior must remain finite and explicit.

WebSocket ingress preserves the existing encoder and fixed-capacity queues.
It sends mono/stereo 20 ms Opus packets (960 samples at 48 kHz, at most 1275
payload bytes). CONNECT, TLS, Upgrade and READY share one absolute startup
deadline across the group. Inbound messages, frames and write buffers are
limited to 4096 bytes. Encoded frames older than 200 ms are dropped and counted;
cancelled output is checked before network delivery. Each write queues once and
flushes without blocking the socket, within an absolute 200 ms deadline. The
default connector terminates the group on failure; select renewable publication
below to recover transport losses. Recovery always reports a discontinuity.

Packet sequence and the 48 kHz sample clock preserve publisher packet gaps and
RTP wrap. The header also carries local capture time and output generation;
these are metadata, not remote clock synchronization or audible cancellation.
Core-local source/stream/discontinuity lineage is not fully serialized, and
browser RTP does not carry that output generation. The selected bus and READY
generation establish Relay attachment identity.

## Keep a WebSocket publisher running through renewal

In this development checkout, pass `RelayPublisherControl` to
`RelayConnector::with_publisher_control`. The caller's existing management
worker supplies the initial capability, then replaces it after each successful
RelaySession renewal. The connector does not issue or renew credentials.

```rust,no_run
use std::time::Instant;
use pocketstation::connector::ConnectorSecret;
use pocketstation_relay::{RelayConnector, RelayPublisherControl, RelayRouteConfiguration};

# fn example(configuration: &RelayRouteConfiguration, initial_valid_until: Instant,
# renewed_capability: ConnectorSecret, renewed_valid_until: Instant)
# -> Result<(), Box<dyn std::error::Error>> {
let control = RelayPublisherControl::new(configuration, initial_valid_until)?;
let relay = RelayConnector::with_publisher_control(control.clone())?;
// Register relay and declare both bus publications on the existing Session.
// After an actual successful RelaySession renewal:
control.replace(2, renewed_capability, renewed_valid_until)?;
// On management failure or permanent owner shutdown:
control.revoke();
# let _ = relay;
# Ok(())
# }
```

The configuration must select WebSocket ingress. All grouped buses must use the
same Relay origin, Session and publisher group. One control handle admits one
active publisher; the caller may clone it for its management worker. Initial
revision defaults to 1; `new_with_revision` accepts an already renewed nonzero
revision. Replacement revision and expiry must both increase. Expiry is a
caller-validated monotonic `Instant` in the next 24 hours. Revocation is permanent.
A later owner must create a new control handle.

On replacement or transport loss, publication pauses for the entire group and
all old sockets close. The connector admits every selected bus with the freshest
capability before resuming media. Every observed READY generation, including a
partially rolled back admission, must increase on its next attachment. A partial
admission closes its admitted sockets and sends no media on that partial group.
Authorization denial, policy close, invalid protocol, wrong bus identity,
non-increasing generation, credential expiry and revocation terminate publication.

Transport recovery has at most three attempts, with 100, 200 and 400 ms waits,
inside a total five-second deadline. Each group admission has at most one second
and also respects the configured startup timeout and fresh credential expiry.
All group closes share at most 200 ms. Stop prevents another attempt and joins
an in-progress admission within these limits. Default WebRTC and static
publication retain their existing configuration and fail-on-loss behavior.

During recovery, feeders count and discard consumed Core PCM frames. Encoded
packets older than the per-bus source END time observed at paired admission are
also discarded. This time observes frames already popped by a feeder; it does
not prove that every frame still queued in Core was captured after READY.
The existing 200 ms encoded-age limit remains. Recovery can lose audio and
partial PCM; packet sequence and sample clocks expose gaps. The connector does
not promise uninterrupted playback, device-clock fidelity or remote delivery.

## Receive one authorized AudioBus as a Core Source

The subscriber capability must be scoped to the exact RelaySession and bus.
Obtain `source_generation` from that RelaySession's control-plane state; do
not infer it locally or reuse it for a later publisher attachment.

```rust,no_run
use std::time::Duration;
use pocketstation::Session;
use pocketstation_relay::{
    RelayAudioBusSourceConfig, RelayAudioBusSources, RelaySubscriberToken,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let relay_session_id = "relay-session-id";
let subscriber_token = RelaySubscriberToken::new("bus-scoped-subscriber-token");
let source_generation = 1; // Read from RelaySession state.

let session = Session::builder().recording_root("recordings").build();
let sources = RelayAudioBusSources::install(&session)?;
let mut config = RelayAudioBusSourceConfig::new(
    "https://relay.example.com",
    relay_session_id,
    "phone-microphone",
    subscriber_token,
    source_generation,
)?;
config.startup_timeout = Duration::from_secs(10);
config.no_frame_timeout = Duration::from_secs(6);

let remote = sources.declare(&session, config)?;
remote.output().send(session.polled_audio()?)?;
remote.output().record("remote-phone-microphone")?;

let mut running = session.start()?; // Ready only after the first Opus frame.
# let _ = running.wait_audio(Duration::from_secs(1))?;
let stop = running.stop();
assert!(stop.is_success());
# Ok(())
# }
```

The current receive profile is deliberately concrete: 48 kHz mono Opus in
20 ms packets, decoded to 960-sample `f32` PCM frames. Any other decoded frame
size fails explicitly. A subscriber source maps the first RTP timestamp onto
Core's local process-monotonic clock; it does not claim the publisher's device
clock or transport complete Core `FrameLineage` over the network.

Startup has one finite deadline and does not report ready before a media frame
arrives. Runtime delivery timeout means no media packet arrived; it is not a
silence or audibility detector. RTP non-contiguity advances the Core
discontinuity epoch. Pool saturation drops the newest decoded packet, reports
the loss, and advances the following discontinuity epoch.

Publisher replacement is not hidden recovery. Close the failed Core Session,
read the new Relay `source_generation`, and create a new Session. The adapter
never silently swaps a bus, credential, source, or generation.

## Inspect publication results

`RelayConnector` retains receipts up to its configured capacity. A receipt key identifies
the endpoint/route publication, and the final result reports stable outcome
state and unit-bearing statistics. Missing receipts are not interpreted as
success.

Use Core's Session and route observations for route delivery. Use the connector
receipt for Relay-specific publication results.

WebSocket receipts distinguish `ingress_frames_sent_total`,
`ingress_payload_bytes_sent_total` and `ingress_source_generation` from RTP
statistics. They retain successful sends even when publication later fails.
Socket sends do not acknowledge delivery at Relay or a receiver; inspect those
boundaries separately.


Renewable receipts add `ingress_attachments_total`, `ingress_renewals_total`,
`ingress_reconnects_total`, `ingress_reconnect_attempts_total`,
`ingress_discontinuities_total` and `ingress_credential_revision` per bus.
`ingress_recovery_input_frames_dropped_total` counts consumed PCM frames;
`ingress_recovery_opus_packets_dropped_total` counts encoded Opus packets;
`normalization_partial_samples_discarded_total` counts interleaved PCM samples.
These quantities have different units and are not added to the compatibility
`dropped_frames_total` observation. Sent packets count successful socket flushes,
without a receiver acknowledgment. Final receipts preserve prior successful
publication when a later attachment fails.

## Current boundaries

- This crate targets PocketStation Relay only. LiveKit, generic WHIP, and other
  transports require separate connectors.
- The current ICE client configuration supports finite STUN authorities. It
  does not allocate TURN credentials.
- The package publishes audio and can decode one authorized bus into Core PCM;
  it does not implement receiver playback, control-plane Session creation, or
  browser/mobile UI.
- Complete PocketStation per-frame lineage is not yet serialized across the
  remote protocol. The subscriber creates Core-local lineage from the exact
  bus, declared attachment generation, RTP continuity, and local observation
  clock. This must not be overstated as original publisher `FrameLineage`.
- Subscriber reconnect and source replacement require a new explicit Core
  Session. There is no implicit continuity across an attachment generation.
- The qualified receive profile is 48 kHz mono with 20 ms Opus packets.
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
Core conformance, Relay protocol compatibility, secret redaction, startup and
shutdown deadlines, and isolated package-consumer proof.
