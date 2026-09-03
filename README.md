# PocketStation Connectors

Send source-aware audio from a PocketStation `Session` to an external service
without adding provider networking, codecs, or credentials to PocketStation
Core.

This repository contains independently versioned first-party Connector
packages. It currently contains one package:

| Package | Result |
|---|---|
| [`pocketstation-relay`](relay/) | publishes application, microphone, or generated audio as named WebRTC `AudioBus` streams through [PocketStation Relay](https://github.com/pocketstation-io/relay) |

There is no built-in catalog for LiveKit, OpenAI, Deepgram, Twilio, generic
WHIP, or generic WebRTC. Those services require their own authentication,
media negotiation, retry behavior, and failure handling.

## Publish two independent audio buses

You need Rust 1.95 or newer, PocketStation Relay, and a source credential for
the RelaySession you want to publish.

```bash
cargo add pocketstation pocketstation-relay
```

Declare one application stem and one microphone stem, then assign a different
Relay bus name to each route:

```rust,no_run
use pocketstation::connector::ConnectorSecret;
use pocketstation::{RouteSettings, Session, Source};
use pocketstation_relay::{RelayConnector, RelayRouteConfiguration};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let session = Session::new();
let application = session.capture(Source::application("Zoom"))?;
let microphone = session.capture(Source::microphone_default())?;

let relay = RelayConnector::new()?;
let registered = relay.register(&session)?;
let token = ConnectorSecret::new("source-token")?;

let application_bus = registered.declare(
    &session,
    RelayRouteConfiguration::new(
        "https://relay.example.com",
        "relay-session-id",
        token.clone(),
        "application",
    )?
    .connector_configuration()?,
    RouteSettings::realtime_audio(),
)?;

let microphone_bus = registered.declare(
    &session,
    RelayRouteConfiguration::new(
        "https://relay.example.com",
        "relay-session-id",
        token,
        "microphone",
    )?
    .connector_configuration()?,
    RouteSettings::realtime_audio(),
)?;

application.send(application_bus)?;
microphone.send(microphone_bus)?;
# Ok(())
# }
```

The two declarations share one WebRTC publisher because they use the same
Relay URL, RelaySession, source credential, publisher group, ICE settings, and
startup deadline. They remain separate PocketStation stems and separate Relay
AudioBuses.

The [Relay Connector guide](relay/README.md) adds recording, Session start and
shutdown, STUN configuration, the low-latency setting, and publication results.

## Know which component does the work

PocketStation Core handles:

- application and microphone capture;
- source, stream, and stem identity;
- Session compilation and start;
- the queue and delivery policy for each route;
- recording, observations, drain, abort, and joined shutdown.

The Relay Connector handles:

- source credential use and redaction;
- Relay signaling and WebRTC setup;
- ICE and DTLS startup;
- PCM-to-Opus encoding;
- RTP publication to each named AudioBus;
- Relay-specific readiness, errors, and receipts.

PocketStation Relay handles authenticated publisher and receiver attachments,
RTP forwarding, receiver pacing, repair, and live media observations. The
control plane creates RelaySessions and receiver invitations when that
deployment mode is selected.

No component creates a second PocketStation Session or captures the same source
again.

## Configure transport behavior

`RelayRouteConfiguration` validates the Relay origin, RelaySession ID, source
credential, bus name, publisher group, ICE servers, latency preference, and
startup deadline before the Session starts.

The current package accepts up to 16 STUN server entries with up to 8 URLs per
entry. The startup deadline must be between 1 millisecond and 120 seconds. TURN
credentials are not supported by connector version 0.1.

Use the standard latency setting when queued continuity matters more than
discarding older audio. Use `with_low_latency()` for interactive voice, where
fresh audio is preferred when provider delivery falls behind. Inspect route
observations and the Relay publication result instead of assuming that
successful Session start means a receiver played audio.

## Handle credentials and failures

Wrap the source credential in `ConnectorSecret`. Its debug output is redacted,
and owned secret text is overwritten when destroyed. Do not copy credentials
into error messages, logs, metrics, or application-visible observations.

One startup deadline covers DNS, signaling, ICE, and DTLS. A failed setup is
reported before publication begins. During execution, Core records route
delivery while the Connector records Relay-specific publication results.
Missing receipts are not interpreted as success.

Stopping the Session drains or aborts accepted audio according to the selected
shutdown mode, closes the WebRTC publisher, joins its worker, and records the
final result.

## Current qualification

The published Relay Connector has package, conformance, and same-host Relay and
browser integration tests. That evidence does not establish every NAT
topology, WAN/TURN operation, physical device, or production traffic level.

The current package publishes audio only. It does not create RelaySessions,
render browser audio, or serialize every PocketStation frame field over the
network. Bus and route correlation must not be described as complete remote
`FrameLineage` delivery.

## Build a connector for another service

Third-party Connectors do not need to live in this repository. Start with the
[Core Connector guide](https://github.com/pocketstation-io/pocketstation/blob/main/docs/guides/connectors.md).

Use `Connector::from_audio_fn` for one send function or implement
`AudioConnector` for a provider that opens, sends, and closes a connection.
Use the driver API only when a distributable package needs typed configuration,
multiple named inputs, explicit service status, or provider-specific
observations.

The package author remains responsible for provider authentication, supported
formats, network behavior, retries, compatibility, security updates,
distribution, and support.

## Develop this repository

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo package -p pocketstation-relay --locked
```

Read the [package documentation](relay/README.md) and
[release notes](relay/RELEASE_NOTES.md) before upgrading or publishing.
