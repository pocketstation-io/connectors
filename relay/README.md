# PocketStation Relay connector

`pocketstation-relay` publishes named, source-aware PocketStation
audio stems to PocketStation Relay through the public
`pocketstation::connector` contract.

The package owns client-side Opus, RTP, WebRTC, and PocketStation Relay
signaling. PocketStation Core remains provider-neutral. A LiveKit, WHIP, or
other transport integration is a separate connector package using the same
Core contract; this package is not a universal relay client.

## Two independent buses, one publisher lifecycle

```rust,no_run
use pocketstation::connector::ConnectorSecret;
use pocketstation::{ApplicationSelector, Session, Source};
use pocketstation_relay::{RelayConnector, RelayRouteConfiguration};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let session = Session::builder().recording_root("recordings").build();
let application = session.capture(Source::application(
    ApplicationSelector::name("PocketStation Demo"),
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
    pocketstation::EdgeContract::realtime_audio(),
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
    pocketstation::EdgeContract::realtime_audio(),
)?;

application.send(application_endpoint)?;
microphone.send(microphone_endpoint)?;
application.record("application")?;
microphone.record("microphone")?;

let mut running = session.start()?;
// The application controls its own bounded run loop or shutdown signal.
let stop = running.stop();
assert!(stop.is_success());
# Ok(())
# }
```

Routes with the same RelaySession identity, source token, publisher group,
ICE configuration, and startup deadline are prepared and joined as one
transactional publisher. Each route keeps its own PocketStation source, stem,
route, and bus identity.

## Operational contract

- Input is bounded 48 kHz interleaved `f32` PCM; publication is Opus over RTP.
- One finite startup deadline covers signaling, ICE, and DTLS establishment.
- Source credentials use Core's redacting `ConnectorSecret` value.
- Provider failures carry stable connector codes, stages, and retryability.
- Core owns prepare rollback, the start gate, stop, join, generic delivery
  observations, and finalization.
- The current transport supports finite STUN configuration. It does not
  allocate TURN relays; networks that require TURN are unsupported in 0.1.

Current evidence is component and same-host only. It does not establish remote,
production, cross-platform or non-PocketStation-relay compatibility.
