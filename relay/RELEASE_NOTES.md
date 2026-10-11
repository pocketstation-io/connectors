# PocketStation Relay connector release notes

## 0.1.6 — Verified proxy TLS and independent audio buses

Application and microphone AudioBuses can publish Opus through authenticated
WSS and explicitly configured HTTP or HTTPS CONNECT proxies. HTTPS verifies
the proxy certificate and hostname; WSS separately verifies the Relay service.
Proxy credentials and the source capability are sent to their respective
services. Connection deadlines remain finite and failure never falls back direct.

Grouped publication retains source generations, per-bus loss observations,
owner renewal and revocation/expiry cancellation through the existing
publisher control. Registry publication binds the selected immutable tag in
both validation and publication jobs, including an explicit recovery event.

### Authorized AudioBus input

The package can subscribe to one capability-scoped Relay `AudioBus`, wait for
the first real Opus packet, decode the qualified 48 kHz mono / 20 ms profile,
and expose it through Core's external `Source` lifecycle. Source generation,
RTP discontinuity, delivery loss counts, startup failure, runtime failure,
and deterministic close remain observable.

This work requires PocketStation Core 1.1.10 for the explicit Opus codec
feature. WAN/TURN connectivity, browser decode, physical capture and model
output require separate product evidence.

## 0.1.5 — Clear package description

The package description now states that the Connector publishes PocketStation
AudioBuses to remote WebRTC receivers. Runtime behavior is unchanged.

```console
cargo update -p pocketstation-relay
```

## 0.1.4 — Accurate route depth observations

This release uses PocketStation Core 1.1.7, which prevents concurrent route
metrics from reporting a queue depth above its configured capacity. Relay
publication, named AudioBuses, output cancellation, and shutdown behavior are
unchanged.

```console
cargo update -p pocketstation -p pocketstation-relay
```

## 0.1.3 — Clear route settings

Relay destinations now use the route settings and execution safety names from
PocketStation 1.1.6. Audio delivery, named bus publication, cancellation, and
shutdown behavior are unchanged.

```console
cargo update -p pocketstation -p pocketstation-relay
```

## 0.1.2 — Stop cancelled output before Relay sends it

When a person interrupts generated speech, stopping the provider task is not
enough. Encoded audio may still be waiting in the Connector queue and can reach
the receiver after the application has moved to a new response.

PocketStation Relay 0.1.2 carries Core's output generation identity through PCM
packetization and Opus encoding. If the application cancels that output, the
Connector discards its queued frames before RTP publication while other
AudioBuses continue.

This release requires `pocketstation 1.1.3`, which introduced output generation
ownership for application-provided audio.

Cancellation cannot recall RTP packets already sent to Relay or audio already
buffered by a receiver. Complete interruption handling must also clear receiver
playout when that receiver provides the capability.

### Upgrade

This update requires no Connector configuration migration.

```console
cargo update -p pocketstation -p pocketstation-relay
```
