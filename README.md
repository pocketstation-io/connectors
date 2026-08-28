# PocketStation connectors

Use a Connector to send audio from a PocketStation `Session` to an external
service. Connectors are separate packages, so their network and provider
dependencies do not become Core dependencies.

## Available connector

PocketStation Relay is the only first-party connector currently available.

| Package | Sends | Destination |
|---|---|---|
| [`pocketstation-relay`](https://crates.io/crates/pocketstation-relay) | independent named audio buses | [PocketStation Relay](https://github.com/pocketstation-io/relay) over WebRTC |

Install the Rust packages:

```bash
cargo add pocketstation pocketstation-relay
```

Then follow the [Relay connector guide](relay/README.md) to publish application
and microphone audio as separate buses.

See the [release notes](relay/RELEASE_NOTES.md) before upgrading.

## What the Relay connector handles

The package owns the Relay-specific work:

- source capability authentication;
- WebRTC signaling, ICE, DTLS, Opus, and RTP;
- named AudioBus publication;
- finite startup and shutdown deadlines;
- redacted credentials and structured failures.

PocketStation Core continues to own capture, graph compilation, bounded
routing, recording, and Session lifecycle.

## Current limits

There are no first-party LiveKit, OpenAI, Deepgram, Twilio, generic WHIP, or
generic WebRTC connectors in this repository. Each service requires its own
authentication, media negotiation, lifecycle, and error handling; changing a
URL is not enough.

The Relay connector's published evidence covers component and same-host
integration tests. It does not claim every NAT topology, platform, or production
load.

## Build another connector

Third-party packages can implement PocketStation's open Connector API without
living in this repository. Start with the
[Core Connector guide](https://github.com/pocketstation-io/pocketstation/blob/main/docs/guides/connectors.md).
The package author owns provider compatibility, security updates, distribution,
and support.
