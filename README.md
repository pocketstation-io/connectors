# PocketStation connectors

Independently packaged connectors for PocketStation's open `Connector`
contract.

This repository begins with the PocketStation Relay connector. It is not a
general provider catalog: new packages require an accepted product need,
explicit ownership, connector conformance, protocol integration evidence, and
a maintained compatibility policy.

Core connector contracts live in
[`pocketstation`](https://github.com/pocketstation-io/pocketstation). Relay
services and signaling live in
[`relay`](https://github.com/pocketstation-io/relay). This repository owns the
native client package adapting those two boundaries without placing WebRTC or
provider dependencies in Core.

