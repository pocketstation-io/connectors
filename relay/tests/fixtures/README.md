# Connector conformance data

`connector-v1-vectors.json` is the packaged copy of
`protocol/conformance/connector/v1/vectors.json`. That source file defines the
protocol. Connector CI verifies this copy against the recorded
SHA-256 value before running the portable semantics test.

When the protocol corpus changes, update the packaged file and its CI hash in
the same reviewed change.
