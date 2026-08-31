# Connector conformance data

`connector-v1-vectors.json` is the packaged copy of
`protocol/conformance/connector/v1/vectors.json`. The source file remains the
protocol authority. Connector CI verifies this copy against the canonical
SHA-256 value before running the portable semantics test.

When the protocol corpus changes, update the packaged file and its CI hash in
the same reviewed change.
