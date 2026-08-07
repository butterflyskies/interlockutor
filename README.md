# Interlockutor

Claim-once work queue and broadcast event bus with interlocking safety guarantees.

## Status

Under construction. See [tasks#131](https://github.com/butterflyskies/tasks/issues/131).

## Architecture decisions

Public API surface in this crate is governed in part by an ADR that lives in
a sibling repository: [ADR-0040](https://github.com/butterflyskies/memory-mcp/blob/main/docs/adr/0040-exhaustive-public-enums.md)
(`butterflyskies/memory-mcp`) requires public enums to be exhaustive by
default and specifies the version-transition and migration guidance a new
variant must carry. See `PayloadError` and `Error` in
`crates/interlockutor/src/lib.rs` for the current application of this rule.

## License

Apache-2.0
