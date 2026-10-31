# Shared model and Flight SQL fixture

[Database tests](../README.md)

`mod.rs` supplies the telemetry-point model and Resource descriptors used by
adapter tests. `flight.rs` starts an in-process Arrow Flight service, so the
real SDK query transport can be tested against deterministic batches/errors.

Tests include this folder with `mod common;`, bind a controlled peer, select a
provider, execute typed reads, and assert both returned records and the request
that reached the peer. The fixture is test infrastructure, not an alternative
production database implementation.

```bash
cargo test -p tpp-database --test core --test query --test write
```

Keep descriptor/row schemas consistent and return real Arrow/Flight framing.
Protocol failures should be configured by the consuming test; do not replace
the SDK path with a direct fake result when validating transport behavior.
