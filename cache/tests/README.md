# Cache observability and live contracts

[Cache guide](../README.md) · [Compose setup](../docker/README.md)

The ordinary suite runs in-memory/unit assertions and shared telemetry checks:

```bash
cargo test -p tpp-cache
```

`observability.rs` verifies exported outcomes and that shared telemetry does
not expose cache keys or bodies. Backend/strategy unit tests also live next
to their implementation under `src`.

The three `*_live.rs` targets are enabled explicitly with `live-tests`; once
enabled they fail if their backend is unavailable. They verify public strategy
semantics, native/named selection, supported/refused capabilities, and resource
ownership using real backend commands.

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo test -p tpp-cache --features live-tests --test redis_live
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```

Use the Dragonfly/Memcached Compose setups for `--test dragonfly_live` and
`--test memcached_live`. Defaults and address overrides are described in
[examples](../examples/README.md) and the respective test setup functions.
Live tests create their own namespaces and exercise cleanup; use disposable
services. A default-suite pass alone does not prove a real backend is reachable.
