# Cache usage walkthroughs

[Cache guide](../README.md) · [Backend setup](../docker/README.md)

`redis.rs` demonstrates the public API from client connection through provider
selection, typed document storage, indexing/Aside, and orderly cleanup.
`dragonfly.rs` exercises the same higher-level contracts through Dragonfly's
shared RESP driver. Both connect to real services.

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo run -p tpp-cache --example redis
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```

```bash
docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
cargo run -p tpp-cache --example dragonfly
docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml down
```

Defaults are Redis at `127.0.0.1:16379` and Dragonfly at `127.0.0.1:16380`.
Use `RUNTIME_CACHE_REDIS_ADDRESS` or `RUNTIME_CACHE_DRAGONFLY_ADDRESS` to change
them. Copy the connection/provider/selected-DB pattern for application startup;
close DBs before the caller-owned client. For a server-free illustration, see
[core construction](../src/core/README.md).

These examples write cache data. Prefer the disposable Compose services over
a shared cache when exploring namespace deletion or index cleanup.
