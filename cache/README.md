# runtime-cache

Backend-independent cache strategies over Redis, Dragonfly, and Memcached. A
Rust port of [`runtime-go/cache`](https://github.com/the-protobuf-project/runtime-go/tree/main/cache),
built around async traits, explicit capabilities, and caller-owned clients.

## Install

```bash
cargo add runtime-cache
```

The library is imported as `runtime_cache`.

## How it fits together

Applications follow three explicit steps:

1. connect a Redis, Dragonfly, or Memcached client;
2. bind shared cache policy to its Provider;
3. select a named or numeric database and use one of its strategies.

The four strategies have deliberately different costs:

- **Document** stores ID-addressed values and supports enumeration when the
  backend has server-side sets.
- **Volatile** stores caller-keyed values without maintaining an index.
- **Indexed** adds secondary field/value memberships to Document.
- **Aside** loads through an authoritative source, collapses concurrent misses,
  remembers absence, and can serve stale values while refreshing.

Typed JSON views and provider-independent retry, tracing, and OpenTelemetry
middleware wrap Document without changing backend strategy code. The complete,
compile-checked quick start and policy guide live in the crate documentation:

```bash
cargo doc --open -p runtime-cache
```

## Backend capabilities

Redis and Dragonfly provide direct operations, sets, lease reporting, scans,
bulk reads, and fenced Aside claims. Memcached provides direct operations and
bulk reads; enumeration, secondary lookup, remaining-TTL reporting, scans, and
cross-process fencing return `CacheError::Unsupported` where required.

## Runnable Redis example

Start the repository's isolated Redis service, run the public-API walkthrough,
then remove only that Compose project's resources:

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo run -p runtime-cache --example redis
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```

The example defaults to `127.0.0.1:16379`. Set
`RUNTIME_CACHE_REDIS_ADDRESS` to use another Redis endpoint.

## Live contract tests

The repository includes separate Compose services and feature-gated public API
tests for each backend:

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo test -p runtime-cache --features live-tests --test redis_live

docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
cargo test -p runtime-cache --features live-tests --test dragonfly_live

docker compose -p runtime-cache-memcached-live -f cache/docker/compose.memcached.live.yaml up -d --wait memcached
cargo test -p runtime-cache --features live-tests --test memcached_live
```

## License

Apache-2.0
