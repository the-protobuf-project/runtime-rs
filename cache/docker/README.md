# Local cache backends

[Cache guide](../README.md) · [Live contract tests](../tests/README.md)

Each Compose file supplies an isolated loopback service for examples and
feature-gated integration tests. Run all commands from the repository root.

| File | Service | Host address | Compose project |
| --- | --- | --- | --- |
| `compose.live.yaml` | Redis 7 | `127.0.0.1:16379` | `runtime-cache-live` |
| `compose.dragonfly.live.yaml` | Dragonfly | `127.0.0.1:16380` | `runtime-cache-dragonfly-live` |
| `compose.memcached.live.yaml` | Memcached 1.6 | `127.0.0.1:11212` | `runtime-cache-memcached-live` |

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo run -p tpp-cache --example redis
cargo test -p tpp-cache --features live-tests --test redis_live
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```

For Dragonfly, use its file/project/service and `--test dragonfly_live`; for
Memcached, use its file/project/service and `--test memcached_live`. Keep the
same `-p` and `-f` on shutdown so cleanup targets the service you started.
Project names retain `runtime-cache` for isolation; the Cargo package is
`tpp-cache`.

Compose waits for protocol healthchecks before tests run. There are no declared
persistent volumes; these services are disposable test storage. Dragonfly
configures 16 logical databases and expiration notifications. Memcached uses
the meta protocol required by the driver. The test feature makes a missing
server a failure, so start the relevant service before enabling it.
