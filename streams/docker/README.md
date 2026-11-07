# Local brokers

[Streams guide](../README.md) · [Tests](../tests/README.md)

One compose file per broker, each on a host port that does not collide with a
default install. Run from the workspace root:

```bash
docker compose -f streams/docker/compose.redis.yaml up -d --wait
docker compose -f streams/docker/compose.redis.yaml down
```

| File | Image | Host port |
| --- | --- | --- |
| `compose.kafka.yaml` | apache/kafka (KRaft, single node) | 19092 |
| `compose.kafka.secure.yaml` | apache/kafka, mTLS (PEM), log on a named volume | 19093 |
| `compose.nats.yaml` | nats with JetStream (`-js`) | 14222 (monitor 14822) |
| `compose.nats.secure.yaml` | nats, mTLS (`compose.nats.secure.conf`), JetStream file store on a named volume | 14223 (monitor 14823) |
| `compose.rabbitmq.yaml` | rabbitmq:management-alpine | 15682 (UI 15683) |
| `compose.rabbitmq.secure.yaml` | rabbitmq:management-alpine, AMQPS-only mTLS (`compose.rabbitmq.secure.conf`), data dir on a named volume | 15671 (UI not published) |
| `compose.redis.yaml` | redis:7-alpine, keyspace expiry events on | 16380 |
| `compose.redis.secure.yaml` | redis:7-alpine, TLS-only mTLS (`--port 0`), AOF `appendfsync always` on a named volume, expiry events on | 16381 |
| `compose.mqtt.yaml` | eclipse-mosquitto (`compose.mqtt.mosquitto.conf`) | 11883 |
| `compose.mqtt.secure.yaml` | eclipse-mosquitto, MQTTS-only mTLS (`compose.mqtt.secure.mosquitto.conf`), sessions persisted on a named volume | 18883 |

ZeroMQ is brokerless and needs nothing here. The Redis file enables keyspace
expiry events because the scheduled (TTL) provider is built on them.
