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
| `compose.nats.yaml` | nats with JetStream (`-js`) | 14222 (monitor 14822) |
| `compose.rabbitmq.yaml` | rabbitmq:management-alpine | 15682 (UI 15683) |
| `compose.redis.yaml` | redis:7-alpine, keyspace expiry events on | 16380 |
| `compose.mqtt.yaml` | eclipse-mosquitto (`compose.mqtt.mosquitto.conf`) | 11883 |

ZeroMQ is brokerless and needs nothing here. The Redis file enables keyspace
expiry events because the scheduled (TTL) provider is built on them.
