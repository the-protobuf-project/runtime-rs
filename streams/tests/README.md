# Tests

[Streams guide](../README.md) · [Local brokers](../docker/README.md)

Unit tests live beside the code: the wire frame (including a byte-level check
against Go's layout), subject wildcards, codecs, retry, and each driver's
broker-free logic.

| File | Needs | Run with |
| --- | --- | --- |
| `zeromq.rs` | nothing: real sockets on `127.0.0.1` | `cargo test -p tpp-streams --features zeromq` |
| `mqtt.rs` | nothing: an in-process `rumqttd` broker | `cargo test -p tpp-streams --features mqtt` |
| `mqtt_e2e.rs` | mTLS Mosquitto on 18883 (`compose.mqtt.secure.yaml`); restart test needs `STREAMS_E2E_RESTART=1` | `--features mqtt,tls,live-tests` |
| `redis_live.rs` | Redis on 16380 (`REDIS_ADDR`) | `--features redis,live-tests` |
| `redis_e2e.rs` | mTLS Redis on 16381 (`compose.redis.secure.yaml`); restart test needs `STREAMS_E2E_RESTART=1` | `--features redis,tls,live-tests` |
| `nats_live.rs` | NATS with JetStream on 14222 | `--features nats,live-tests` |
| `nats_e2e.rs` | mTLS NATS on 14223 (`compose.nats.secure.yaml`); restart test needs `STREAMS_E2E_RESTART=1` | `--features nats,tls,live-tests` |
| `rabbitmq_live.rs` | RabbitMQ on 15682 | `--features rabbitmq,live-tests` |
| `rabbitmq_e2e.rs` | mTLS RabbitMQ on 15671 (`compose.rabbitmq.secure.yaml`); restart test needs `STREAMS_E2E_RESTART=1` | `--features rabbitmq,tls,live-tests` |
| `kafka_live.rs` | Kafka on 19092 | `--features kafka,live-tests` |
| `kafka_e2e.rs` | mTLS Kafka on 19093 (`compose.kafka.secure.yaml`); restart test needs `STREAMS_E2E_RESTART=1` | `--features kafka,tls,live-tests` |

Live tests are compiled only with `live-tests`, so a plain `cargo test` never
reaches for a broker. Start the one you need from [docker/](../docker/README.md)
first. Each live suite ports the matching Go suite and adds checks that
dropping a subscription releases its server-side state.
