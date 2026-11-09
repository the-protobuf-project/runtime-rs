# Tests

[Streams guide](../README.md) · [Local brokers](../docker/README.md)

Unit tests live beside the code: the wire frame (including a byte-level check
against Go's layout), subject wildcards, codecs, retry, and each driver's
broker-free logic.

| File | Needs | Run with |
| --- | --- | --- |
| `zeromq.rs` | nothing: real sockets on `127.0.0.1` | `cargo test -p tpp-streams --features zeromq` |
| `mqtt.rs` | nothing: an in-process `rumqttd` broker | `cargo test -p tpp-streams --features mqtt` |
| `mqtt_e2e.rs` | Mosquitto from `compose.mqtt.secure.yaml`, mTLS on both ports: 18883 (cert CN is the username) and 18884 (username/password plus ACLs, users `e2e-app` and `e2e-other`); runs `mosquitto_sub` in that container (`docker exec`) to show headers are user properties; restart test needs `STREAMS_E2E_RESTART=1` | `--features mqtt,tls,live-tests` |
| `opcua.rs` | nothing: an in-process async-opcua server, restarted in place for the reconnect test | `cargo test -p tpp-streams --features opcua` |
| `opcua_live.rs` | opc-plc from `compose.opcua.yaml` on 14840 (SecurityPolicy None, anonymous); restart test needs `STREAMS_E2E_RESTART=1` | `--features opcua,live-tests` |
| `opcua_e2e.rs` | opc-plc from `compose.opcua.secure.yaml` on 14841 (Basic256Sha256 SignAndEncrypt, username/password, trusted client certificate, pinned server certificate; certificates from `compose.opcua.secure.sh`); proves None, wrong credentials, anonymous, and an untrusted certificate are refused; restart test needs `STREAMS_E2E_RESTART=1` | `--features opcua,tls,live-tests` |
| `opcua_pubsub.rs` | nothing: an in-process `rumqttd` broker, plus a raw MQTT client that reads the wire and plays another Part 14 publisher | `cargo test -p tpp-streams --features opcua` |
| `opcua_pubsub_live.rs` | OPC Publisher, opc-plc and Mosquitto from `compose.opcua.pubsub.yaml` on 11885: reads a third-party publisher's NetworkMessages and retained metadata | `--features opcua,live-tests` |
| `opcua_pubsub_e2e.rs` | Mosquitto from `compose.mqtt.secure.yaml` on 18884 (mTLS plus `e2e-app`'s password, under its ACLs, so the Part 14 prefix is `e2e-app/<run>/opcua`); proves the ACLs and a wrong password refuse | `--features opcua,tls,live-tests` |
| `redis_live.rs` | Redis on 16380 (`REDIS_ADDR`) | `--features redis,live-tests` |
| `redis_e2e.rs` | mTLS + ACL Redis on 16381 (`compose.redis.secure.yaml`), providers as user `streams`; restart test needs `STREAMS_E2E_RESTART=1` | `--features redis,tls,live-tests` |
| `nats_live.rs` | NATS with JetStream on 14222 | `--features nats,live-tests` |
| `nats_e2e.rs` | mTLS NATS on 14223 (password and NKey users) and 14224 (certificate-mapped user), both from `compose.nats.secure.yaml`; restart test needs `STREAMS_E2E_RESTART=1` | `--features nats,tls,live-tests` |
| `rabbitmq_live.rs` | RabbitMQ on 15682 (container `tpp-streams-rabbitmq`; the header tests read it with `docker exec` through the management API and `rabbitmqctl`) | `--features rabbitmq,live-tests` |
| `rabbitmq_e2e.rs` | mTLS RabbitMQ on 15671 (`compose.rabbitmq.secure.yaml`), logging in with SASL EXTERNAL as the client cert's CN; restart test needs `STREAMS_E2E_RESTART=1` | `--features rabbitmq,tls,live-tests` |
| `kafka_live.rs` | Kafka on 19092 | `--features kafka,live-tests` |
| `kafka_e2e.rs` | Kafka with ACLs (`compose.kafka.secure.yaml`): mTLS on 19093, SASL/SCRAM-SHA-512 over TLS on 19094; restart test needs `STREAMS_E2E_RESTART=1` | `--features kafka,tls,live-tests` |

Live tests are compiled only with `live-tests`, so a plain `cargo test` never
reaches for a broker. Start the one you need from [docker/](../docker/README.md)
first. Each live suite ports the matching Go suite and adds checks that
dropping a subscription releases its server-side state.
