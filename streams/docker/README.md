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
| `compose.kafka.secure.yaml` | apache/kafka, mTLS (PEM) and SASL_SSL SCRAM-SHA-512 (user and ACL setup in `compose.kafka.secure.sh`, test-only credentials in `compose.kafka.secure.env`), StandardAuthorizer ACLs, log on a named volume | 19093 (mTLS), 19094 (SASL_SSL) |
| `compose.nats.yaml` | nats with JetStream (`-js`) | 14222 (monitor 14822) |
| `compose.nats.secure.yaml` | nats, mTLS plus test-only password and NKey users (`compose.nats.secure.conf`, seed in `compose.nats.secure.nkey`) limited to `e2e.>`, JetStream file store on a named volume; and `nats-certmap`, mTLS with `verify_and_map` (`compose.nats.certmap.conf`) | 14223 (monitor 14823), certmap 14224 |
| `compose.rabbitmq.yaml` | rabbitmq:management-alpine | 15682 (UI 15683) |
| `compose.rabbitmq.secure.yaml` | rabbitmq:management-alpine, AMQPS-only mTLS (`compose.rabbitmq.secure.conf`), SASL EXTERNAL only with the cert CN `tpp-streams-e2e` as the sole user and no guest (`.definitions.json`, `.enabled_plugins`), data dir on a named volume; `STREAMS_DURABILITY=strict\|prod` is accepted, and both are RabbitMQ's default (confirm after the fsync) | 15671 (UI not published) |
| `compose.redis.yaml` | redis:7-alpine, keyspace expiry events on | 16380 |
| `compose.redis.secure.yaml` | redis:7-alpine, TLS-only mTLS (`--port 0`) plus ACL users (`compose.redis.secure.acl`: `default` off, `streams` limited to the driver's commands, keys and channels, `admin` for test inspection), AOF `appendfsync always` on a named volume, expiry events on | 16381 |
| `compose.mqtt.yaml` | eclipse-mosquitto (`compose.mqtt.mosquitto.conf`) | 11883 |
| `compose.opcua.yaml` | mcr.microsoft.com/iotedge/opc-plc (Microsoft's OPC PLC simulator), SecurityPolicy None, anonymous, writable typed nodes from `compose.opcua.nodes.json` | 14840 |
| `compose.opcua.secure.yaml` | opc-plc, Basic256Sha256 / Aes256Sha256RsaPss only (no None), username/password user `e2e-operator` (no anonymous), client application certificate trusted via `--tf` (no auto-accept), server certificate provisioned so the client pins it; run `compose.opcua.secure.sh` first to issue the certificates into `tls/out/opcua/` | 14841 |
| `compose.mqtt.secure.yaml` | eclipse-mosquitto, MQTTS-only mTLS (`compose.mqtt.secure.mosquitto.conf`): cert CN as username on 18883; username/password from a `mosquitto_passwd` file written at start, under ACLs (`compose.mqtt.secure.acl`), on 18884; sessions persisted on a named volume | 18883, 18884 |

ZeroMQ is brokerless and needs nothing here. The Redis file enables keyspace
expiry events because the scheduled (TTL) provider is built on them.
