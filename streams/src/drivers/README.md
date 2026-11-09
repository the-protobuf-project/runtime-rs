# Stream drivers

[Streams guide](../../README.md) · [Local services](../../docker/README.md)

Each driver is a Cargo feature of its name and ports the matching package in
runtime-go. The client crate for each is the most-starred maintained Rust
client for that broker:

| Feature | Client crate | Go client it replaces | Entry point |
| --- | --- | --- | --- |
| `kafka` | [`rdkafka`](https://github.com/fede1024/rust-rdkafka) | franz-go | `KafkaProvider::connect(ClientConfig, KafkaConfig)` |
| `nats` | [`async-nats`](https://github.com/nats-io/nats.rs) (official) | nats.go | `NatsProvider::new` (core), `JetStreamProvider::new` (JetStream) |
| `rabbitmq` | [`lapin`](https://github.com/amqp-rs/lapin) | amqp091-go | `RabbitProvider::new(Arc<Connection>, RabbitConfig)` |
| `redis` | [`redis`](https://github.com/redis-rs/redis-rs) (official) | go-redis | `RedisProvider::new` (pub/sub), `::scheduled` (TTL), `::durable` (Streams) |
| `mqtt` | [`rumqttc`](https://github.com/bytebeamio/rumqtt) | paho.golang | `MqttProvider::connect(MqttOptions, MqttConfig)` |
| `zeromq` | [`zeromq`](https://github.com/zeromq/zmq.rs) (official, pure Rust) | zmq4 | `ZmqProvider::publisher` / `::subscriber(ZmqConfig)` |
| `opcua` | [`async-opcua`](https://github.com/FreeOpcUa/async-opcua) (pure Rust, RustCrypto) | none: Rust only | `OpcUaProvider::new(Arc<Session>, OpcUaConfig)` |

## Ownership

Drivers take the client you built and never close it. Two take something
other than a client, for a reason:

- **Kafka** takes a `ClientConfig`, because every named consumer needs a client
  of its own and the provider builds them from your template.
- **MQTT** takes `MqttOptions` as a template for the same reason: a durable
  consumer is a persistent session, which is a connection with its own client
  id. The provider drives each connection's event loop in a task it owns.

## Where drivers differ from Go

Wire formats, topic and key layouts, and metadata match Go everywhere. These
behaviours differ on purpose:

- A `group` on a plain `subscribe` joins a group where the backend has one
  (Kafka consumer group, NATS queue group, MQTT `$share/`) and is refused with
  `Unsupported` where it does not (RabbitMQ, Redis, ZeroMQ). Go silently
  ignored it; the contract refuses options that change what a call means.
- Redis batches are pipelined into one round trip; Go published one at a time.
- ZeroMQ compares the whole topic frame, so a subscription to `user` no longer
  receives `users`. The `zeromq` crate has no `inproc` transport.
- Kafka pins each partition's start before `subscribe` returns, so a publish
  right after cannot be missed.
- Self-dialing constructors and codec options are not ported: clients are
  yours, and a `Payload` carries its own codec.

## OPC UA

OPC UA is how PLCs, SCADA, and building controllers expose their state, so
this driver speaks to them on their terms rather than carrying runtime-go's
frame. It is client/server: a stream's subjects are node ids
(`ns=3;s=Temperature`, or the stable `nsu=<uri>;s=Temperature`), `publish` is
a Write to the node's Value that returns after the server's Write response,
`subscribe` is a data-change monitored item in one client subscription per
provider, and `Batch` is one Write request. The payload is
`OpcUaValue` (`"opcua.datavalue"`): a `DataValue` in OPC UA binary encoding,
so the value stays typed and its status code and source and server
timestamps reach the subscriber. Groups, TTL, `Durable`, and `Positioned`
are refused: OPC UA has no consumer groups, no scheduled writes, and no
reader acknowledgement, and HistoryRead is a historian query, not a replay
position. OPC UA has no message id, so `publish` returns a generated one that
does not travel.

`async-opcua` is the maintained successor of locka99/opcua; no Rust OPC UA
crate has wide adoption, and this one is pure Rust on RustCrypto, so it adds
no C build and no aws-lc. Its `rsa` 0.9 dependency carries RUSTSEC-2023-0071
(Marvin: non-constant-time RSA private-key arithmetic, with no fixed release).
Every RSA security policy, Basic256Sha256 and Aes256Sha256RsaPss included,
does private-key operations (OAEP decryption and signing) when it opens or
renews a secure channel, so a secured channel inherits the advisory. Both
policies pad with OAEP, not PKCS#1 v1.5, which removes the classic padding
oracle but not the timing leak; avoid Basic128Rsa15, and keep OPC UA ports
off untrusted networks, as OPC UA deployments already do.

Two async-opcua 0.19 behaviours the driver works around, both in the module
docs: after a reconnect the recreated subscription is never published to
until something triggers a publish, so a watchdog nudges a subscription that
has been silent past twice its keep-alive; and a Publish response from the
OPC Foundation's .NET stack holding more than about 1,800 notifications drops
the connection as `BadDecodingError`, so `OpcUaConfig::max_notifications_per_publish`
defaults to 1000.

### PubSub over MQTT

`opcua::pubsub` is OPC UA Part 14 over an MQTT broker, JSON mapping only (no
UADP): `PubSubProvider::connect(MqttOptions, PubSubConfig)` dials with the MQTT
driver's handshake. A stream is a WriterGroup (`<group>` on this PublisherId,
or `<publisher>/<group>` to read another's), a subject is a DataSetWriter
(`<writer>`, payload a `DataSet`) or one of its fields (`<writer>/<field>`,
payload `OpcUaValue`). `publish` is one `ua-data` NetworkMessage on
`opcua/json/data/<PublisherId>/<WriterGroup>/<DataSetWriter>` at QoS 1, after
the PUBACK; `Batch` puts several DataSetMessages in one NetworkMessage;
`subscribe` delivers each field as an `OpcUaValue` message, so subscribers
decode exactly as on client/server. Sequence numbers are tracked per writer and
gaps logged and counted. `publish_metadata` retains `ua-metadata` on
`opcua/json/metadata/...`. Groups are MQTT shared subscriptions; `Durable` is
refused (one MQTT ack would settle a whole NetworkMessage of fields), as are
`Positioned`, TTL, and publishing under another PublisherId. Verified against
Microsoft OPC Publisher (`docker/compose.opcua.pubsub.yaml`).
