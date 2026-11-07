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
