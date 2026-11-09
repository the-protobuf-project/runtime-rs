# tpp-streams

Backend-agnostic publish/subscribe streams over Kafka, NATS, RabbitMQ, Redis,
MQTT, ZeroMQ, and OPC UA. A Rust port of [`runtime-go/streams`](https://github.com/the-protobuf-project/runtime-go/tree/main/streams),
built around async traits, explicit capabilities, and caller-owned clients.

## Install

```bash
cargo add tpp-streams --features redis   # or kafka, nats, rabbitmq, mqtt, zeromq, opcua
```

The library is imported as `streams`. Every backend is a Cargo feature, so an
application builds only the clients it talks to; `kafka` compiles librdkafka
from C. `protobuf` adds a prost codec beside the default JSON one.

## Quick start

Start Redis with `docker compose -f streams/docker/compose.redis.yaml up -d --wait`.

```rust
use futures::StreamExt;
use streams::{Options, Stream, Streams, typed,
    drivers::redis::{RedisConfig, RedisProvider, redis}};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Order { user: String, total: u32 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:16380")?;
    let provider = RedisProvider::new(client, RedisConfig {
        prefix: "demo".into(), ..RedisConfig::default()
    });
    let stream = provider.create(
        Stream::named("orders")
            .subjects(["order.placed"])
            .header("lang", "rust"),
    ).await?;

    let orders = typed::<Order>(provider.bind(&stream.id).await?);
    let mut placed = orders.subscribe("order.placed", &Options::default()).await?;
    orders.publish("order.placed", &Order { user: "ada".into(), total: 42 }, &Options::default()).await?;
    println!("{:?}", placed.next().await);
    Ok(())
}
```

## How it fits together

1. **Connect** a client you own and hand it to a provider. Providers never
   close what you gave them.
2. **Create** a stream with the subjects it accepts. Publishing or subscribing
   to anything else fails with `StreamError::UnknownSubject`.
3. **Bind** the stream to get a `Manager`, which publishes and subscribes. A
   subscription is a `futures::Stream`; dropping it ends the subscription.
4. **Reach for a capability** with `as_durable`, `as_positioned`, or
   `as_batch`. A backend that lacks one says so with `StreamError::Unsupported`
   rather than silently giving weaker delivery.

| Capability | Kafka | NATS | RabbitMQ | Redis | MQTT | ZeroMQ | OPC UA |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Publish / Subscribe | Yes | Yes | Yes | Yes | Yes | Yes | Write / monitored item |
| Durable (ack / nak) | Yes | JetStream | Yes | Streams | Persistent session | No | No |
| Positioned replay | Yes | JetStream | No | Streams | No | No | No |
| Batch | Native | Pipelined (JetStream) | Pipelined | Pipelined | Pipelined | Each | One Write |
| Headers | Record headers | NATS headers | AMQP headers | Stream fields; frame v2 on pub/sub | MQTT 5 user properties | Frame v2 | PubSub only (user properties) |
| Expiry | No | JetStream (whole seconds) | Yes (at the head of the queue) | No | Yes (whole seconds) | No | PubSub only |
| Retain (last value) | No | No | No | No | Yes | No | No |
| Delayed nak | No | JetStream | No | Streams (up to the reclaim interval) | No | No | No |

## From one provider to every transport

```mermaid
sankey-beta

Stream provider,Kafka,5
Stream provider,NATS core,3
Stream provider,NATS JetStream,7
Stream provider,RabbitMQ,5
Stream provider,Redis pub/sub,3
Stream provider,Redis Streams,6
Stream provider,MQTT,6
Stream provider,ZeroMQ,3
Stream provider,OPC UA client/server,2
Stream provider,OPC UA PubSub,4
Kafka,Publish and subscribe,1
Kafka,Durable consumers,1
Kafka,Replay,1
Kafka,Batch,1
Kafka,Headers,1
NATS core,Publish and subscribe,1
NATS core,Batch,1
NATS core,Headers,1
NATS JetStream,Publish and subscribe,1
NATS JetStream,Durable consumers,1
NATS JetStream,Replay,1
NATS JetStream,Batch,1
NATS JetStream,Headers,1
NATS JetStream,Expiry,1
NATS JetStream,Delayed redelivery,1
RabbitMQ,Publish and subscribe,1
RabbitMQ,Durable consumers,1
RabbitMQ,Batch,1
RabbitMQ,Headers,1
RabbitMQ,Expiry,1
Redis pub/sub,Publish and subscribe,1
Redis pub/sub,Batch,1
Redis pub/sub,Headers,1
Redis Streams,Publish and subscribe,1
Redis Streams,Durable consumers,1
Redis Streams,Replay,1
Redis Streams,Batch,1
Redis Streams,Headers,1
Redis Streams,Delayed redelivery,1
MQTT,Publish and subscribe,1
MQTT,Durable consumers,1
MQTT,Batch,1
MQTT,Headers,1
MQTT,Expiry,1
MQTT,Retained last value,1
ZeroMQ,Publish and subscribe,1
ZeroMQ,Batch,1
ZeroMQ,Headers,1
OPC UA client/server,Publish and subscribe,1
OPC UA client/server,Batch,1
OPC UA PubSub,Publish and subscribe,1
OPC UA PubSub,Batch,1
OPC UA PubSub,Headers,1
OPC UA PubSub,Expiry,1
```

## Test coverage

```mermaid
sankey-beta

Tests,Unit and doc tests,141
Tests,Kafka,32
Tests,NATS,44
Tests,RabbitMQ,29
Tests,Redis,65
Tests,MQTT,37
Tests,ZeroMQ,21
Tests,OPC UA client/server,28
Tests,OPC UA PubSub,23
Unit and doc tests,No broker,141
Kafka,Secure with restart,19
Kafka,Plaintext broker,13
NATS,Secure with restart,21
NATS,Plaintext broker,23
RabbitMQ,Secure with restart,8
RabbitMQ,Plaintext broker,21
Redis,Secure with restart,19
Redis,Plaintext broker,46
MQTT,Secure with restart,14
MQTT,In-process broker,23
ZeroMQ,Real sockets,21
OPC UA client/server,In-process server,11
OPC UA client/server,OPC PLC simulator,9
OPC UA client/server,Secure with restart,8
OPC UA PubSub,In-process broker,15
OPC UA PubSub,Secure with restart,5
OPC UA PubSub,Microsoft OPC Publisher,3
```

Payloads are framed byte-for-byte as runtime-go frames them, and every broker
driver keeps Go's topic, key, and metadata layout, so Go and Rust services can
share one broker. OPC UA is the exception, and Rust only: its values travel as
native typed OPC UA `DataValue`s, so PLCs, SCADA, and other OPC UA tools can
read them.

See [source and middleware](src/README.md), [drivers](src/drivers/README.md),
[examples](examples/README.md), [Docker](docker/README.md), [tests](tests/README.md), and
[benchmarks](benches/README.md).
