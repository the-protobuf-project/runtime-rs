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
provider, and `Batch` is one awaited Write per value, in order (OPC UA
Part 4 leaves the order of repeated writes to one node within a single Write
undefined). The payload is
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

## RabbitMQ headers, expiry, retain, delayed nak

Headers go in the AMQP basic-properties `headers` table as long strings, on
single publishes, the channel pool and batches; the body stays runtime-go's
version-1 frame, and a header-less publish sends no table, as Go does. The
management API (`/api/queues/<vhost>/<queue>/get`, `rabbitmqadmin get
messages`) shows them as ordinary properties. Keys with the `x-` prefix and
`CC`/`BCC` (sender-selected routing) are RabbitMQ's and refused with
`InvalidHeader` at `create`, `update` and publish, and skipped on receipt with
non-string values. Stream defaults live with the provider's in-process stream
declarations; a message's headers are in the message and survive a restart in
a durable queue. `Options::expires` is the per-message `expiration` (ms, at
most 2^32-1). RabbitMQ expires a message only at the head of its queue, so one
behind an unexpired message waits, still counted in the queue's depth, until it
reaches the head and is then dropped; a per-queue `x-message-ttl` would change
the semantics, so it is not used. `retain` is refused (a queue has no last
value). `nak_after` is refused (`can_delay_redelivery() == false`): the stock
image lacks the delayed-message plugin, and a dead-letter + TTL retry queue
would add topology runtime-go does not know; the message is requeued at once,
as the error says, rather than stranded.

## Kafka headers, expiry, retain, delayed nak

Headers are Kafka record headers (values as UTF-8 bytes) on single publishes,
whichever of the eight pooled producers sends them, and on every record of a
batch; the payload stays runtime-go's version-1 frame, and a header-less
publish writes no header block, as Go does. `kafka-console-consumer.sh
--property print.headers=true` prints them (`NO_HEADERS` for a Go-shaped
record). Kafka reserves no header names, so only the contract's own checks
apply, at `create`, `update` and publish. On receipt a repeated key keeps its
last value (Kafka's `lastHeader`), a null value reads as empty, and a non-UTF-8
header from another producer is skipped with a warning. Stream defaults ride
in the declaration JSON in the compacted metadata topic (`headers`, absent
when empty), so `get`/`list` return them and they survive a broker restart; a
message's headers live in its record and come back on redelivery and replay.
`Options::expires` is refused (Kafka has no per-message expiry, only topic
retention). `retain` is refused: a compacted topic would keep only the newest
record per key for the whole subject, changing every reader's semantics.
`nak_after` is refused (`can_delay_redelivery() == false`): a partition is
read in order, so a held-back record would hold back the partition, and a
client-side sleep would be a fake.

## NATS headers, expiry, retain, delayed nak

Headers are NATS headers on core and JetStream publishes alike, single and
batch, beside the `Nats-Msg-Id` the JetStream path already sends; the body
stays runtime-go's version-1 frame, and a header-less core publish is still a
plain `PUB`. `nats sub --headers-only` and `nats stream get` show them. The
`Nats-` prefix belongs to the server (`Nats-Msg-Id`, `Nats-TTL`,
`Nats-Expected-*`) and is refused with `InvalidHeader` at `create`, `update`
and publish, and dropped from received headers. Stream defaults live in the
process for core NATS and, for JetStream, in the stream's metadata as one JSON
object under `runtime-go.headers` (absent when empty, so Go-written metadata is
unchanged), which survives a server restart.

`Options::expires` is JetStream's per-message `Nats-TTL` (NATS 2.11+, whole
seconds rounded up, under a second refused). Every stream the provider creates
or updates sets `allow_msg_ttl`; a stream created elsewhere without it refuses
an expiry by name until updated through the provider. The server never turns
`allow_msg_ttl` back off, so an update from a runtime-go that does not set it
is refused by the server. Core NATS refuses an expiry: it holds nothing.
`retain` is refused on both: core NATS keeps nothing, and JetStream's
last-per-subject form (`max_msgs_per_subject: 1`) would discard the log durable
consumers read. `nak_after` is JetStream's `-NAK {"delay":…}`, held back by the
server (`can_delay_redelivery() == true`); core NATS has no deliveries.

## Redis headers, expiry, retain, delayed nak

Durable (Redis Streams): headers are fields of the entry beside `msg`, header
`lang` stored as `h:lang`; `msg` stays runtime-go's version-1 frame, which Go
reads while ignoring the extra fields. `redis-cli XRANGE <key> - +` shows them.
The contract refuses `:` in a key, so `h:<key>` never collides with `msg` or
`meta`; nothing else is reserved. Every delivery reads them back, reclaimed
ones and a plain `subscribe` (XREAD) included. Pub/sub and scheduled: frame
version 2 (`provider::pack_with`), version 1 when there are no headers, so a
header-less message is byte-for-byte Go's. Stream defaults are stored in the
stream's metadata JSON (`headers`, absent when empty) and checked at `create`
and `update`.

`Options::expires` and `retain` are refused in every mode: pub/sub holds
nothing to expire, a scheduled message fires on its own timer, Redis has no
per-entry expiry in a stream, and no mode keeps a last value. `nak_after` is
real on the durable provider: the entry stays pending and is XCLAIMed back to
its own consumer with `IDLE reclaim_after - delay` and `JUSTID` (no delivery
counted), so the group's reclaim (`min-idle = reclaim_after`) reaches it once
the delay has passed, never sooner, at most one reclaim pass later; the server
holds the timing, so it survives a restart. Redis cannot set an idle time in the
future, so the longest delay is `RedisConfig::reclaim_after` and a longer one is
refused by name; with reclaiming off, `can_delay_redelivery()` is false. The
secure ACL grants the `streams` user `+xclaim` for this.

## ZeroMQ headers, expiry, retain, delayed nak

Headers ride in the payload frame as version 2 (`provider::pack_with`); a
header-less message is still the version-1 frame Go reads, and a Go subscriber
refuses a version-2 one by name. Stream defaults live in each side's in-process
declaration and are checked at `create` and `update`. Expiry and retain are
refused (no queue, nothing kept); there are no deliveries, so no delayed nak.

## MQTT headers, expiry, retain, delayed nak

Headers are MQTT 5 user properties on the PUBLISH (key order), on single
publishes and on every message of a batch; the payload stays runtime-go's
version-1 frame, and a header-less publish sends no properties, as Go does.
`mosquitto_sub -V mqttv5 -F '%P %t'` prints them (`lang:rust site:plant-3
<topic>`). MQTT reserves no user property names, so only the contract's checks
apply, at `create`, `update` and publish; a key another publisher repeats keeps
its last value. Subscriptions and durable consumers read them back, and the
broker stores them with queued messages, so they survive a redelivery and a
broker restart. `Options::expires` is the Message Expiry Interval in whole
seconds, rounded up (a sub-second expiry is 1 s, never 0); Mosquitto drops a
message that waited longer in an offline session's queue or as a retained
value, while one handed to a connected subscriber at once is delivered.
`retain` is the retain flag: a later subscriber is handed the framed message
(a durable consumer once, when its subscription is new, not on every resume),
and `MqttProvider::clear_retained` clears it with an empty retained publish,
which subscriptions skip. `nak_after` is refused (`can_delay_redelivery() ==
false`): MQTT has no negative acknowledgement, so the message stays outstanding
until the session reconnects.

OPC UA PubSub inherits this: headers are user properties on the
NetworkMessage's PUBLISH beside its `application/json` content type, the Part
14 JSON unchanged, and every field message a subscriber takes from it carries
them (another publisher's user properties included); the stream's headers are
also stamped on its retained `ua-metadata`. Expiry is the Message Expiry
Interval on the data message. `retain` is refused: Part 14 retains metadata,
not data, and subscribers skip retained data. PubSub now publishes and
subscribes through the MQTT driver's own publishing pool and subscribe helper.

OPC UA client/server refuses headers (`Stream::headers` at `create`/`update`,
publish and batch headers), expiry and retain with `Unsupported`: a Write sets
a value, status and timestamps and has nowhere to put the rest. It has no
deliveries, so no delayed nak.
