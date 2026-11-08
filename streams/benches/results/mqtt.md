# MQTT (Mosquitto 2, rumqttc 0.25): throughput results

All numbers come from `benches/throughput` with 10 subjects, QoS 1, and runtime-go's
protobuf test messages (StringValue of 256 bytes, or Int64Value for `batch`),
decoded on every receipt. Five benchmark agents shared one Mac with Docker
Desktop while these ran, so treat the figures as before/after comparisons,
not final numbers. Results are in `target/bench-results.jsonl` under the
labels `mqtt-*`.

```text
cargo bench -p tpp-streams --features mqtt,protobuf,live-tests,tls --bench throughput -- \
    --backend mqtt --profile <plain|tls> --scenario <fanout|batch|durable|group> \
    --publishers 512 --messages N --rate R
```

Profiles:

- `plain` is `docker/compose.mqtt.yaml`, MQTT on 11883 with no auth.
- `tls` is `docker/compose.mqtt.secure.yaml`'s password listener on 18884. It
  uses mutual TLS with the test CA and client certificate (rustls on ring),
  then logs in as `e2e-app` with its password, and the ACLs apply. The ACL
  gives each user only `<user>/#`. The profile therefore publishes under the
  prefix `e2e-app/bench` instead of `bench`, which keeps the broker's
  authorization exactly what the e2e suite tests. I did not widen the ACL.

All four scenarios are supported. `durable` uses persistent sessions with
manual PUBACKs. `group` uses `$share/<group>/...` subscriptions.

## What changed and why

| # | Change | Where | Why |
|---|---|---|---|
| 1 | `max_inflight_messages 1000`, `max_queued_messages 1000000`, `max_queued_bytes 0` | both Mosquitto configs | Mosquitto sends `max_inflight_messages` to MQTT 5 clients as the CONNACK Receive Maximum, and the default is **20**. That limited the publishing connection to 20 unacknowledged QoS 1 publishes, however many callers were publishing. That limit, not the driver, was the 13–19k/s ceiling. The queue limit had a default of 1000 (10000 in the secure config). Beyond it, Mosquitto *silently drops* QoS 1 messages for a slow or absent session, which shows up as `lost` and would break a durable consumer's promise. The limit is now high enough that broker memory runs out first. |
| 2 | `publish_batch` is pipelined | `manager.rs`, `PublishLink::publish_all` | It used to go through `provider::publish_each`, which waited one PUBACK round trip per message. Now the whole batch is queued in order on the publishing connection, and the call awaits all PUBACKs together. Ids, `PartialBatch` per-index failures, and the subject, wildcard, and delay checks all behave as before. `PublishLink::publish` is split into `enqueue` and `settled`, so single publishes and batches share the waiter pairing. |
| 3 | Request channel 256 → 8192 | `mod.rs` | When the channel is full, a publish backs off for 1 ms and retries. A batch of 100 from each of 32 publishers (3200 requests) used to hit that limit all the time. |
| 4 | Durable sessions set Receive Maximum to the prefetch, and size their request channel above it | `dial`, `Session::Durable { prefetch }` | This fixes a latent deadlock and gives prefetch a real meaning. Before, the broker could push up to 65535 unacknowledged messages. rumqttc's event loop drains buffered incoming events before it services the request channel. Acks could therefore fill the 256-slot channel, and then `ack().await` blocked the consumer. The routing task was blocked on the full prefetch channel, so it never polled the loop. Now the broker holds back anything beyond the prefetch in the session queue. A consumer can therefore owe at most `prefetch` acks, and queueing a PUBACK never waits. At-least-once is unchanged, because the ack is still sent after the work. |

Not changed:

- **Go wire format.** The `pack`/`unpack` frame and the topic layout are unchanged.
- **Publish semantics.** A publish still returns only when the PUBACK arrives.
- **Shared code.** No files outside `drivers/mqtt`, the MQTT bench target, and the MQTT compose files were touched.

### Durability modes (secure broker)

`STREAMS_DURABILITY=strict|prod` (default `strict`). The start command
appends the autosave lines to a copy of the config:

- **`strict`**: `autosave_interval 1`. This is what the e2e suite runs against. A crash loses at most about a second of session state.
- **`prod`**: `autosave_interval 1800`, Mosquitto's default. State is written every 30 minutes and on every clean shutdown, so `docker restart` keeps it either way. A hard crash can lose up to 30 minutes of queued messages.

Mosquitto has no per-message fsync, so even `strict` writes a periodic
snapshot. The compose header documents the trade-off.

## Before / after (unpaced, `--rate 0`)

| scenario / profile | before (32 publishers) | after (32 publishers) | after (512 publishers) |
|---|---|---|---|
| fanout / plain | 13.0k/s | 31.4k/s | **109k/s** (p50 3.8 ms, p99 16 ms) |
| batch / plain (batch 100) | 14.4k/s | **230k/s** | — |
| durable / plain | 19.6k/s | 31.0k/s | 84k/s (256 publishers) |
| group / plain (2 per subject) | 17.6k/s | 31.5k/s | — |
| fanout / tls (prod) | 18.7k/s | — | **88k/s** (p50 5.2 ms, p99 14 ms) |
| durable / tls (prod) | — | — | 82k/s |
| group / tls (prod) | — | — | 85k/s |
| batch / tls (prod) | — | — | 116k/s |

With 32 publishers, the single-publish scenarios are limited by the harness:
32 concurrent publishes at about 1 ms per round trip is about 32k/s. Every
publish waits for its PUBACK, as the contract requires. Throughput then
scales with concurrency until Mosquitto's single thread saturates.

## Paced runs (`--messages 5×R`)

Every run below reports `ok: true`: no loss, no duplicates, no errors.

| R | plain | tls (prod) |
|---|---|---|
| 50k | fanout ✓ p50 1.7 / p99 16.7 ms · durable ✓ 1.7 / 14.6 · group ✓ 2.2 / 29.3 · batch ✓ | fanout ✓ 4.0 / 26.0 · durable ✓ 4.6 / 29.5 · group ✓ 3.6 / 29.3 · batch ✓ |
| 75k | — | fanout ✓ 5.0 / 41.2 (74.7k delivered) · durable ✓ 6.8 / 38.7 |
| 100k | fanout ✓ 3.1 / 21.9 (99.7k delivered, 1024 publishers) · durable ✓ 6.1 / 26.9 · batch ✓ | batch ✓ (98.9k delivered) |

Strict durability, tls, durable at 50k: ✓ (p50 5.2 / p99 31.7 ms). With 10
sessions caught up, the one-second snapshot is cheap. It costs more when a
deep backlog is queued.

**20k:** reached in every scenario and profile.
**50k:** reached in every scenario and profile.
**100k:** reached on plain for fanout, durable, and batch, and on tls for
batch. Single-publish tls tops out around 85–90k/s unpaced on this machine,
so I ran it paced only up to 75k.

## Best parameters

- **Publishers.** Use 512–1024 concurrent single publishers to go past about 50k/s. Each one waits a full round trip. 32 publishers are enough for batch.
- **Batch.** A batch of 100 is plenty. One connection keeps up to 1000 messages in flight, which is the broker's Receive Maximum.
- **Prefetch.** 1024, as the harness uses. It now becomes the durable session's Receive Maximum.
- **Broker.** `max_inflight_messages` at 1000 or more. `max_queued_messages` high, or 0, so Mosquitto never drops messages silently.

## What limits it

1. **Mosquitto is single-threaded.** At 100k msg/s fanout it handles 4 packets per message (PUBLISH in, PUBACK out, PUBLISH out, PUBACK in). That is about 400k packets/s on one core, which is where it saturates. TLS adds record encryption on the same core, which is why tls lands about 15–20% lower.
2. **One publishing connection in the driver, with rumqttc writing and flushing each request separately.** This is fine at 100k/s, but it is the next limit on the client side. A pool of publishing connections would spread it out, although the broker thread would still be the ceiling.
3. **Docker Desktop's VM networking.** It adds about 1 ms to every round trip. That raises p50 and is why single publishes need high concurrency.
4. **Shared host noise.** Four other brokers were benchmarked at the same time.

## Tests

- `mqtt_e2e` with `STREAMS_E2E_RESTART=1` on strict durability: 3 runs in a row, 10/10 each time.
- `tests/mqtt.rs`, the in-process rumqttd suite: 15/15.
- Driver unit tests: 6/6.
- `cargo clippy -p tpp-streams --all-targets --features mqtt,protobuf,live-tests,tls -- -D warnings`: clean.
- `rustfmt --edition 2024 --check` on every file I changed: clean.
- `cargo tree -p tpp-streams --all-features -i aws-lc-rs`: no match.
