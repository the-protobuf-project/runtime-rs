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

## Round 5: against the official tool

The baseline is EMQX's open-source load tool, `emqx/emqtt-bench` 0.6.3, in a
container reaching the plaintext Mosquitto (`docker/compose.mqtt.yaml`) at
`host.docker.internal:11883`. It therefore crosses Docker Desktop's port
forward just as the Rust driver does. The runs use MQTT 5 and 256-byte
payloads. `streams/benches/native-baseline.sh mqtt` runs it, capped at 500k
messages.

**Steady state.** emqtt-bench prints each counter's rate once a second. The
script drops the first sample (connecting and ramping up) and the last (a
partial second), then reports the median of the rest. A QoS 0 run that ends in
under three seconds has no steady state, so its best second is reported as
"peak". The publish rate is `pub_succ` (PUBACKed), and delivery is `recv`.

**What the tool can and cannot do.** At `-I 0` (unpaced), emqtt-bench keeps
**one** QoS 1 publish outstanding per connection. Its `-F` in-flight option
made no difference between 1 and 1000, both through the forward and inside
Docker's network. Its QoS 1 rate is therefore set by its connection count,
each connection waiting for its PUBACK. That is the shape of the harness's
single-publish scenarios, where each publisher awaits one PUBACK. The tool
cannot pipeline the way `publish_batch` does. Its QoS 0 runs are the only
pipelined figure, and they serve as the broker's raw ceiling.

Measured back to back with the driver runs below (round 2 of this session):

| baseline (emqtt-bench, through the forward) | publish | delivery |
|---|---|---|
| (a) QoS 1, 1 publisher → 1 subscriber, 1 topic | 2.5k/s | 2.5k/s |
| (b) QoS 1, 1000 publishers over 10 topics, 1 subscriber each | 45.6k/s | 45.1k/s |
| (b) same, 512 / 2000 / 4000 publishers (other runs) | 20.7k / 39.7k / 50.2k | 20.9k / 41.0k / 50.5k |
| (c) QoS 0, 1 → 1 (raw ceiling) | 200k/s | 105k/s |
| (c) QoS 0, 10 publishers over 10 topics (raw ceiling) | 893k/s peak | 238k/s peak (366k in a quieter run) |

(a) is pure round-trip latency: one message per round trip, about 0.4 ms.
The tool's QoS 1 fanout plateaus at roughly 40–50k/s. At that point it uses
about 2.7 cores, while Mosquitto runs at about half a core. The limit is the
Erlang client, not the broker.

Driver, unpaced, plaintext, 300k messages. Each pair was measured in the same
minutes as the baseline. `before` is HEAD's driver, and `after` is this
round's. Run 1 had heavy load from other agents and run 2 was calmer:

| scenario | round 4 | before (run 1 / run 2) | after (run 1 / run 2) | after ÷ tool's (b) delivery |
|---|---|---|---|---|
| fanout, 512 publishers | 109k | 81–122k / 89–91k | 86–146k / 89–92k | **~2.0×** |
| batch, 32 × 100 | 230k | 184–222k / 208–230k | 250–260k / 244–249k | **~5.4×** (≈ 100% of the QoS 0 10-publisher peak) |
| durable, 512 publishers, 1 consumer | 84k | 82–144k / 86–90k | 128–145k / 86–99k | **~2.0×** |
| group, 512 publishers, 2 per subject | — | 84–139k / 83k | 143–147k / 82–146k | **~1.8–3.2×** |

Every run reports `ok`: nothing lost, duplicated or undecodable. Absolute
figures drifted by 2× within the hour (one 500k fanout ran at 168k/s, and
another at 82k/s minutes later). Only the same-minute comparisons count.

### What changed

1. **A pool of publishing connections** (`MqttConfig::publish_connections`,
   `DEFAULT_PUBLISH_CONNECTIONS`). This round measured it at 4; the default was
   then set to 1, because with more than one connection publishes sent
   concurrently without awaiting can reach the broker out of order, and
   Mosquitto's single thread is the limit anyway. Raise it for batch speed. Publishes take the connections
   in turn. Each connection keeps its own waiter table, event-loop task, and
   the broker's Receive Maximum (1000), so a batch can have 4000 in flight
   instead of 1000. A batch stays on one connection, in order. A publish still
   returns only after its PUBACK, and nothing on the wire changed. With
   `client_id` set, extra connections append `-1`, `-2`, and so on.
   `MQTT_PUBLISH_CONNECTIONS` overrides the count in the bench target. Batch
   gained 10–35%. Single publishes at 512 publishers are within noise when the
   machine is calm. They are limited by round trips (512 in flight ÷ about
   5 ms) and by Mosquitto's thread, not by one connection.
2. **Receive path uses `provider::unpack_shared`**. A subscription to an
   exact subject shares one `Arc<str>` subject with every message, where
   before it allocated a `String` and then an `Arc` per message. Wildcard
   subscriptions still build the subject from the topic.
3. **Baseline script**: `mqtt()` in `native-baseline.sh`, as above.

One thing I tried and did not adopt: `set_tcp_nodelay true` on Mosquitto. It
took the tool's latency-bound (a) from about 1k to 2.5k/s in one A/B. It did
not change the driver's 512-publisher fanout (64k vs 74k, 59k vs 63k,
alternating), so the config is unchanged.

### What limits it

- **Mosquitto's single thread.** At about 90k/s QoS 1 fanout, `top` inside the
  container shows it at 70–85% of one core: four packets per message, and the
  forward on both sides of it. The driver process stays below 1.5 cores, and
  its tokio workers are mostly parked. More publishers past 512 add latency,
  not throughput (1000: 48k, 2000: 55k, in a loaded minute).
- **Docker Desktop's forward** (`com.docker.backend` at 2.6 cores during a
  loaded run). It is shared with every other agent's broker.
- **The native tool is the weaker client here.** It cannot keep more than one
  QoS 1 publish in flight per connection, so the 80% target is met by a wide
  margin. The useful ceiling is its QoS 0 rate, which `batch` now matches.

### Tests (this round)

- `mqtt_e2e`, `STREAMS_E2E_RESTART=1`, strict: 10/10, three runs in a row.
- `tests/mqtt.rs` (in-process rumqttd): 15/15. Driver unit tests: 6/6.
  `examples/mqtt.rs` against the plaintext broker: ok.
- `cargo clippy -p tpp-streams --all-targets --features mqtt,protobuf,live-tests,tls -- -D warnings`: clean.
- `rustfmt --edition 2024 --check` on the changed files: clean.
- `cargo tree -p tpp-streams --all-features -i aws-lc-rs`: no match.
