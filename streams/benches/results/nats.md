# NATS: throughput results (round 4)

Backends: `nats` (core NATS, `NatsProvider`) and `jetstream` (`JetStreamProvider`).
Numbers come from one Mac with Docker Desktop while four other agents ran their
own benches, so they are noisy. Use them to compare before and after, not as final
figures. Every run used 10 subjects and 256-byte StringValue payloads (Int64Value
for `batch`), and decoded each message on receipt.

```text
cargo bench -p tpp-streams --features nats,protobuf,live-tests,tls --bench throughput -- \
    --backend <nats|jetstream> --profile <plain|tls> --scenario <...> --messages N --rate R
```

## What changed

1. **JetStream `publish_batch` is pipelined** (`src/drivers/nats/jetstream.rs`). It used to
   call `provider::publish_each`, which waits for one PubAck round trip per message. Now it
   sends every frame in order with `send_publish` and collects the PubAcks afterwards
   through a `FuturesOrdered`, with at most 1,024 per call waiting at once. This is the
   same approach runtime-go and our Kafka and RabbitMQ drivers use.
   - **Ordering:** frames go out one after another, so the stream stores them in order.
     The first version used `stream::buffered`, and it reordered messages: the semaphore
     handed out permits in order, but the sends that followed ran in poll order. The new
     live test `jetstream_a_large_batch_lands_in_order` caught this.
   - **No deadlock:** async-nats allows 5,000 PubAcks in flight per context. When a send
     has to wait for one of those permits, the batch keeps collecting its own outstanding
     PubAcks, so it frees permits instead of waiting on itself. 32 concurrent batches of
     1,000 (32,000 requested) complete.
   - **Delivery guarantees:** each message still carries its own `Nats-Msg-Id`, so
     retrying a batch is deduplicated. Failures are reported by index in `PartialBatch`.
     A single `publish` still returns only after the server has stored the message.
2. **`tls` profile** (`benches/throughput/targets/nats.rs`). It uses mTLS with
   `docker/tls/out` and logs in as the `e2e-password` user, the same way
   `tests/nats_e2e.rs` connects. The password user's permissions now also allow
   `bench.>`. `forbidden.*` is still refused, so the permission test is unchanged.
3. **`STREAMS_DURABILITY=strict|prod`** (`docker/compose.nats.secure.yaml`). This choice
   mounts `compose.nats.secure.<mode>.conf` as `durability.conf`, which the main config
   includes.
   - `strict` (the default) keeps `sync_interval: always`.
   - `prod` uses the server default: writes reach the page cache before the ack and are
     fsynced every 2 minutes.

   The compose header explains the trade-off.

Things I tried and left out:
- **Raising `client_capacity`** (2048 to 8192): no difference I could measure.
- **Ack batching (`AckPolicy::All`):** an ack is already a fire-and-forget publish that
  async-nats queues. `AckPolicy::All` would also change the consumer contract that
  runtime-go shares, and durable consumers already keep pace with publishing.
- **Larger `max_ack_pending` or pull batch:** no consumer-side limit showed up.

## Before and after (plain, unpaced, noisy)

| backend / scenario | before | after |
|---|---|---|
| jetstream batch (32 pubs × 100) | 33.4k/s | 108k–154k/s |
| jetstream fanout, 32 pubs | 34.9k/s | unchanged (see limits); 69k (128 pubs), 98k–117k (512 pubs) |
| jetstream durable, 32 pubs | 35.4k/s | 69k (128 pubs), 88k (512 pubs) |
| core fanout / batch / group | 368k / 1.19M / 325k | unchanged (no driver bottleneck) |

## Does it handle the goal? (paced, `--messages` = 5 × rate, `ok: true`, delivered ≥ 0.95 R)

| backend | scenario | plain | tls (prod) | p50 / p99 ms (plain, tls) |
|---|---|---|---|---|
| nats | fanout | 100k ✔ | 100k ✔ | 1.1 / 4.4, 1.3 / 6.7 |
| nats | batch | 100k ✔ | 100k ✔ | n/a |
| nats | group (2 per subject) | 100k ✔ | 100k ✔ | 1.1 / 5.5, 1.3 / 4.9 |
| jetstream | fanout (256 pubs) | 50k ✔ | 50k ✔ | 1.3 / 12.6, 2.3 / 8.8 |
| jetstream | batch (32 × 100) | 100k ✔ | 100k ✔ | n/a |
| jetstream | durable (256 pubs) | 50k ✔ | 50k ✔ | 2.4 / 10.9, 2.1 / 8.3 |
| jetstream | fanout / durable at 100k (512 pubs) | 87k / 77k ✘ | 80k ✘ | ~6 / 16 |

Unpaced ceilings:

| backend | scenario | plain | tls (prod) |
|---|---|---|---|
| nats | fanout | 350k–455k/s | 284k/s |
| nats | batch | 1.2M/s | 1.1M/s |
| nats | group (2 per subject) | — | 325k/s |
| jetstream | batch | 108k–154k/s | 102k–142k/s |
| jetstream | fanout (512 pubs) | 98k–117k/s | 60k/s at 256 pubs |
| jetstream | durable (512 pubs) | 88k/s | 72k/s (63k/s with 2 consumers) |

Unpaced core latency (50–180 ms p50) is queueing in the publisher. Paced latency is
about 1 ms.

`tls` with `strict` durability is far slower: about **1.1k–1.3k msg/s** on JetStream,
for single publishes and batches alike. Every write waits for an fsync on Docker
Desktop's VM disk. Above a few thousand messages in flight, PubAcks pass async-nats'
5-second ack timeout and are reported as publish errors. That limit comes from the
broker and the disk, not the driver. Use `prod` for throughput.

## Best parameters

- **Core NATS:** the defaults (32 publishers). The async-nats per-subscription buffer
  (`subscription_capacity`, 65,536) was never exceeded, even with 1M messages unpaced:
  `lost` = 0. If it were exceeded, async-nats would drop messages under its slow-consumer
  policy and emit a `SlowConsumer` event. Those drops would show up as `lost`, not as a
  driver fault.
- **JetStream single publish:** `--publishers` 256–512. Each `publish` waits for its
  PubAck by contract, so throughput is about in-flight ÷ round trip. 32 publishers top
  out near 35k/s.
- **JetStream batch:** `--batch` 100–1000 with 32 publishers.
- **Durable:** harness prefetch 1024 (the pull batch), server default `max_ack_pending`
  (1000). The consumer always kept up with the publishers.
- **`group`:** works on core NATS as a queue group, with no duplicates. It is not
  supported on JetStream: `subscribe` there ignores `group`, as in runtime-go, because a
  JetStream group is a durable consumer's name. Use `durable` with `--consumers N`
  instead.

## What limits it

- **Core NATS:** the single connection's writer and reader tasks, plus the Docker
  Desktop VM network. It is far above 100k/s on both profiles.
- **JetStream single publish:** the PubAck round trip through the Docker Desktop network
  and the server's per-stream store lock. 100k/s needs more than 512 publishers on this
  machine; the server's stream write path tops out around 100k–150k/s here.
- **JetStream batch:** the server store, at 100k–150k/s.
- **TLS:** costs about 10–30% unpaced.
- **Strict durability:** fsync on every write, about 1k/s.

## Proposed shared changes

None needed for NATS. A harness flag for the JetStream ack timeout would help strict-mode
runs. (async-nats' ack timeout is 5 s; `JetStreamProvider` builds its context with
`jetstream::new`.)

## Tests

- `nats_e2e` with `STREAMS_E2E_RESTART=1`, strict: 19/19 passed, three runs in a row.
- `nats_live`: 16/16 passed, including the new large ordered batch test.
- Library unit tests: passed.
- `clippy -D warnings`, `rustfmt --check`, and the `aws-lc-rs` check (no match): clean.

## Round 5: against the official tool

`nats bench` (natsio/nats-box) and the driver were run in alternation over the
same Docker Desktop port forward, plaintext, 256-byte payloads, 200k messages for
JetStream and 1M for core. The Mac was shared with other benchmarks the whole time
(load average 15–55), so a single run of either tool swings by 2–4×. `nats bench js
pub async --dedup` measured anywhere from 40k to 190k/s. The figures below are
medians of 3–9 paired runs. Compare the ratios, not the absolute numbers.

### What the official tool measures

`nats bench js pub` only publishes. The driver's `fanout`, `batch` and `durable`
runs also read every message back through the same forward. With an ordered
consumer reading at the same time, `nats bench` itself drops from about 150k to
about 97k/s. With a durable consumer acking at the same time, it drops to about
86k/s. Round 4's "67%" set a pub+sub run against a publish-only run. The like-for-like
comparisons are:

| JetStream | `nats bench` (median) | before (round-4 driver, paired) | after | ratio after |
|---|---|---|---|---|
| publish only, single publishes (512 pubs, `--consumers 0`) vs `js pub async --dedup` | 151k | 159k | 138k | ~0.9 (noise; paired runs split 3–4) |
| publish only, `batch --message string` (32 × 500, 256 B) | 151k | — | 324k | > 2 |
| publish + ordered consumer: `fanout` 512 pubs vs `js pub` + `js ordered` | 97k delivered | 78k | 96k | **0.99** |
| publish + ordered consumer: `batch --message string` | 97k delivered | — | 108k | 1.1 |
| publish + durable consumer, explicit acks: `durable --consumers 1` vs `js pub` + `js consume` | 86k delivered | 74k | 80k | **0.93** |

In every pair the delivered rate equalled the published rate. `lost` and `dup` were 0 and every run reported `ok`.

| core NATS (1M msgs) | `nats bench` pub+sub | ours, 1 / 32 / 256 publishers (median delivered) |
|---|---|---|
| fanout | 228k (169k–469k) | 236k / 280k / 300k |

Core NATS is at or above the tool at every publisher count. Round 4's 256-publisher
dip did not reproduce once publishers and subscribers had separate connections, though the noise
on this machine cannot rule out a small effect.

### What changed

- **JetStream PubAcks (`drivers/nats/acks.rs`, new).** async-nats' `send_publish`
  turns each publish into its own request, which costs three things:
  - an inbox from the global nuid generator, which sits behind a mutex;
  - a permit from one semaphore shared by every publisher on the connection;
  - two `tokio::time::timeout`s on the runtime's timer wheel.

  `sample` taken during a 32-publisher batch run showed 30% of worker time in
  `__psynch_mutexwait` on those three locks. The driver now does what nats.go's
  `PublishAsync` does:
  - one `<inbox>.*` reply subscription per connection, opened on the first publish;
  - a counter for reply tokens and 16 sharded pending maps;
  - one task that routes each PubAck to its waiter and sweeps waits older than 5 s
    every 250 ms. That 5 s is async-nats' JetStream ack timeout.

  In-flight publishes are capped at 4,000 per connection. That is nats.go's
  `PublishAsyncMaxPending`. Without the cap, 32 × 1,024 in flight overflowed the
  stream's inbound queue and the server answered `429 too many requests` (10167). A
  batch takes and returns permits 64 at a time, and it settles its own oldest waits
  before it waits on anyone else.

  Nothing changes on the wire: the same subject, `Nats-Msg-Id` and frame. The
  reply goes under the client's inbox prefix. `publish` still returns only after a
  PubAck that names a stream and sequence. A 503 no-responders reply is reported as
  no stream, and a JetStream error is reported as a failure. After the sweep,
  `mutexwait` was gone from the profile, and the client sat mostly idle waiting on
  the network.
- **Batch:** the waits are a plain `VecDeque` of receivers, not a `FuturesOrdered` of
  boxed futures.
- **Receive paths:** all three now use `provider::unpack_shared` with one shared
  subject per subscription (`LastSubject`). Those are the core subscribe, the JetStream ordered subscribe and the durable consume.
- **Bench target:** publishers and subscribers each have their own connection, as in
  `nats bench` and as in a service that both produces and consumes heavily. When they shared one, PubAcks queued behind
  deliveries on the connection's single reader task.
- **Harness (generic):** `--consumers 0` attaches no receiver, which gives a publish-only run to set
  beside a tool that only publishes. Only that case changes. `fanout` and `batch`
  still default to 1 receiver.

### What limits it now

The client process is mostly idle (`__psynch_cvwait`) in every JetStream run.
What limits throughput is the PubAck round trip through the Docker Desktop forward,
and the server delivering each message back out through that same forward. The
official tool hits the same limit. Single publishes need about 500 in flight, so
`--publishers` should be 256–512. `batch` needs about 32.

### Tests

- `nats_e2e`, secure, `STREAMS_E2E_RESTART=1`, strict: 19/19 passed, three times in a
  row. This includes the forbidden-publish case, which now fails through the ack
  sweep.
- `nats_live`: 16/16 passed.
- Library tests: 17/17 passed. New: PubAck parsing, and `LastSubject`.
- `clippy -D warnings`, `rustfmt --check` and the `aws-lc-rs` check (no match): clean.
