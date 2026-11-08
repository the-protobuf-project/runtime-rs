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
