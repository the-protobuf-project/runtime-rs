# Kafka throughput

Broker: apache/kafka 3.9.1, single-node KRaft in Docker Desktop on a 14-core Mac.
Every topic has 10 partitions (the bench `KafkaConfig`), and the harness uses 10 subjects, so there are 100 partitions.
Messages are 256-byte `StringValue`s, or `Int64Value`s for `batch`, decoded on every receipt.

Profiles:

- `plain` is `docker/compose.kafka.yaml` over PLAINTEXT on 19092.
- `tls` is `docker/compose.kafka.secure.yaml` over **mutual TLS on 19093**, the e2e suite's primary (`Mtls`) principal, with ACLs on. SCRAM on 19094 is not benchmarked.

Unless a row says otherwise, `tls` was measured with `STREAMS_DURABILITY=prod`.

**Noise warning:** five agents ran brokers on the same Docker VM at the same time. The Kafka container alone used about 100–120 % CPU next to the RabbitMQ and other brokers. The same command gave 15.6k one run and 48k another, so treat these numbers as before/after comparisons, not as final figures.

## What changed and why

1. **Producer defaults, applied only where the caller has not set them** (`producer_config`). `acks=all` and idempotence are unchanged.
   - `linger.ms=1`, down from librdkafka's 5. A single publish waits for its ack, so a publisher's rate is roughly (publishes in flight) ÷ (latency per publish), and the 5 ms linger added 5 ms to every publish. Measured at 256 publishers: linger 5 gave 28.4k/s, linger 1 gave 35.5k–53.6k/s, and linger 0 gave 11.8k/s. Linger 0 is the worst because the idempotent producer allows only 5 requests in flight, so tiny requests starve it.
   - `queue.buffering.max.messages=1000000` and `queue.buffering.max.kbytes=1048576`, so many concurrent batches wait for room instead of hitting a full queue (rdkafka sleeps 100 ms on a full queue).
   - `batch.num.messages=10000`.
   - Compression is left off. The bench payload is all `x`, so compression would flatter the numbers unrealistically.
2. **Durable acks no longer commit synchronously, one per message.**
   - Before: each ack did a blocking `commit(Sync)`, one broker round trip per message.
   - Now: an ack records the offset in a per-consumer ledger, and a background committer issues one synchronous commit at a time, covering every ack made since the previous commit.
   - The committed offset is the **lowest unacked offset** in each partition, so nothing is ever committed past an unsettled delivery. This replaces the old behaviour, where an out-of-order ack committed past earlier deliveries. A nak or a dropped delivery holds its partition in place, and those messages are redelivered on reassignment.
   - On a rewind (after a rebalance), the partition's ledger is reset, and commits are limited to partitions the consumer still owns.
   - When the subscription is dropped, the ledger closes and a final commit runs before the consumer leaves the group. Any ack after that fails, as before.
   - A crash between an ack and its commit causes redelivery, which is still at-least-once.
   - Durable rate: before, sequential sync commits capped it at about 10 ÷ commit RTT. After: 15.6k–49.6k/s unpaced on `plain`, and 20k paced passed.
3. **Group subscribe no longer loses the first messages.**
   - Before: a member with no committed offset started at "latest" whenever its assignment arrived, after `subscribe` had already returned. 1018 of 200k messages were lost, so `ok` was false.
   - Now: a consumer context records each partition's end at subscribe time. On assignment, a partition with no committed offset starts at that recorded end. This matches what the tail reader already did. It uses rdkafka's safe `assign`/`incremental_assign` and no unsafe code.
   - Result: 0 lost, 0 duplicates.
4. Each subject's topic name is computed once per manager instead of on every publish.
5. `STREAMS_DURABILITY=strict|prod` in the secure compose file.
   - `strict` is the default. It sets `log.flush.interval.messages=1`, which fsyncs every append.
   - `prod` is Kafka's default: flushing is left to the page cache, and durability comes from acks=all and replication.
   - The trade-off is documented in the compose file's header.

## Parameters that work

| Scenario | Settings |
|---|---|
| Single publishes (`fanout`, `durable`, `group`) | `--publishers 256` on plain; 256–512 on tls. The default 32 is latency-bound at about 4k/s (≈32 ÷ 6 ms). |
| `batch` | `--batch 500` reaches 100k; `--batch 100` stops around 80–87k. |
| `group` | `--consumers 2`–`4` per subject. |
| Partitions | 10 per topic. |

## Results

### Paced runs (`--messages` ≥ 5×R, at least 100k)

✓ means the run reported `ok` and delivered at least 0.95×R. ✗ means it fell short.

| Profile | Scenario | 20k | 50k | 100k | p50 / p99 at 20k |
|---|---|---|---|---|---|
| plain | fanout | ✓ | – | – | 6.4 / 80 ms |
| plain | batch | ✓ | ✓ | ✓ (batch 500); ✗ 80k (batch 100) | – |
| plain | durable | ✓ | – | – | 8.5 / 64 ms |
| plain | group | ✓ | – | – | 2.6 / 1355 ms |
| tls (prod) | fanout | ✓ (512 publishers); ✗ 14.4k (256 publishers) | – | – | 14 / 188 ms |
| tls (prod) | batch | ✓ | ✓ | ✗ 83.6k (batch 100) | – |
| tls (prod) | durable | ✗ 19.0k (256 publishers); ✗ 16.9k (512 publishers) | – | – | 7.4 / 76 ms |
| tls (prod) | group | ✓ | – | – | 9.1 / 1408 ms |

The group p99 of about 1.4 s comes from rebalances while the 20 members are still joining. It is not steady-state latency.

### Unpaced ceilings seen

| Profile | Scenario | Rate |
|---|---|---|
| plain | fanout | 53.6k/s at 256 publishers (35k on another run); 4.0k/s at 32 publishers, before and after the change |
| plain | batch | 87k/s (batch 100); 105k/s (batch 500) |
| plain | durable | 15.6k–49.6k/s |
| plain | group | 29k/s |
| tls (prod) | fanout | 15.6k–48k/s |
| tls (prod) | batch | 83k/s (batch 100); 100k/s (batch 500) |
| tls (prod) | durable | 25.7k/s |
| tls (prod) | group | 46k/s |
| tls (strict) | fanout | 1.9k/s: an fsync per produce request |
| tls (strict) | batch | 33k/s |

Before the changes, at the default 32 publishers, plain fanout ran at 4.0k/s, with 25 `MessageTimedOut` publish errors in that one run (not seen again).

## Limits

- **Single publishes are latency-bound by design**, because every publish waits for an acks=all ack. Throughput is publishers ÷ latency, so the harness needs hundreds of publishers to reach 20k+.
- The idempotent producer allows at most 5 requests in flight per connection. Under load, librdkafka grows the batches to compensate.
- At 20–50k single publishes per second, the broker container's CPU on the shared Docker Desktop VM is the limit: about 100–120 % for Kafka while the bench process used about 13 %.
- Over TLS, the same broker work plus encryption puts single publishes around 15–20k under contention. 20k passed for fanout with 512 publishers, but durable missed it in both runs: 19.0k with 256 publishers and 16.9k with 512.
- **Strict durability** (an fsync per append) caps single publishes near 2k/s. That is the cost of the e2e profile, not a driver limit.

## Proposed, not done

- Shard publishes across several producer instances. Each instance has its own connection and its own 5 in-flight requests. One batch would stay on one producer, to keep its order.
