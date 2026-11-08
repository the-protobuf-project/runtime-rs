# RabbitMQ throughput

Driver: `streams/src/drivers/rabbitmq` (lapin 4.12, tokio). Broker:
`rabbitmq:management-alpine` (4.4.0), `plain` = `docker/compose.rabbitmq.yaml`,
`tls` = `docker/compose.rabbitmq.secure.yaml` (AMQPS mTLS, SASL EXTERNAL, data on
a named volume). Every figure below came from one Mac with Docker Desktop while
four other agents were benchmarking their brokers, so they swing by ±30% from
run to run. Use them to compare, not as the final matrix. 100k messages unless
noted; 10 subjects; StringValue 256 B (batch: Int64Value).

## Scenarios

`fanout`, `batch` and `durable` are supported. `group` is not: a RabbitMQ
subscription is an exclusive queue of its own, and `subscribe` refuses
`Options::group`, because sharing a subject under a name is what a durable
consumer (one named queue) already is. The harness would panic, so `group`
was not run.

## What changed and why

1. **Publishing channel pool** (`RabbitConfig::publish_channels`, default
   `DEFAULT_PUBLISH_CHANNELS = 4`). Single publishes used to share one
   confirm-mode channel. The Rust side holds no lock across the send: the mutex
   only guards swapping in a reopened channel, and confirms are already awaited
   concurrently. But one AMQP channel is one Erlang process on the broker, which
   handles its publishes and confirms in turn. Single publishes now take the
   pool round-robin, and each slot is reopened on its own after the broker
   closes it, so a publish to a deleted exchange also now takes down a quarter
   of the in-flight publishes rather than all of them. A batch takes one
   channel, so its order holds. A caller that awaits each publish in turn stays
   ordered on any channel, because the next is not sent before the previous is
   confirmed.
   - 32 publishers (harness default): no difference within the noise
     (interleaved A/B, three rounds each: fanout 1 ch 26.2k/22.1k/18.7k vs
     4 ch 21.2k/17.5k/33.7k; durable 13.0k/14.2k/11.4k vs 15.6k/12.7k/12.3k).
   - 128 publishers: 1 ch **33.7k / 47.6k**, 4 ch **63.9k / 58.8k** msg/s
     (fanout, 200k). Here the single channel process is the bottleneck. 8 or
     16 channels were no better than 4.
2. **Batch sends are queued in order before any write is awaited.** Before,
   each `basic_publish` was awaited in turn, which is one hand-off to lapin's
   IO thread and back per message. Now every send in the batch is first polled
   in order (a small in-order join; `join_all` does not promise first-poll
   order for large sets, and the first poll is what numbers the message for
   confirmation and queues its frames). Then every write, and then every
   confirm, is awaited. A/B, 300k messages: batch 100 **83.0k → 99.0k**,
   batch 500 **97.2k → 123.3k** msg/s.
3. **`tls` bench profile** in `benches/throughput/targets/rabbitmq.rs`: AMQPS on
   15671 with the e2e suite's client certificate, logging in by SASL EXTERNAL
   (`RABBITMQ_E2E_ADDR`, `STREAMS_E2E_TLS_DIR`). `RABBITMQ_PUBLISH_CHANNELS`
   overrides the pool size for experiments.
4. **`STREAMS_DURABILITY=strict|prod`** on the secure compose. On RabbitMQ the
   two are the same broker settings. RabbitMQ's default is already the strict
   behaviour: with confirms, a persistent message on a durable queue is
   confirmed only after the queue has written it to disk and fsynced it, with
   fsyncs grouped per batch of pending confirms. There is no supported weaker
   setting that keeps confirms and persistence. The mode is accepted and
   recorded as a container label; the compose header explains why.

## Measured and ruled out

- **Consumer acks.** I temporarily made `ack()` fire and forget, so the
  consumer never waited on lapin's IO thread. Durable came out 11.4k (32 pubs)
  and 17.2k (128 pubs), against 12–15k and 25k with ordinary acks: no gain.
  Durable is bounded by the publish confirm, which waits on the broker's
  fsync, not by acks. Cumulative `basic.ack multiple=true` was therefore not
  added. Each delivery is acked on its own (`Delivery::ack` is per message),
  and coalescing would only help if acks were the bottleneck. It would also
  need deferred acks for out-of-order settlement, which hold prefetch slots
  and change behaviour.
- **Client CPU.** During unpaced fanout the `lapin-io-loop` thread used ~26% of
  one core, and the tokio workers ~7% each. The client is not saturated.
- **Prefetch.** The harness asks for 1024 through `Options`, which the driver
  passes as `basic.qos` per consumer channel. The driver default stays 32
  (runtime-go's, which is right for fair round-robin), and consumers never
  lagged the publishers.
- `TCP_NODELAY` is already set by lapin's tokio connector.

## Best numbers seen (noisy)

Unpaced ceilings (`--rate 0`):

| profile | scenario | publishers | msg/s delivered | p50 / p99 ms |
| --- | --- | --- | --- | --- |
| plain | fanout | 32 | 20–33k | 0.9–1.3 / 2–5 |
| plain | fanout | 128 | 59–64k | 1.7 / 5.3 |
| plain | batch (100) | 32 | 99k | n/a |
| plain | batch (500) | 32 | 123–163k | n/a |
| plain | durable | 32 | 12–16k | 1.4–1.7 / 4–7 |
| plain | durable | 256 | 29.7k | 5.4 / 25 |
| tls (prod) | fanout | 32 | 20.9k | 1.3 / 4.6 |
| tls (prod) | batch (500) | 32 | 154k | n/a |
| tls (prod) | durable | 32 | 13.4k | 1.6 / 5.2 |
| tls (prod) | durable | 128 | 35.2k | 2.7 / 8.7 |

Paced (`--rate R`, `--messages ≥ 5R`), all `"ok": true`:

| profile | scenario | R | params | delivered | p50 / p99 ms | ≥ 0.95R |
| --- | --- | --- | --- | --- | --- | --- |
| plain | fanout | 20k | 32 pubs | 17.2k | 1.6 / 6.6 | no |
| plain | fanout | 20k | 64 pubs | 19.9k | 0.9 / 3.5 | yes |
| plain | fanout | 50k | 256 pubs | 49.9k | 1.4 / 7.0 | yes |
| tls | fanout | 20k | 64 pubs | 20.0k | 1.1 / 3.7 | yes |
| tls | fanout | 50k | 256 pubs | 46.3k | 3.7 / 16.7 | no (0.93) |
| plain | batch | 50k | batch 100 | 50.0k | n/a | yes |
| plain | batch | 100k | batch 500 | 89.8k | n/a | no (0.90) |
| tls | batch | 100k | batch 500 | 99.7k | n/a | yes |
| plain | durable | 20k | 128 pubs | 20.0k | 1.6 / 7.6 | yes |
| tls | durable | 20k | 128 pubs | 20.0k | 1.9 / 8.0 | yes |
| tls | durable | 30k | 256 pubs | 29.5k | 3.5 / 18.9 | yes |

## Parameters that work best

- `publish_channels` 4 (the default).
- Single publishes are bound by how many confirms are in flight: rate ≈
  publishers ÷ confirm round trip (~1–1.5 ms through Docker Desktop's VM;
  2–3 ms for durable, which includes the fsync). 20k needs ≥ 64 concurrent
  publishers for fanout and ≥ 128 for durable; 50k fanout needs ~256. With the
  harness default of 32, fanout sits around 20k and durable around 13k.
- Batch: 500 per batch, 32 publishers. 100 per batch caps near 100k.
- Prefetch 1024 for the bench. Keep 32 (the default) where several consumers
  share a queue.

## Goal

- **20k**: reached on every supported scenario and both profiles, given enough
  concurrent publishers (64 fanout, 128 durable) or batching. Not reached with
  32 single-message publishers on fanout (17–21k, borderline) or durable
  (12–16k).
- **50k**: reached for fanout (256 publishers, plain) and batch. Durable
  peaked at ~35k.
- **100k**: batch only (99.7k paced on tls, 123–163k unpaced on plain).

## What limits it

- **Protocol and contract.** `publish` returns only after the broker's
  confirm, so each caller has one message in flight. Throughput is publishers
  divided by the round trip, and the driver cannot pipeline across calls
  without weakening what a returned publish means. `Batch` is the pipelined
  path, and it is the one that reaches 100k.
- **Broker, durable.** Confirms for persistent messages on durable queues wait
  for the queue's fsync to a Docker volume inside the VM. The broker used 2–4
  cores for ~10k durable msg/s while other brokers were running.
- **Broker, one channel.** One channel is one serial process; the pool lifts
  that limit at high concurrency.
- **Docker Desktop VM networking.** It adds most of the ~1 ms round trip; on
  Linux with the broker local, the same driver should do several times better
  per publisher.
- **One connection.** The provider takes one caller-owned connection, so one
  lapin IO thread carries publishes, confirms and deliveries. It was not
  saturated here (~26% CPU), but a separate connection for consumers is the
  next lever, and it is the application's choice.

## Tests

The secure e2e suite with `STREAMS_E2E_RESTART=1` (strict) passed 3/3 in a row
(7 tests each). The plaintext live suite passed (13), and so did the driver
unit tests (10). Clippy (`rabbitmq,protobuf,live-tests,tls`, `-D warnings`) is
clean, rustfmt (2024) is clean on the changed files, and `cargo tree -i
aws-lc-rs` finds no match.
