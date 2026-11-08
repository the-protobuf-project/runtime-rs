# Redis throughput (round 4)

Backends: `redis` (pub/sub, `RedisProvider::new`) and `redis-durable` (Redis
Streams, `RedisProvider::durable`). Profiles: `plain` (docker/compose.redis.yaml)
and `tls` (docker/compose.redis.secure.yaml: mTLS on 16381, ACL user `streams`,
database 3, AOF). All numbers below come from one Mac running five benchmark
agents and Docker Desktop at once, so they are noisy (the same unpaced pub/sub
run ranged 11k–42k/s); read them as before/after on this driver, not as final
figures.

```text
cargo bench -p tpp-streams --features redis,protobuf,live-tests,tls --bench throughput -- \
    --backend redis-durable --profile tls --scenario durable --messages 500000 --rate 100000 --publishers 256
```

## What changed and why

**Durable consumer loop (src/drivers/redis/durable.rs).** The baseline durable
run published at 44k/s but delivered at 12k/s with p50 latency of 2.3 s: the
consumers were the bottleneck. Every loop iteration paid three round trips on
the reader's connection (XAUTOCLAIM, XPENDING for the lag gauge, then
XREADGROUP COUNT 64), and every message paid one XACK round trip that the
harness's receiver awaits before reading the next message.

- XREADGROUP / XAUTOCLAIM `COUNT` now follow the consumer's prefetch (clamped to
  1..=1024) instead of a fixed 64. Default prefetch is still 64.
- XAUTOCLAIM runs at most every `min(reclaim_after / 2, 1 s)` while new work
  keeps arriving, immediately again when it found something, and immediately
  after a read that came back empty (an idle consumer). Redelivery still
  happens within about 1.5x the reclaim interval; the 300 ms reclaim tests pass
  unchanged.
- The lag gauge (XPENDING summary) is sampled once a second instead of per read.
- Acks are coalesced: each consumer has an ack task that drains every queued
  ack into one multi-id `XACK key group id1 id2 ...` (up to 1024 ids). Nothing
  waits to fill a batch, so an idle consumer's ack goes out alone at once.
  Deliveries share one `Arc` ack target instead of cloning key, group and
  subject per message.
- New `RedisConfig::confirm_acks` (default **true**, the old behaviour): with it
  on, `ack()` returns after the XACK carrying its id is answered, so ack errors
  are reported. Off, `ack()` queues the id and returns; failures are logged.
  At-least-once is unchanged either way: an entry stays in the group's pending
  list until its XACK lands, and one whose XACK is lost is reclaimed and
  redelivered as if the handler had died. JetStream's plain ack makes the same
  trade. The bench target turns it **off** (`REDIS_CONFIRM_ACKS=1` measures the
  default). A new live test, `unconfirmed_acks_still_settle_every_message`,
  checks that 500 unconfirmed acks all land (pending goes to 0) and nothing
  redelivers.

Before/after, plain, unpaced, 100k messages, 32 publishers:

| run | publish/s | delivered/s | p50 | p99 |
|---|---|---|---|---|
| baseline | 43.7k | 12.2k | 2285 ms | 5839 ms |
| loop changes, acks confirmed | 22.5k | 12.0k | 3691 ms | 4128 ms |
| loop changes, acks unconfirmed | 20–24k | 20–24k | 1.6–1.9 ms | 6.5–8.3 ms |
| unconfirmed, 128 publishers, 200k | 84.9k | 84.6k | 1.8 ms | 8.3 ms |

With confirmed acks the harness's consumer is capped at one message per XACK
round trip per subject (about 10 x 1.2k/s here). Coalescing cannot help that,
because the harness never has more than one ack in flight per consumer; it helps
consumers that ack concurrently or out of the loop.

**Command timeout on the shared connection (src/drivers/redis/mod.rs).** The
ConnectionManager used redis-rs's default 500 ms response timeout. Against the
secure broker with `appendfsync always`, a 20k/s paced durable run got 698
`timed out` publish errors (each ambiguous: an XADD that times out may have
landed, and it is never repeated). The timeout is now 5 s; the same run has 0
errors. A dead server is still noticed by the connection dropping.

**Pipelining check.** The shared RESP3 ConnectionManager is a redis-rs
multiplexed connection: concurrent commands are written back to back and their
replies matched in order, not serialised. Throughput with single publishes is
therefore `publishers / round trip`; 32 publishers top out around 20–40k/s
through Docker Desktop's VM networking, 128 around 60–85k/s, 256 reach 100k/s.
The Replay retry adds nothing on the success path.

**TLS profile (benches/throughput/targets/redis.rs).** `tls` connects exactly
as tests/redis_e2e.rs does: `rediss://streams:...@127.0.0.1:16381/3`, CA plus
client certificate from docker/tls/out (`REDIS_TLS_ADDR`, `REDIS_TLS_CERTS`
override). The `streams` ACL user may only touch `~e2e*:...` keys and
`&e2e*:stream:ch:*` channels, so the profile uses the key prefix `e2ebench`
instead of `bench`. That keeps the ACL as narrow as the e2e suite proves it is,
rather than widening it for a benchmark.

**Durability switch (docker/compose.redis.secure.yaml).**
`STREAMS_DURABILITY=strict` (default; `appendfsync always`, what the e2e suite
runs) or `prod` (`appendfsync everysec`, Redis's default for AOF: survives a
process crash or `docker restart`, may lose about a second of acknowledged
writes on a host/VM crash). A small `sh` entrypoint maps it onto `--appendfsync`
and then execs the image's own entrypoint, which drops to the redis user.

## Results (noisy)

Paced: `--messages 5R`, 10 subjects, 256-byte StringValue (batch: Int64Value,
`--batch 100`). Single-publish scenarios used `--publishers 128` at 20k/50k and
`--publishers 256` at 100k. `tls` is the `prod` durability mode unless noted.

| profile | scenario | 20k/s | 50k/s | 100k/s | unpaced ceiling |
|---|---|---|---|---|---|
| plain | fanout | ok (p50 1.1 / p99 6.0 ms) | ok (1.6 / 6.9) | ok at 256 pubs (1.3 / 4.1); 79k at 128 | 85k (128 pubs) |
| plain | batch | ok | ok | ok | 451k |
| plain | durable | ok (1.6 / 8.1) | ok (2.4 / 9.8) | ok at 256 pubs (1.3 / 4.2); 53k at 128 | 85k (128 pubs) |
| plain | durable batch | – | – | – | 365k pub / 230k delivered |
| tls | fanout | ok (1.3 / 21) | ok (1.8 / 7.9) | 60k at 128 pubs | 102k (256 pubs) |
| tls | batch | ok | ok | ok | 816k |
| tls | durable | ok (1.5 / 9.0) | ok at 256 pubs (1.4 / 14); 46.7k at 128 | 72k at 128 pubs | 69k (256 pubs) |
| tls | durable batch | – | – | ok | 479k |
| tls strict | durable | **1.2k/s** (p50 89 ms) | – | – | ~1.2k |
| tls strict | durable batch | – | – | – | 68k |
| tls strict | fanout | ok (0.7 / 1.9) | – | – | unaffected (PUBLISH is not persisted) |

`durable --consumers 2` (two consumers per subject sharing a name, splitting
the work): tls, 50k/s paced, 128 publishers: 47.2k/s, ok, no duplicates;
publisher-limited at that publisher count.

So: 20k/s is reached in every scenario and profile except durable single
publishes under strict fsync. 50k/s is reached everywhere in prod/plain (some
runs fall a few percent short at 128 publishers on tls, inside the noise).
100k/s is reached by `batch` on both profiles and by single publishes on plain
with 256 publishers; tls single-publish runs peaked at 70–100k.

## Best parameters

- Publishers: 128–256 for single publishes (throughput is in-flight count /
  RTT); `batch` saturates with the default 32 x 100.
- Prefetch: the harness's 1024 is now also the XREADGROUP COUNT; 256–1024 works
  equally well, 64 (the default) is enough for ~50k/s over 10 subjects.
- `confirm_acks: false` for consumers that ack one message before reading the
  next, when a logged rather than returned ack failure is acceptable.
- `max_len`: not needed for these runs (the bench deletes its streams);
  `MAXLEN ~` is approximate and cheap, so setting it in production costs little.

## What limits it

- **Single publishes:** request/response latency across Docker Desktop's VM
  network (~0.5–1.5 ms under this load), times the harness's publisher count.
  The driver pipelines everything it is given.
- **Durable under `appendfsync always`:** the VM disk's fsync rate. Redis
  fsyncs once per event-loop write batch, so pipelined batches amortise it
  (68k/s) while single XADDs from 128 callers do not (1.2k/s). This is the
  broker configuration, not the driver.
- **TLS:** Redis's TLS is single-threaded on the main thread; it costs roughly
  20–30% against plain at the same publisher count.
- **Unpaced delivered rate in `batch`:** the ten consumers (decode + channel
  hand-off per message) trail the 400–800k/s publishers.

## Pub/sub and slow subscribers

No run lost a message on pub/sub. Redis disconnects a subscriber whose output
buffer exceeds `client-output-buffer-limit pubsub` (default `32mb 8mb 60`: hard
32 MB, or over 8 MB for 60 s), which would show as `lost`. With redis-rs it
does not happen in practice: its pub/sub connection drains the socket into an
**unbounded** in-process channel, so a slow reader backs up in client memory
rather than on the server, and the driver's `prefetch`-sized channel bounds
only the hand-off after it. A subscriber that falls behind indefinitely grows
without limit instead of being cut off; bounding that would need a different
pub/sub reader than redis-rs offers, and is worth a follow-up. Groups are still
refused on pub/sub subscribe, so the `group` scenario is unsupported for
`redis`; Streams consumers sharing a name (`durable --consumers N`) are the
work-splitting path.
