# Throughput benchmark

[Streams guide](../README.md) · [Local brokers](../docker/README.md)

`throughput/` drives every provider through the `streams` contract, so each
backend is measured the same way: the same messages, subjects and checks.
Messages are runtime-go's protobuf codec test messages
(`google.protobuf.StringValue` with a 256-byte body, or `Int64Value` for
batches), framed as runtime-go frames them and decoded on every receipt.

## Running one scenario

Start the broker from [docker/](../docker/README.md), then from the workspace
root:

```bash
cargo bench -p tpp-streams --features redis,protobuf,live-tests,tls \
    --bench throughput -- --backend redis-durable --profile plain \
    --scenario durable --messages 250000 --rate 50000 --publishers 256 --consumers 2
```

| Flag | Meaning |
| --- | --- |
| `--backend` | `kafka`, `nats`, `jetstream`, `rabbitmq`, `redis`, `redis-durable`, `mqtt`, `zeromq` |
| `--profile` | `plain`, or `tls` for the secure broker (mutual TLS and the backend's authentication) |
| `--scenario` | `fanout`, `batch`, `durable` or `group` |
| `--rate` | target messages a second across all publishers; `0` runs unpaced |
| `--messages`, `--subjects`, `--publishers`, `--consumers`, `--batch`, `--payload` | run shape |

Each run prints a JSON summary and appends it to `target/bench-results.jsonl`.
A run is `ok` when nothing was lost, undecodable or unacknowledged (and, for
`group`, nothing arrived twice). It holds a target rate when it is `ok` and
delivers at least 95% of that rate.

An acknowledged publish waits about one round trip, so single-publish
scenarios need hundreds of publishers to show what a broker can do; with the
default 32 the harness measures itself.

## Running the whole matrix

```bash
streams/benches/run-matrix.sh            # every backend
streams/benches/run-matrix.sh kafka mqtt # some of them
```

It runs one backend at a time, starting and removing its brokers, through the
plaintext, TLS with production durability (`STREAMS_DURABILITY=prod`) and TLS
syncing every message (`strict`) profiles, at 20k, 50k and 100k msg/s plus an
unpaced run. Run nothing else on the machine meanwhile.

## Results

`results/<backend>.md` records each driver's tuning work: what changed, the
before and after numbers, the parameters that work best, and what limits it.
Those numbers were measured while other backends ran at the same time, so they
show direction and size; the serial matrix gives the comparable figures.
