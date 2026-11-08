# Examples

[Streams guide](../README.md) · [Local brokers](../docker/README.md)

One runnable program per backend, each porting the matching runtime-go
example. Start the broker from [docker/](../docker/README.md), then:

```bash
cargo run -p tpp-streams --features redis --example redis
```

| Example | Shows |
| --- | --- |
| `kafka` | Ordering by partition key and a durable consumer. |
| `nats` | Core NATS and JetStream through one contract. |
| `rabbitmq` | A durable consumer that naks once and acks on redelivery. |
| `redis` | All three modes: pub/sub, scheduled delivery, and Streams consumers. |
| `mqtt` | A persistent session outliving its consumer. |
| `zeromq` | Brokerless PUB/SUB over TCP. |
