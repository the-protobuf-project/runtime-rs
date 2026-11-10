//! Delivery-side instruments shared by durable providers.

use tpp_telemetry::opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Meter, UpDownCounter},
};

/// Counts deliveries, settlements, messages in flight, and consumer lag.
///
/// Built once per provider from a caller-supplied Meter; `Metrics::default()`
/// records nothing, so a provider without a Meter pays no instrument cost.
#[derive(Clone, Default)]
pub struct Metrics {
    instruments: Option<Instruments>,
}

#[derive(Clone)]
struct Instruments {
    delivered: Counter<u64>,
    settled: Counter<u64>,
    inflight: UpDownCounter<i64>,
    lag: Gauge<i64>,
}

impl Metrics {
    /// Builds the instruments from `meter`.
    pub fn new(meter: &Meter) -> Self {
        Self {
            instruments: Some(Instruments {
                delivered: meter
                    .u64_counter("streams_delivered_total")
                    .with_description("Messages handed to a durable consumer")
                    .with_unit("1")
                    .build(),
                settled: meter
                    .u64_counter("streams_settled_total")
                    .with_description("Durable deliveries acked or naked")
                    .with_unit("1")
                    .build(),
                inflight: meter
                    .i64_up_down_counter("streams_inflight")
                    .with_description("Durable deliveries handed over and not yet settled")
                    .with_unit("1")
                    .build(),
                lag: meter
                    .i64_gauge("streams_consumer_lag")
                    .with_description("Messages retained ahead of a durable consumer")
                    .with_unit("1")
                    .build(),
            }),
        }
    }

    /// Records one delivery handed to `consumer`.
    pub fn delivered(&self, subject: &str, consumer: &str) {
        if let Some(x) = &self.instruments {
            let labels = labels(subject, consumer);
            x.delivered.add(1, &labels);
            x.inflight.add(1, &labels);
        }
    }

    /// Records one settlement; `outcome` is `"ack"` or `"nak"`.
    pub fn settled(&self, subject: &str, consumer: &str, outcome: &'static str) {
        if let Some(x) = &self.instruments {
            let labels = labels(subject, consumer);
            x.inflight.add(-1, &labels);
            let mut labels = labels.to_vec();
            labels.push(KeyValue::new("outcome", outcome));
            x.settled.add(1, &labels);
        }
    }

    /// Records how far `consumer` is behind the head of the log.
    pub fn lag(&self, subject: &str, consumer: &str, n: i64) {
        if let Some(x) = &self.instruments {
            x.lag.record(n, &labels(subject, consumer));
        }
    }
}

/// The two bounded attributes every consumer-side instrument carries.
fn labels(subject: &str, consumer: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("subject", subject.to_owned()),
        KeyValue::new("consumer", consumer.to_owned()),
    ]
}
