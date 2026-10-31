//! Create-once metric handles shared by every operation of a component.
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, UpDownCounter},
};

pub(crate) struct Metrics {
    pub count: Counter<u64>,
    pub duration: Histogram<f64>,
    pub active: UpDownCounter<i64>,
    pub read: Counter<u64>,
    pub written: Counter<u64>,
    pub queue: UpDownCounter<i64>,
}
impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            count: meter
                .u64_counter("runtime.operation.count")
                .with_unit("{operation}")
                .build(),
            duration: meter
                .f64_histogram("runtime.operation.duration")
                .with_unit("s")
                .build(),
            active: meter
                .i64_up_down_counter("runtime.operation.active")
                .with_unit("{operation}")
                .build(),
            read: meter
                .u64_counter("runtime.records.read")
                .with_unit("{record}")
                .build(),
            written: meter
                .u64_counter("runtime.records.written")
                .with_unit("{record}")
                .build(),
            queue: meter
                .i64_up_down_counter("runtime.ingestion.queue.depth")
                .with_unit("{record}")
                .build(),
        }
    }
}

pub(crate) fn labels(
    component: &'static str,
    backend: &'static str,
    operation: &'static str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new("component", component),
        KeyValue::new("backend", backend),
        KeyValue::new("operation", operation),
    ]
}
