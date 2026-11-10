//! Flattens the in-memory metric exporter's data points for assertions.
use super::Measurement;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter,
    data::{AggregatedMetrics, MetricData},
};
use std::collections::BTreeMap;

/// Every data point `exporter` holds, sums and histogram counts alike.
pub(super) fn collect(exporter: &InMemoryMetricExporter) -> Vec<Measurement> {
    let mut out = Vec::new();
    let Some(resource) = exporter.get_finished_metrics().unwrap_or_default().pop() else {
        return out;
    };
    for scope in resource.scope_metrics() {
        for metric in scope.metrics() {
            let name = metric.name().to_string();
            let mut push = |value: f64, attrs: BTreeMap<String, String>| {
                out.push(Measurement {
                    name: name.clone(),
                    value,
                    attributes: attrs,
                })
            };
            let attrs = |it: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                it.map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect()
            };
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    for p in sum.data_points() {
                        push(p.value() as f64, attrs(&mut p.attributes()));
                    }
                }
                AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                    for p in sum.data_points() {
                        push(p.value() as f64, attrs(&mut p.attributes()));
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                    for p in hist.data_points() {
                        push(p.count() as f64, attrs(&mut p.attributes()));
                    }
                }
                _ => (),
            }
        }
    }
    out
}
