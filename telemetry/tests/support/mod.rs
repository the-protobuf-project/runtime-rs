//! In-memory SDK providers verify actual exported signals without globals.
use opentelemetry::{logs::LoggerProvider, metrics::MeterProvider, trace::TracerProvider};
use opentelemetry_sdk::{
    logs::{InMemoryLogExporter, SdkLoggerProvider},
    metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    },
    trace::{InMemorySpanExporter, SdkTracerProvider},
};
use std::collections::BTreeMap;
use telemetry::Telemetry;

pub struct Harness {
    pub signals: Telemetry,
    pub traces: InMemorySpanExporter,
    pub logs: InMemoryLogExporter,
    pub trace_provider: SdkTracerProvider,
    log_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
    metrics: InMemoryMetricExporter,
}

pub struct Measurement {
    pub name: String,
    pub value: f64,
    pub attributes: BTreeMap<String, String>,
}

impl Harness {
    pub fn new() -> Self {
        let traces = InMemorySpanExporter::default();
        let logs = InMemoryLogExporter::default();
        let metrics = InMemoryMetricExporter::default();
        let trace_provider = SdkTracerProvider::builder()
            .with_simple_exporter(traces.clone())
            .build();
        let log_provider = SdkLoggerProvider::builder()
            .with_simple_exporter(logs.clone())
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metrics.clone()).build())
            .build();
        let signals = Telemetry::default()
            .with_meter(meter_provider.meter("runtime-test"))
            .with_tracer(trace_provider.tracer("runtime-test"))
            .with_logger(log_provider.logger("runtime-test"));
        Self {
            signals,
            traces,
            logs,
            trace_provider,
            log_provider,
            meter_provider,
            metrics,
        }
    }

    pub fn measurements(&self) -> Vec<Measurement> {
        self.trace_provider.force_flush().unwrap();
        self.log_provider.force_flush().unwrap();
        self.meter_provider.force_flush().unwrap();
        let mut output = Vec::new();
        let Some(resource) = self.metrics.get_finished_metrics().unwrap().pop() else {
            return output;
        };
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            output.push(Measurement {
                                name: metric.name().into(),
                                value: point.value() as f64,
                                attributes: point
                                    .attributes()
                                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                    .collect(),
                            });
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            output.push(Measurement {
                                name: metric.name().into(),
                                value: point.value() as f64,
                                attributes: point
                                    .attributes()
                                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                    .collect(),
                            });
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                        for point in hist.data_points() {
                            output.push(Measurement {
                                name: metric.name().into(),
                                value: point.count() as f64,
                                attributes: point
                                    .attributes()
                                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                    .collect(),
                            });
                        }
                    }
                    _ => (),
                }
            }
        }
        output
    }
}
