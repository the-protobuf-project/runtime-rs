//! Sustained ingestion rates through the paths generated code uses. Opt-in,
//! and meaningful only in release:
//! `cargo test --release -p tpp-database --test arrow_throughput -- --ignored --nocapture`
use database::arrow::ArrowProvider;
use database::store::{
    Column, DB, For, Kind, ListOptions, Model, Provider, Record, Resource, Role,
};
use database::{IngestConfig, buffered, mirror};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use telemetry::Telemetry;
use telemetry::opentelemetry::{metrics::MeterProvider, trace::TracerProvider};

const POINTS: i64 = 300_000;

struct Point {
    device: String,
    object: String,
    value: f64,
    observed_at: i64,
}

static POINTS_RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
    Resource::new(
        "points",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("object", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
    .with_name("Point")
});

/// The conversions a generator emits: no serde, no reflection.
impl Model for Point {
    fn resource() -> &'static Resource {
        &POINTS_RESOURCE
    }
    fn to_record(&self) -> database::Result<Record> {
        Ok(Record::from([
            ("device".into(), self.device.as_str().into()),
            ("object".into(), self.object.as_str().into()),
            ("value".into(), self.value.into()),
            ("observed_at".into(), self.observed_at.into()),
        ]))
    }
    fn from_record(_: Record) -> database::Result<Self> {
        unimplemented!("writes only")
    }
}

fn point(i: i64) -> Point {
    Point {
        device: format!("dev{}", i % 64),
        object: format!("analog-input:{}", i % 500),
        value: i as f64 * 0.1,
        observed_at: 1_800_000_000_000_000_000 + i,
    }
}

/// Batched span export and periodic metric export, as an application wires them.
fn exported() -> (Telemetry, SdkTracerProvider, SdkMeterProvider) {
    let traces = SdkTracerProvider::builder()
        .with_batch_exporter(InMemorySpanExporter::default())
        .build();
    let metrics = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(InMemoryMetricExporter::default()).build())
        .build();
    let signals = Telemetry::default()
        .with_tracer(traces.tracer("bench"))
        .with_meter(metrics.meter("bench"));
    (signals, traces, metrics)
}

async fn arrow(name: &str, signals: Telemetry) -> DB {
    let root = std::env::temp_dir().join(format!("tpp-rate-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let provider = ArrowProvider::new(root).with_telemetry(signals);
    provider.create_database("bms").await.unwrap();
    provider.set_database("bms").await.unwrap()
}

fn tuned() -> IngestConfig {
    IngestConfig {
        batch_size: 5_000,
        queue_capacity: 50_000,
        flush_interval: Duration::from_millis(250),
        ..IngestConfig::default()
    }
}

/// One generated `create` per point from `producers` tasks, then close.
async fn creates(label: &str, db: DB, producers: i64, check: &[&DB]) {
    let points = Arc::new(For::<Point>::new(&db));
    let started = Instant::now();
    let tasks: Vec<_> = (0..producers)
        .map(|p| {
            let points = points.clone();
            tokio::spawn(async move {
                for i in (p..POINTS).step_by(producers as usize) {
                    assert!(points.create(&point(i)).await.unwrap().pending);
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    db.close().await.unwrap();
    let rate = POINTS as f64 / started.elapsed().as_secs_f64();
    println!("{label:<52} {rate:>12.0} points/s");
    for side in check {
        let stored = side
            .count(&POINTS_RESOURCE, &ListOptions::default())
            .await
            .unwrap();
        assert_eq!(stored, POINTS, "{label}: every point stored");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "throughput measurement; run in release"]
async fn generated_create_rates() {
    let off = Telemetry::default;
    for producers in [1, 8] {
        let label = format!("create, buffered arrow, {producers} producer(s)");
        let store = arrow(&format!("b{producers}"), off()).await;
        creates(
            &label,
            buffered(store.clone(), tuned()).unwrap(),
            producers,
            &[&store],
        )
        .await;
    }
    let (a, b) = (arrow("ma", off()).await, arrow("mb", off()).await);
    let mirrored = buffered(mirror(a.clone(), b.clone()), tuned()).unwrap();
    creates(
        "create, buffered mirror(arrow, arrow), 8 producers",
        mirrored,
        8,
        &[&a, &b],
    )
    .await;

    let (signals, traces, metrics) = exported();
    let store = arrow("t", signals).await;
    let observed = buffered(store.clone(), tuned()).unwrap();
    creates(
        "create, buffered arrow, telemetry on, 8 producers",
        observed,
        8,
        &[&store],
    )
    .await;
    let _ = (traces.shutdown(), metrics.shutdown());

    let store = arrow("many", off()).await;
    let points = For::<Point>::new(&store);
    let started = Instant::now();
    for chunk in (0..POINTS).collect::<Vec<_>>().chunks(5_000) {
        let batch: Vec<Point> = chunk.iter().map(|&i| point(i)).collect();
        points.create_many(&batch).await.unwrap();
    }
    let rate = POINTS as f64 / started.elapsed().as_secs_f64();
    println!(
        "{:<52} {rate:>12.0} points/s",
        "create_many, 5,000-point batches"
    );
}
