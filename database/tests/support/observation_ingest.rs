//! Background spans link to producers without leaking task-local context.
use super::{Backend, Harness, db, record, resource, spans};
use database::{IngestConfig, instrument};
use std::{sync::Arc, time::Duration};
use telemetry::opentelemetry::{
    Context,
    trace::{FutureExt, TraceContextExt, Tracer, TracerProvider},
};

#[tokio::test]
async fn ingestion_links_producers_to_batches_and_keeps_queue_depth_balanced() {
    let h = Harness::new();
    let db = instrument(db(Arc::new(Backend::new(0))), h.signals.clone());
    let writer = db
        .batch_writer(
            resource(),
            IngestConfig {
                batch_size: 2,
                queue_capacity: 1,
                flush_interval: Duration::from_secs(60),
                ..IngestConfig::default()
            },
        )
        .unwrap();
    let parent = Context::current_with_span(h.trace_provider.tracer("app").start("request"));
    writer
        .write(record())
        .with_context(parent.clone())
        .await
        .unwrap();
    writer
        .write(record())
        .with_context(parent.clone())
        .await
        .unwrap();
    writer.close().await.unwrap();
    let completed = spans(&h);
    let enqueue = completed
        .iter()
        .filter(|s| s.name == "database_ingestion.enqueue")
        .collect::<Vec<_>>();
    assert_eq!(enqueue.len(), 2);
    assert!(
        enqueue
            .iter()
            .all(|s| s.parent_span_id == parent.span().span_context().span_id())
    );
    let batch = completed
        .iter()
        .find(|s| s.name == "database_ingestion.batch_write")
        .unwrap();
    assert_eq!(batch.links.links.len(), 2);
    let write = completed
        .iter()
        .find(|s| s.name == "database.write")
        .unwrap();
    assert_eq!(write.parent_span_id, batch.span_context.span_id());
    let points = h.measurements();
    assert!(
        points
            .iter()
            .any(|m| m.name == "runtime.ingestion.queue.depth" && m.value == 0.0)
    );
    assert!(
        points
            .iter()
            .any(|m| m.name == "runtime.records.written" && m.value == 2.0)
    );
    parent.span().end();
}

#[tokio::test]
async fn worker_failure_releases_queue_leases_for_unprocessed_records() {
    use super::sink::Sink;
    use database::{Capabilities, DB};
    use tokio::sync::Semaphore;
    let h = Harness::new();
    let gate = Arc::new(Semaphore::new(0));
    let backend = Arc::new(Sink {
        gate: Some(gate.clone()),
        fail: true,
        ..Sink::default()
    });
    let db = instrument(
        DB::new(
            "fake",
            "private".into(),
            Capabilities {
                series: Some(backend.clone()),
                ..Capabilities::default()
            },
        ),
        h.signals.clone(),
    );
    let writer = db
        .batch_writer(
            resource(),
            IngestConfig {
                batch_size: 1,
                queue_capacity: 4,
                ..IngestConfig::default()
            },
        )
        .unwrap();
    writer.write(record()).await.unwrap();
    backend.started.notified().await;
    writer.write(record()).await.unwrap();
    writer.write(record()).await.unwrap();
    gate.add_permits(1);
    assert!(writer.close().await.is_err());
    let points = h.measurements();
    assert!(
        points
            .iter()
            .any(|m| m.name == "runtime.ingestion.queue.depth" && m.value == 0.0)
    );
    assert!(
        !points
            .iter()
            .any(|m| m.name == "runtime.records.written" && m.value > 0.0)
    );
    assert!(
        points
            .iter()
            .filter(|m| m.name == "runtime.operation.active")
            .all(|m| m.value == 0.0)
    );
}
