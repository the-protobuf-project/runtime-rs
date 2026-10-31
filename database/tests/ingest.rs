//! Admission, timer/count flushing, terminal errors and cancellation-safe drain.
mod common;
#[path = "support/sink.rs"]
mod sink;
use common::{Reading, record, resource};
use database::{BatchWriter, Error, IngestConfig};
use sink::Sink;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::timeout};

#[tokio::test]
async fn count_manual_and_close_flushes_cover_all_admitted_records() {
    let sink = Arc::new(Sink::default());
    let writer = BatchWriter::new(
        sink.clone(),
        resource(),
        IngestConfig {
            batch_size: 2,
            ..IngestConfig::default()
        },
    )
    .unwrap();
    let _: Reading = serde_json::from_value(serde_json::to_value(record()).unwrap()).unwrap();
    for _ in 0..3 {
        writer.write(record()).await.unwrap();
    }
    writer.flush().await.unwrap();
    assert_eq!(
        sink.batches
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    writer.write(record()).await.unwrap();
    writer.close().await.unwrap();
    assert_eq!(
        sink.batches
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        4
    );
}

#[tokio::test(start_paused = true)]
async fn interval_flushes_partial_batches_without_new_input() {
    let sink = Arc::new(Sink::default());
    let writer = BatchWriter::new(
        sink.clone(),
        resource(),
        IngestConfig {
            flush_interval: Duration::from_secs(2),
            ..IngestConfig::default()
        },
    )
    .unwrap();
    writer.write(record()).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    timeout(Duration::from_secs(1), sink.committed.notified())
        .await
        .unwrap();
    assert_eq!(sink.batches.lock().unwrap().len(), 1);
    writer.close().await.unwrap();
}

#[tokio::test]
async fn a_full_queue_applies_backpressure_and_cancelled_send_is_not_admitted() {
    let gate = Arc::new(Semaphore::new(0));
    let sink = Arc::new(Sink {
        gate: Some(gate.clone()),
        ..Sink::default()
    });
    let writer = BatchWriter::new(
        sink.clone(),
        resource(),
        IngestConfig {
            batch_size: 1,
            queue_capacity: 1,
            ..IngestConfig::default()
        },
    )
    .unwrap();
    writer.write(record()).await.unwrap();
    sink.started.notified().await;
    writer.write(record()).await.unwrap();
    assert!(
        timeout(Duration::from_millis(20), writer.write(record()))
            .await
            .is_err()
    );
    gate.add_permits(2);
    writer.close().await.unwrap();
    assert_eq!(sink.batches.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn failures_reach_both_flush_and_close_and_stop_admission() {
    let sink = Arc::new(Sink {
        fail: true,
        ..Sink::default()
    });
    let writer = BatchWriter::new(sink, resource(), IngestConfig::default()).unwrap();
    writer.write(record()).await.unwrap();
    let error = writer.flush().await.unwrap_err();
    assert!(error.to_string().contains("injected write failure"));
    assert!(writer.write(record()).await.is_err());
    assert!(matches!(writer.close().await, Err(Error::Ingestion(_))));
}

#[tokio::test]
async fn cancelling_close_does_not_abort_an_admitted_write() {
    let gate = Arc::new(Semaphore::new(0));
    let sink = Arc::new(Sink {
        gate: Some(gate.clone()),
        ..Sink::default()
    });
    let writer = BatchWriter::new(sink.clone(), resource(), IngestConfig::default()).unwrap();
    writer.write(record()).await.unwrap();
    let close = tokio::spawn(writer.close());
    sink.started.notified().await;
    close.abort();
    let _ = close.await;
    gate.add_permits(1);
    timeout(Duration::from_secs(1), sink.committed.notified())
        .await
        .unwrap();
    assert_eq!(sink.batches.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oversize_and_invalid_records_fail_before_admission() {
    let sink = Arc::new(Sink::default());
    let writer = BatchWriter::new(
        sink.clone(),
        resource(),
        IngestConfig {
            max_record_bytes: 1,
            ..IngestConfig::default()
        },
    )
    .unwrap();
    assert!(writer.write(record()).await.is_err());
    writer.close().await.unwrap();
    assert!(sink.batches.lock().unwrap().is_empty());
    assert!(
        BatchWriter::new(
            sink,
            resource(),
            IngestConfig {
                queue_capacity: 0,
                ..IngestConfig::default()
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn an_unresponsive_backend_is_bounded_and_the_failure_reaches_close() {
    let sink = Arc::new(Sink {
        gate: Some(Arc::new(Semaphore::new(0))),
        ..Sink::default()
    });
    let writer = BatchWriter::new(
        sink,
        resource(),
        IngestConfig {
            write_timeout: Duration::from_millis(20),
            ..IngestConfig::default()
        },
    )
    .unwrap();
    writer.write(record()).await.unwrap();
    let error = writer.close().await.unwrap_err();
    assert!(matches!(error, Error::Ingestion(source) if matches!(source.as_ref(), Error::Timeout)));
}
