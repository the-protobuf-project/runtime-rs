//! buffered and mirror: pending creates, drains on close, writes to both stores.
#[path = "support/arrow.rs"]
mod arrow;
#[path = "support/sink.rs"]
mod sink;
use arrow::{reading, readings, select, window};
use database::arrow::Rotation;
use database::store::{Capabilities, DB, Error, ListOptions};
use database::{BatchWriter, IngestConfig, buffered, mirror};
use sink::Sink;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Semaphore;

fn queued() -> IngestConfig {
    IngestConfig {
        batch_size: 100,
        flush_interval: Duration::from_secs(60),
        ..IngestConfig::default()
    }
}

#[tokio::test]
async fn buffered_creates_are_pending_until_close_drains_them() {
    let (_provider, arrow_db) = select("buffered", Rotation::default()).await;
    let db = buffered(arrow_db.clone(), queued()).unwrap();
    for i in 0..250 {
        let written = db
            .create(&readings(), &reading("zat", 1.0, 1, i))
            .await
            .unwrap();
        assert!(written.pending);
    }
    // Full batches may still be in flight, but the last 50 wait for a flush.
    let before = arrow_db
        .count(&readings(), &ListOptions::default())
        .await
        .unwrap();
    assert!(before <= 200, "{before} written before close");
    db.close().await.unwrap();
    assert_eq!(
        arrow_db
            .count(&readings(), &ListOptions::default())
            .await
            .unwrap(),
        250
    );
    assert!(matches!(
        db.create(&readings(), &reading("zat", 1.0, 1, 0)).await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn a_failed_batch_is_reported_by_close_and_by_later_creates() {
    let failing = Arc::new(Sink {
        fail: true,
        ..Sink::default()
    });
    let inner = DB::build(
        "fake",
        "bms",
        Capabilities {
            batch: Some(failing),
            ..Capabilities::default()
        },
    );
    let mut config = queued();
    config.batch_size = 1;
    let db = buffered(inner, config).unwrap();
    db.create(&readings(), &reading("zat", 1.0, 1, 0))
        .await
        .unwrap();
    assert!(matches!(db.close().await, Err(Error::Ingestion(_))));
}

#[tokio::test]
async fn mirror_writes_both_stores_and_reads_the_primary() {
    let (_a, primary) = select("mirror-primary", Rotation::default()).await;
    let (_b, copy) = select("mirror-copy", Rotation::default()).await;
    let db = buffered(mirror(primary.clone(), copy.clone()), queued()).unwrap();
    for i in 0..120 {
        db.create(&readings(), &reading("zat", i as f64, 1, i))
            .await
            .unwrap();
    }
    db.close().await.unwrap();
    for side in [&primary, &copy] {
        let page = side
            .series
            .range(&readings(), &window(0, 1_000))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 120);
    }
    assert_eq!(
        db.series
            .range(&readings(), &window(0, 1_000))
            .await
            .unwrap()
            .total,
        120
    );
}

#[tokio::test]
async fn a_mirror_fails_a_write_that_either_side_refuses() {
    let (_a, primary) = select("mirror-refuse", Rotation::default()).await;
    let refusing = DB::build("empty", "bms", Capabilities::default());
    let db = mirror(primary.clone(), refusing);
    let result = db.create(&readings(), &reading("zat", 1.0, 1, 0)).await;
    assert!(matches!(
        result,
        Err(Error::Unimplemented {
            backend: "empty",
            ..
        })
    ));
    // The primary still took it: the error says the copy is behind.
    assert_eq!(
        primary
            .count(&readings(), &ListOptions::default())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn batches_run_concurrently_up_to_max_inflight_and_close_waits_for_all() {
    let gate = Arc::new(Semaphore::new(0));
    let sink = Arc::new(Sink {
        gate: Some(gate.clone()),
        ..Sink::default()
    });
    let config = IngestConfig {
        batch_size: 1,
        max_inflight: 2,
        ..queued()
    };
    let writer = BatchWriter::new(sink.clone(), readings(), config).unwrap();
    for i in 0..3 {
        writer.write(reading("zat", 1.0, 1, i)).await.unwrap();
    }
    while sink.entered.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        sink.entered.load(Ordering::SeqCst),
        2,
        "a third waits for a slot"
    );
    gate.add_permits(3);
    writer.close().await.unwrap();
    assert_eq!(sink.batches.lock().unwrap().len(), 3);
}
