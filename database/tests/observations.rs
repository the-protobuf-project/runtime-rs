//! Real exported observations preserve capabilities, results and cancellation.
#[path = "support/observed_backend.rs"]
mod backend;
#[path = "support/observability.rs"]
mod capture;
mod common;
use backend::Backend;
use capture::Harness;
use common::{record, resource};
use database::store::{
    AggregateOptions, Capabilities, DB, ListOptions, RangeOptions, Scan, instrument,
};
use futures::TryStreamExt;
use opentelemetry_sdk::trace::SpanData;
use std::sync::{Arc, atomic::Ordering};

fn spans(h: &Harness) -> Vec<SpanData> {
    h.traces.get_finished_spans().unwrap()
}
fn db(backend: Arc<Backend>) -> DB {
    DB::build(
        "fake",
        "private_tenant",
        Capabilities {
            driver: Some(backend.clone()),
            batch: Some(backend.clone()),
            series: Some(backend.clone()),
            schema: Some(backend.clone()),
            streams: Some(backend),
            ..Capabilities::default()
        },
    )
}

#[tokio::test]
async fn rewiring_preserves_all_capabilities_and_emits_once_without_sensitive_data() {
    let h = Harness::new();
    let backend = Arc::new(Backend::new(0));
    let db = instrument(
        instrument(db(backend.clone()), h.signals.clone()),
        h.signals.clone(),
    )
    .clone();
    let res = resource();
    let _: common::Reading =
        serde_json::from_value(serde_json::to_value(record()).unwrap()).unwrap();
    let range = RangeOptions {
        end: 2_000_000_000,
        page_token: "secret-token".into(),
        filter: "device = \"boiler\"".into(),
        ..RangeOptions::default()
    };
    db.batch.create_many(&res, &[record()]).await.unwrap();
    db.series.range(&res, &range).await.unwrap();
    let aggregate = AggregateOptions::default();
    db.series.aggregate(&res, &aggregate).await.unwrap();
    db.create(&res, &record()).await.unwrap();
    db.get(&res, "secret-key").await.unwrap();
    db.update(&res, &record()).await.unwrap();
    db.delete(&res, "secret-key").await.unwrap();
    db.exists(&res, "secret-key").await.unwrap();
    db.list(&res, &ListOptions::default()).await.unwrap();
    db.streams
        .stream(&res, &Scan::new(0, 2_000_000_000))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    db.schema.drop_schema(&res).await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 11);
    assert_eq!(spans(&h).len(), 11);
    assert_eq!(h.logs.get_emitted_logs().unwrap().len(), 11);
    let captured = format!("{:?}{:?}", spans(&h), h.logs.get_emitted_logs().unwrap());
    for secret in [
        "private_tenant",
        "secret-key",
        "secret-token",
        "secret-cursor",
        "boiler",
    ] {
        assert!(!captured.contains(secret));
    }
    for point in h.measurements() {
        assert!(
            point
                .attributes
                .keys()
                .all(|k| ["component", "backend", "operation", "outcome"].contains(&k.as_str()))
        );
        if point.name == "runtime.operation.active" {
            assert_eq!(point.value, 0.0);
        }
    }
}

#[tokio::test]
async fn stream_errors_and_early_drop_finish_lifetimes_and_count_delivered_rows() {
    for mode in [1, 2] {
        let h = Harness::new();
        let db = instrument(db(Arc::new(Backend::new(mode))), h.signals.clone());
        let mut stream = db
            .streams
            .stream(&resource(), &Scan::new(0, 2_000_000_000))
            .await
            .unwrap();
        assert!(
            spans(&h).is_empty(),
            "opening a stream must not mark it completed"
        );
        assert!(stream.try_next().await.unwrap().is_some());
        if mode == 2 {
            assert!(stream.try_next().await.is_err());
        }
        drop(stream);
        let completed = spans(&h);
        assert_eq!(completed.len(), 1);
        let outcome = if mode == 1 { "cancelled" } else { "timeout" };
        assert!(
            completed[0]
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == "outcome" && kv.value.to_string() == outcome)
        );
        let points = h.measurements();
        assert!(
            points
                .iter()
                .any(|m| m.name == "runtime.records.read" && m.value == 1.0)
        );
        assert!(
            points
                .iter()
                .filter(|m| m.name == "runtime.operation.active")
                .all(|m| m.value == 0.0)
        );
    }
}

#[tokio::test]
async fn backend_errors_are_returned_intact_but_never_exported_as_raw_messages() {
    let h = Harness::new();
    let db = instrument(db(Arc::new(Backend::new(3))), h.signals.clone());
    let error = db
        .batch
        .create_many(&resource(), &[record()])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("secret-token"));
    let exported = format!("{:?}{:?}", spans(&h), h.logs.get_emitted_logs().unwrap());
    assert!(!exported.contains("secret-token"));
    assert!(
        h.measurements()
            .iter()
            .any(|m| m.attributes.get("outcome").is_some_and(|v| v == "invalid"))
    );
}

#[path = "support/observation_ingest.rs"]
mod ingestion_tests;

#[path = "support/sink.rs"]
mod sink;
