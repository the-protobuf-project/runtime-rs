//! Arrow file captures: round trips, restarts, crash recovery and refusals.
#[path = "support/arrow_points.rs"]
mod points;
use database::IngestConfig;
use database::arrow::{ArrowProvider, load, segments};
use database::store::{
    AggregateOptions, Column, Error, Kind, ListOptions, Provider, Resource, Role,
};
use points::{open, point, points, root, window};

#[tokio::test]
async fn writes_survive_restarts_and_read_back_in_time_order() {
    let provider = ArrowProvider::new(root("restart"));
    let first = open(&provider).await;
    first
        .create_many(&[point("zat", 21.0, 30), point("sat", 14.0, 10)])
        .await
        .unwrap();
    first.db().close().await.unwrap();
    let second = open(&provider).await;
    second.create(&point("zat", 35.0, 20)).await.unwrap();

    let (rows, next) = second.range(&window(0, 100)).await.unwrap();
    assert!(next.is_empty());
    let times: Vec<i64> = rows.iter().map(|p| p.observed_at).collect();
    assert_eq!(times, [10, 20, 30]);
    assert!(rows[1].alarm);

    let files = segments(&provider.table_dir("bms", "points")).unwrap();
    assert_eq!(files.len(), 2, "each selection starts its own segment");
    let loaded = load(&files[0]).unwrap();
    assert!(loaded.complete);
    assert_eq!(loaded.resource.table(), "points");
    assert_eq!(loaded.records.len(), 2);
}

#[tokio::test]
async fn a_truncated_final_batch_keeps_the_rows_before_it() {
    let provider = ArrowProvider::new(root("crash"));
    let points_db = open(&provider).await;
    points_db.create(&point("zat", 21.0, 1)).await.unwrap();
    points_db.create(&point("zat", 22.0, 2)).await.unwrap();
    points_db.db().close().await.unwrap();

    let path = segments(&provider.table_dir("bms", "points"))
        .unwrap()
        .remove(0);
    let bytes = std::fs::read(&path).unwrap();
    // Drop the end-of-stream marker and part of the second batch.
    std::fs::write(&path, &bytes[..bytes.len() - 40]).unwrap();
    let loaded = load(&path).unwrap();
    assert!(!loaded.complete);
    assert_eq!(loaded.records.len(), 1);
    assert_eq!(loaded.records[0]["observed_at"], 1);
}

#[tokio::test]
async fn the_batch_writer_drains_into_a_segment() {
    let provider = ArrowProvider::new(root("ingest"));
    let points_db = open(&provider).await;
    let writer = points_db.batch_writer(IngestConfig::default()).unwrap();
    for i in 0..250 {
        writer.write(&point("zat", 21.0, i)).await.unwrap();
    }
    writer.close().await.unwrap();
    assert_eq!(points_db.count(&ListOptions::default()).await.unwrap(), 250);
}

#[tokio::test]
async fn refuses_keys_missing_databases_mismatched_schemas_and_bad_aggregations() {
    let provider = ArrowProvider::new(root("refuse"));
    assert!(matches!(
        provider.set_database("bms").await,
        Err(Error::Invalid(_))
    ));
    let points_db = open(&provider).await;
    points_db.create(&point("zat", 21.0, 1)).await.unwrap();
    assert!(matches!(
        points_db.get("k").await,
        Err(Error::Unimplemented {
            backend: "arrow",
            ..
        })
    ));

    let narrower = Resource::new(
        "points",
        vec![
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap();
    let db = points_db.db();
    assert!(matches!(
        db.series.range(&narrower, &window(0, 10)).await,
        Err(Error::Invalid(_))
    ));
    let empty = AggregateOptions {
        end: 10,
        every: std::time::Duration::from_secs(60),
        ..AggregateOptions::default()
    };
    assert!(matches!(
        db.series.aggregate(&points(), &empty).await,
        Err(Error::Invalid(_))
    ));

    assert!(db.schema.has_schema(&points()).await.unwrap());
    db.schema.drop_schema(&points()).await.unwrap();
    assert!(!db.schema.has_schema(&points()).await.unwrap());
    assert_eq!(points_db.count(&ListOptions::default()).await.unwrap(), 0);
}
