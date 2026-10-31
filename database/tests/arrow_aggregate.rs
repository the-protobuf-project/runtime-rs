//! Arrow aggregation: buckets, groups and reductions with InfluxDB semantics.
#[path = "support/arrow.rs"]
mod arrow;
use arrow::{SECOND, reading, readings, select};
use database::arrow::Rotation;
use database::store::{AggregateOptions, Reducer, Reduction};
use std::time::Duration;

#[tokio::test]
async fn aggregation_buckets_groups_and_reduces_like_influxdb() {
    let (_provider, db) = select("aggregate", Rotation::default()).await;
    let series = &db.series;
    let rows = [
        reading("zat", 20.0, 2, 0),
        reading("zat", 22.0, 4, 30 * SECOND),
        reading("sat", 14.0, 1, 59 * SECOND),
        reading("zat", 30.0, 6, 61 * SECOND),
        reading("zat", 99.0, 9, 125 * SECOND), // outside the window
    ];
    db.batch.create_many(&readings(), &rows).await.unwrap();

    let query = AggregateOptions {
        start: 0,
        end: 120 * SECOND,
        every: Duration::from_secs(60),
        filter: "value < 50".into(),
        reduce: vec![
            Reduction::new(Reducer::Avg, "value", "mean"),
            Reduction::new(Reducer::Max, "value", "peak"),
            Reduction::new(Reducer::Sum, "pulses", "pulses"),
            Reduction::new(Reducer::Count, "value", "n"),
        ],
        group_by: vec!["object".into()],
        ..AggregateOptions::default()
    };
    let buckets = series.aggregate(&readings(), &query).await.unwrap();
    let summary: Vec<(i64, &str, f64, f64, f64, f64)> = buckets
        .iter()
        .map(|b| {
            (
                b.start / SECOND,
                b.group["object"].as_str(),
                b.values["mean"],
                b.values["peak"],
                b.values["pulses"],
                b.values["n"],
            )
        })
        .collect();
    // Oldest bucket first, then tags ascending, as runtime-go's timescale.
    assert_eq!(
        summary,
        [
            (0, "sat", 14.0, 14.0, 1.0, 1.0),
            (0, "zat", 21.0, 22.0, 6.0, 2.0),
            (60, "zat", 30.0, 30.0, 6.0, 1.0),
        ]
    );

    let mut bad = query.clone();
    bad.group_by = vec!["value".into()];
    assert!(series.aggregate(&readings(), &bad).await.is_err());
}
