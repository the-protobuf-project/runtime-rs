//! AIP-160 filtering, descending order and server-side time-bucket aggregation.
use super::{
    Result,
    model::{Reading, window},
};
use database::store::{AggregateOptions, DB, For, Model, Reducer, Reduction};
use std::time::Duration;

pub async fn run(db: &DB, start: i64) -> Result<()> {
    let readings = For::<Reading>::new(db);
    let mut range = window(start, start + 2);
    range.descending = true;
    range.filter = "value >= 20".into();
    let (rows, _) = readings.range(&range).await?;
    assert_eq!(rows[0].device, "pump");
    println!("3. Filtered value >= 20, newest first: {rows:?}");
    let buckets = db
        .series
        .aggregate(
            Reading::resource(),
            &AggregateOptions {
                start,
                end: start + 2,
                every: Duration::from_secs(60),
                reduce: vec![Reduction::new(Reducer::Avg, "value", "mean")],
                ..AggregateOptions::default()
            },
        )
        .await?;
    assert_eq!(buckets[0].values["mean"], 25.0);
    println!("4. Server-computed one-minute mean: {buckets:?}");
    Ok(())
}
