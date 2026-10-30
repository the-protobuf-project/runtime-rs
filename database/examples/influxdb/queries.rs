//! Typed filtering, descending order and server-side time-bucket aggregation.
use super::{Result, model::Reading};
use database::{
    Aggregate, Collection, Comparison, DB, Predicate, Range, Reduce, Reduction, Resource,
};

pub async fn run(db: &DB, resource: &Resource, start: i64) -> Result<()> {
    let readings = Collection::<Reading>::new(db.clone(), resource.clone());
    let mut range = Range::new(start, start + 2);
    range.descending = true;
    range.predicates.push(Predicate {
        column: "value".into(),
        comparison: Comparison::Ge,
        value: 20.into(),
    });
    let rows = readings.range(&range).await?;
    assert_eq!(rows[0].device, "pump");
    println!("3. Filtered value >= 20, newest first: {rows:?}");
    let buckets = db
        .series()?
        .aggregate(
            resource,
            &Aggregate {
                range: Range::new(start, start + 2),
                every_seconds: 60,
                reduce: vec![Reduce {
                    column: "value".into(),
                    function: Reduction::Mean,
                    alias: "mean".into(),
                }],
                group_by: vec![],
            },
        )
        .await?;
    assert_eq!(buckets[0]["mean"], 25.0);
    println!("4. Server-computed one-minute mean: {buckets:?}");
    Ok(())
}
