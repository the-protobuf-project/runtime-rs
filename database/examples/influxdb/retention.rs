//! Verify that the database's creation-time retention hides expired points.
use super::{Result, model::Reading};
use database::{Collection, DB, Range, Resource};

pub async fn run(db: &DB, resource: &Resource, now: i64) -> Result<()> {
    let readings = Collection::<Reading>::new(db.clone(), resource.clone());
    let expired = now - 2 * 3600 * 1_000_000_000;
    readings
        .write(&[Reading {
            device: "expired".into(),
            value: -1.0,
            observed_at: expired,
        }])
        .await?;
    assert!(
        readings
            .range(&Range::new(expired, expired + 1))
            .await?
            .is_empty()
    );
    println!("7. One-hour retention excludes a two-hour-old point");
    Ok(())
}
