//! Typed point writes, identity-preserving rewrites, and time-range reads.
use super::{Result, model::Reading};
use database::{Collection, DB, Range, Resource};

pub async fn run(db: &DB, resource: &Resource, start: i64) -> Result<()> {
    let readings = Collection::<Reading>::new(db.clone(), resource.clone());
    readings
        .write(&[
            Reading {
                device: "boiler".into(),
                value: 10.0,
                observed_at: start,
            },
            Reading {
                device: "pump".into(),
                value: 20.0,
                observed_at: start + 1,
            },
        ])
        .await?;
    // Same table + tag set + timestamp rewrites this point's value.
    readings
        .write(&[Reading {
            device: "boiler".into(),
            value: 30.0,
            observed_at: start,
        }])
        .await?;
    let rows = readings.range(&Range::new(start, start + 2)).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value, 30.0);
    println!("2. Wrote, rewrote and read typed points: {rows:?}");
    Ok(())
}
