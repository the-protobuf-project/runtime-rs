//! Generated-style point writes, identity-preserving rewrites, and windows.
use super::{
    Result,
    model::{Reading, window},
};
use database::store::{DB, For};

pub async fn run(db: &DB, start: i64) -> Result<()> {
    let readings = For::<Reading>::new(db);
    let point = |device: &str, value, observed_at| Reading {
        device: device.into(),
        value,
        observed_at,
    };
    readings
        .create_many(&[point("boiler", 10.0, start), point("pump", 20.0, start + 1)])
        .await?;
    // Same table + tag set + timestamp rewrites this point's value.
    readings.create(&point("boiler", 30.0, start)).await?;
    let (rows, _) = readings.range(&window(start, start + 2)).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value, 30.0);
    println!("2. Wrote, rewrote and read typed points: {rows:?}");
    Ok(())
}
