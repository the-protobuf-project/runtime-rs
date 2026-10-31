//! Verify that the database's creation-time retention hides expired points.
use super::{
    Result,
    model::{Reading, window},
};
use database::store::{DB, For};

pub async fn run(db: &DB, now: i64) -> Result<()> {
    let readings = For::<Reading>::new(db);
    let expired = now - 2 * 3600 * 1_000_000_000;
    readings
        .create(&Reading {
            device: "expired".into(),
            value: -1.0,
            observed_at: expired,
        })
        .await?;
    assert!(
        readings
            .range(&window(expired, expired + 1))
            .await?
            .0
            .is_empty()
    );
    println!("7. One-hour retention excludes a two-hour-old point");
    Ok(())
}
