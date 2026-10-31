//! Create an actual database from Rust, including its retention policy.
use super::Result;
use database::influxdb::{ClientConfig, InfluxAdmin, InfluxProvider, Retention};
use database::store::{DB, Provider};
use std::time::Duration;

pub async fn run(
    mut config: ClientConfig,
    name: &str,
    signals: telemetry::Telemetry,
) -> Result<(InfluxAdmin, DB)> {
    let admin = InfluxAdmin::new(&config, Duration::from_secs(30))?.with_telemetry(signals.clone());
    admin
        .create_database(name, Retention::For(Duration::from_secs(3600)))
        .await?;
    // No CLI database creation. Existing names return AlreadyExists rather
    // than changing retention or adopting a database this example doesn't own.
    config.database = name.into();
    let db = InfluxProvider::new(config)
        .with_telemetry(signals)
        .set_database(name)
        .await?;
    println!("1. Created and selected {name} with one-hour retention");
    Ok((admin, db))
}
