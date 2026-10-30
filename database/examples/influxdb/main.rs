//! Run every InfluxDB example in order against one disposable database.
//! Creates the database from Rust, demonstrates the workflow, then removes it.
mod cleanup;
mod ingest;
mod model;
mod provision;
mod queries;
mod records;
mod retention;

use database::drivers::influxdb::ClientConfig;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ClientConfig::from_env()?;
    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or("time out of range")?;
    let name = format!("runtime_example_{now}");
    let start = now - now.rem_euclid(60_000_000_000);
    let resource = model::resource()?;
    let (admin, db) = provision::run(config, &name).await?;
    let result: Result<()> = async {
        records::run(&db, &resource, start).await?;
        queries::run(&db, &resource, start).await?;
        ingest::run(&db, &resource, start).await?;
        retention::run(&db, &resource, now).await?;
        cleanup::run(&db, &resource).await
    }
    .await;
    // Only this run's newly created database is removed, also on stage failure.
    let cleanup = admin.drop_database(&name).await;
    result?;
    cleanup?;
    println!("8. Removed disposable database {name}; InfluxDB stays running");
    Ok(())
}
