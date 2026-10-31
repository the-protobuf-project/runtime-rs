//! Opt-in real-server write rates: how batch size and in-flight batches trade
//! against InfluxDB's WAL flush, which every durable write waits for.
use database::influxdb::{ClientConfig, InfluxAdmin, InfluxProvider, Retention};
use database::store::{Column, DB, Kind, Provider, Record, Resource, Role};
use std::time::{Duration, Instant};

fn points() -> Resource {
    Resource::new(
        "rate",
        vec![
            Column::new("object", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
}

fn batch(base: i64, size: usize) -> Vec<Record> {
    (0..size as i64)
        .map(|i| {
            Record::from([
                ("object".into(), format!("ai:{}", i % 2_000).into()),
                ("value".into(), (i as f64).into()),
                ("observed_at".into(), (base + i).into()),
            ])
        })
        .collect()
}

async fn rate(db: &DB, size: usize, inflight: usize, rounds: usize) -> f64 {
    let base = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let started = Instant::now();
    let tasks: Vec<_> = (0..inflight)
        .map(|lane| {
            let db = db.clone();
            tokio::spawn(async move {
                for round in 0..rounds {
                    let offset = ((round * inflight + lane) * size) as i64;
                    db.batch
                        .create_many(&points(), &batch(base + offset, size))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    (size * inflight * rounds) as f64 / started.elapsed().as_secs_f64()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local InfluxDB credentials; creates and deletes its own database"]
async fn durable_write_rates_by_batch_size_and_inflight() {
    let mut config = ClientConfig::from_env().unwrap();
    let admin = InfluxAdmin::new(&config, Duration::from_secs(30)).unwrap();
    let name = format!("rust_rate_{}", chrono::Utc::now().timestamp());
    admin
        .create_database(&name, Retention::Infinite)
        .await
        .unwrap();
    config.database = name.clone();
    let db = InfluxProvider::new(config)
        .set_database(&name)
        .await
        .unwrap();
    for (size, inflight) in [(5_000, 1), (10_000, 1), (5_000, 4), (10_000, 4), (5_000, 8)] {
        let r = rate(&db, size, inflight, 3).await;
        println!("batch {size:>6}, {inflight} in flight: {r:>9.0} points/s");
    }
    admin.drop_database(&name).await.unwrap();
}
