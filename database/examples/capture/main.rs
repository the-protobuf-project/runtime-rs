//! Live capture into InfluxDB with every point also recorded to Arrow files.
//!
//! Generated-style producers call `create` once per point on
//! `buffered(mirror(influx, arrow))`. On Ctrl-C or after CAPTURE_SECONDS
//! (default 900, fifteen minutes) producers stop, `close` drains every queued
//! point into both stores, and the run is counted back from each store.
//! Without INFLUX_HOST the capture records to Arrow only.
mod feed;

use database::arrow::ArrowProvider;
use database::influxdb::{ClientConfig, InfluxAdmin, InfluxProvider, Retention};
use database::store::{DB, Error, For, ListOptions, Model, Provider};
use database::{IngestConfig, buffered, mirror};
use feed::Point;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn setting<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn influx(name: &str) -> Result<Option<DB>> {
    let Ok(mut config) = ClientConfig::from_env() else {
        return Ok(None);
    };
    let admin = InfluxAdmin::new(&config, Duration::from_secs(30))?;
    match admin.create_database(name, Retention::Infinite).await {
        Ok(()) | Err(Error::AlreadyExists) => {}
        Err(e) => return Err(e.into()),
    }
    config.database = name.into();
    Ok(Some(InfluxProvider::new(config).set_database(name).await?))
}

#[tokio::main]
async fn main() -> Result<()> {
    let root: String = setting("CAPTURE_DIR", "captures".to_owned());
    let name: String = setting("CAPTURE_DATABASE", "bms".to_owned());
    let (rate, seconds, lanes) = (
        setting("CAPTURE_RATE", 22_000u64),
        setting("CAPTURE_SECONDS", 900u64),
        4,
    );
    let recorder = ArrowProvider::new(&root);
    recorder.create_database(&name).await?;
    let recording = recorder.set_database(&name).await?;
    let primary = influx(&name).await?;
    let stores: Vec<DB> = primary.iter().cloned().chain([recording.clone()]).collect();
    let live = match primary {
        Some(influx) => mirror(influx, recording),
        None => recording,
    };
    let db = buffered(
        live,
        IngestConfig {
            batch_size: 5_000,
            queue_capacity: 100_000,
            flush_interval: Duration::from_millis(250),
            // Each durable InfluxDB write waits ~1 s for the WAL; 8 in flight
            // measured ~40k points/s against InfluxDB 3 Core.
            max_inflight: 8,
            ..IngestConfig::default()
        },
    )?;

    let run = format!("run{}", chrono::Utc::now().timestamp());
    let (stop, sent) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicU64::new(0)),
    );
    let producers: Vec<_> = (0..lanes)
        .map(|lane| {
            let (points, run, stop, sent) = (
                For::<Point>::new(&db),
                run.clone(),
                stop.clone(),
                sent.clone(),
            );
            tokio::spawn(
                async move { feed::run(points, run, (lane, lanes), rate, &stop, &sent).await },
            )
        })
        .collect();
    let stores_named: Vec<_> = stores.iter().map(|s| s.backend).collect();
    println!(
        "capturing {run} at ~{rate}/s into {stores_named:?} for {seconds}s; Ctrl-C stops early"
    );
    let started = Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    let mut report = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal?; break }
            () = &mut deadline => break,
            _ = report.tick() => {
                let n = sent.load(Ordering::Relaxed);
                println!("  {:>5.0}s {n:>10} points ({:.0}/s)", started.elapsed().as_secs_f64(), n as f64 / started.elapsed().as_secs_f64());
            }
        }
    }

    // Stop producing, then drain: nothing admitted is lost on a clean stop.
    stop.store(true, Ordering::Relaxed);
    for producer in producers {
        producer.await??;
    }
    db.close().await?;
    let (total, elapsed) = (
        sent.load(Ordering::Relaxed),
        started.elapsed().as_secs_f64(),
    );
    println!(
        "sent {total} points in {elapsed:.1}s ({:.0}/s)",
        total as f64 / elapsed
    );
    let this_run = ListOptions::default().filter(format!("run = \"{run}\""));
    for store in &stores {
        let stored = store.count(Point::resource(), &this_run).await?;
        let verdict = if stored == total as i64 {
            "every point recorded"
        } else {
            "MISSING POINTS"
        };
        println!("  {:<10} {stored:>10} stored: {verdict}", store.backend);
    }
    Ok(())
}
