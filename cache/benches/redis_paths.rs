//! Wall-clock timings of the cache paths that talk to Redis, against a live server.
//!
//! Not a criterion suite: the cache crate has none, and these paths are network-bound, so a
//! median of repeated runs with `std::time` is what says whether a change saved round trips.
//! Start the service as for the live tests, then:
//!
//! ```text
//! cargo bench -p tpp-cache --features live-tests --bench redis_paths
//! ```
//!
//! `RUNTIME_CACHE_REDIS_ADDRESS` defaults to `127.0.0.1:16379`; `RUNTIME_CACHE_BENCH_RUNS`
//! (default 7) sets the repetitions. Each run writes under a fresh UUID prefix and deletes
//! what it wrote.

use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use cache::{
    Config, Provider,
    core::{DB, Loader, Options},
    drivers::redis::{RedisClient, RedisConfig, RedisProvider},
};
use futures::FutureExt;

/// Documents behind one `list`: enough for many 256-key bulk batches.
const DOCUMENTS: usize = 10_000;
/// Sequential single-key operations per timed sample.
const OPERATIONS: usize = 5_000;

#[tokio::main]
async fn main() -> cache::Result<()> {
    let address = env::var("RUNTIME_CACHE_REDIS_ADDRESS").unwrap_or("127.0.0.1:16379".into());
    let runs: usize = env::var("RUNTIME_CACHE_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);
    let client = Arc::new(
        RedisClient::connect(RedisConfig {
            address,
            ..RedisConfig::default()
        })
        .await?,
    );
    let config = Config {
        prefix: format!("bench-{}", uuid::Uuid::new_v4().simple()),
        ..Config::default()
    };
    let provider = RedisProvider::new(client.clone(), config);
    let db = provider.set_database("bench").await?;

    seed(&db).await?;
    report("document list, 10k docs", runs, 1, || list(&db)).await?;
    report("volatile touch(0), per op", runs, OPERATIONS, || touch(&db)).await?;
    report("aside miss+load, per op", runs, OPERATIONS, || aside(&db)).await?;

    provider.drop_database("bench").await?;
    db.close().await?;
    client.close().await;
    Ok(())
}

/// Times `runs` samples of `work`, each `per` operations, and prints the median per op.
async fn report<F, Fut>(name: &str, runs: usize, per: usize, mut work: F) -> cache::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = cache::Result<()>>,
{
    work().await?; // warm: connection, script cache, allocator
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        work().await?;
        samples.push(start.elapsed() / per as u32);
    }
    samples.sort();
    let median = samples[samples.len() / 2];
    println!(
        "{name:<28} median {:>10.1?}  min {:>10.1?}  max {:>10.1?}",
        median,
        samples[0],
        samples[samples.len() - 1]
    );
    Ok(())
}

async fn seed(db: &DB) -> cache::Result<()> {
    let opts = Options::default().with_ttl(Duration::from_secs(600));
    for i in 0..DOCUMENTS {
        let id = format!("doc-{i:05}");
        db.document
            .create(b"a small document body", &opts.clone().with_id(&id))
            .await?;
    }
    for i in 0..OPERATIONS {
        db.volatile.set(&format!("vol-{i}"), b"v", &opts).await?;
    }
    Ok(())
}

async fn list(db: &DB) -> cache::Result<()> {
    let bodies = db.document.list().await?;
    assert_eq!(bodies.len(), DOCUMENTS);
    Ok(())
}

async fn touch(db: &DB) -> cache::Result<()> {
    for i in 0..OPERATIONS {
        db.volatile
            .touch(&format!("vol-{i}"), Duration::ZERO)
            .await?;
    }
    Ok(())
}

async fn aside(db: &DB) -> cache::Result<()> {
    // Aside values are JSON documents.
    let loader: Loader =
        Arc::new(|id: String| async move { Ok(format!("{id:?}").into_bytes()) }.boxed());
    let aside = db.aside(loader);
    let opts = Options::default().with_ttl(Duration::from_secs(600));
    for i in 0..OPERATIONS {
        let id = format!("aside-{}", uuid::Uuid::new_v4().simple());
        let mut out = Vec::new();
        aside.get_or_load(&id, &mut out, &opts).await?;
        if i == 0 {
            assert_eq!(out, format!("{id:?}").as_bytes());
        }
    }
    Ok(())
}
