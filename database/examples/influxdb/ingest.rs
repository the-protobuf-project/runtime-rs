//! Bounded background ingestion, flush/drain, keyset pages and streamed scans.
use super::{
    Result,
    model::{Reading, window},
};
use database::IngestConfig;
use database::store::{DB, For, Scan};
use futures::TryStreamExt;

pub async fn run(db: &DB, start: i64) -> Result<()> {
    let readings = For::<Reading>::new(db);
    let writer = readings.batch_writer(IngestConfig {
        batch_size: 3,
        queue_capacity: 2,
        ..IngestConfig::default()
    })?;
    for i in 0..10 {
        writer
            .write(&Reading {
                device: "sensor".into(),
                value: i as f64,
                observed_at: start + 100 + i,
            })
            .await?;
    }
    writer.flush().await?; // Confirms server acknowledgement, not just admission.
    writer.close().await?; // Stops admission and drains remaining points.
    let mut range = window(start + 100, start + 110);
    range.page_size = 3;
    let mut count = 0;
    loop {
        let (page, next) = readings.range(&range).await?;
        count += page.len();
        println!("5. Ingested points, next keyset page: {page:?}");
        range.page_token = next;
        if range.page_token.is_empty() {
            break;
        }
    }
    assert_eq!(count, 10);
    let mut stream = readings
        .stream(&Scan::new(start + 100, start + 110))
        .await?;
    let mut count = 0;
    while let Some(row) = stream.try_next().await? {
        println!("   Streamed: {row:?}");
        count += 1;
    }
    assert_eq!(count, 10);
    println!("6. Streamed every ingested observation");
    Ok(())
}
