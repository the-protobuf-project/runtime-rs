//! Bounded background ingestion, flush/drain, keyset pages and streamed scans.
use super::{Result, model::Reading};
use database::{Collection, DB, IngestConfig, Range, Resource, Scan};
use futures::TryStreamExt;

pub async fn run(db: &DB, resource: &Resource, start: i64) -> Result<()> {
    let readings = Collection::<Reading>::new(db.clone(), resource.clone());
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
    let mut window = Range::new(start + 100, start + 110);
    window.limit = 3;
    let mut token = None;
    let mut count = 0;
    loop {
        let page = readings.page(&window, token.as_deref()).await?;
        count += page.items.len();
        println!("5. Ingested points, next keyset page: {:?}", page.items);
        token = page.next_page_token;
        if token.is_none() {
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
