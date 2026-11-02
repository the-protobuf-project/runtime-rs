//! A full read does work proportional to the table, not to the table times its pages. These
//! count batches read back from disk rather than time, so they cannot be flaky, and they hold
//! the old offset path to the same measure to show the count can see the difference.

use super::super::ArrowDriver;
use super::super::rotation::Rotation;
use crate::store::window::Range;
use crate::store::{Column, Kind, Record, Resource, Role};

const BATCHES: i64 = 100;
const ROWS: i64 = 1_000;
const PAGE: usize = 1_000;
/// One read per batch to index it, and about nine per page, each page here being one batch
/// out of a nine-batch block. The factor is a block over a page; it does not grow with the
/// table.
const LINEAR: u64 = BATCHES as u64 * 12;

fn resource() -> Resource {
    Resource::new(
        "points",
        vec![
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .expect("a valid resource")
}

async fn table(name: &str) -> ArrowDriver {
    let dir = std::env::temp_dir().join(format!("seek-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let driver = ArrowDriver::new(dir, false, Rotation::default());
    for batch in 0..BATCHES {
        let records: Vec<Record> = (0..ROWS)
            .map(|row| {
                let time = batch * ROWS + row;
                Record::from([
                    ("value".to_owned(), (time as f64).into()),
                    ("observed_at".to_owned(), time.into()),
                ])
            })
            .collect();
        driver.write(&resource(), &records).await.expect("written");
    }
    driver
}

fn window() -> Range {
    Range {
        limit: PAGE,
        ..Range::new(i64::MIN, i64::MAX)
    }
}

/// Reads every page from `token`, returning the rows seen and the pages it took.
async fn read_all(driver: &ArrowDriver, range: &Range, mut token: String) -> (i64, u64) {
    let (mut rows, mut pages) = (0, 0);
    loop {
        let page = driver
            .page(&resource(), range, &token, true)
            .await
            .expect("a page");
        rows += page.items.len() as i64;
        pages += 1;
        if page.next_page_token.is_empty() {
            return (rows, pages);
        }
        token = page.next_page_token;
    }
}

#[tokio::test]
async fn reading_a_table_by_pages_reads_each_batch_a_bounded_number_of_times() {
    let driver = table("linear").await;
    let (rows, pages) = read_all(&driver, &window(), String::new()).await;
    assert_eq!((rows, pages), (BATCHES * ROWS, BATCHES as u64));

    let reads = driver.batches_read();
    assert!(reads <= LINEAR, "{reads} batches read, bound {LINEAR}");
}

#[tokio::test]
async fn the_old_offset_tokens_still_work_and_are_the_quadratic_this_replaces() {
    let driver = table("offsets").await;
    let (rows, _) = read_all(&driver, &window(), "0".to_owned()).await;
    assert_eq!(rows, BATCHES * ROWS);

    // Every page decodes every batch.
    let reads = driver.batches_read();
    assert_eq!(reads, (BATCHES * BATCHES) as u64);
    assert!(reads > 5 * LINEAR);
}
