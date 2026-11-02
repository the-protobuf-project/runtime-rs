//! Paging by key returns exactly what sorting the whole table would, in the orders that
//! are hard for it: out of time order, tied, across segments, filtered, growing underneath.
//!
//! The timing test is opt-in and meaningful only in release:
//! `cargo test --release -p tpp-database --test arrow_paging -- --ignored --nocapture`
#[path = "support/arrow_points.rs"]
mod points;
use database::arrow::{ArrowProvider, Rotation};
use database::store::{Error, For, ListOptions, RangeOptions};
use points::{Point, open, point, root, window};
use std::time::{Duration, Instant};

/// A shuffle with no dependency: multiplication by a value coprime to `n` permutes 0..n.
fn scrambled(n: i64) -> impl Iterator<Item = i64> {
    (0..n).map(move |i| (i * 7_919) % n)
}

/// Pages through `options`, returning each row's value, which identifies it.
async fn read(db: &For<Point>, mut options: RangeOptions) -> Vec<f64> {
    let mut values = Vec::new();
    loop {
        let page = db.db().series.range(db.resource(), &options).await.unwrap();
        values.extend(page.items.iter().map(|r| r["value"].as_f64().unwrap()));
        if page.next_page_token.is_empty() {
            return values;
        }
        options.page_token = page.next_page_token;
    }
}

fn paged(page_size: i32, descending: bool) -> RangeOptions {
    RangeOptions {
        page_size,
        descending,
        ..window(0, 1_000_000)
    }
}

/// Tied times every ten rows, out of order, written in small batches across segments. Each
/// row's value is its write order, so a stable sort of the writes is the expected answer.
async fn disordered(name: &str) -> (For<Point>, Vec<(i64, f64)>) {
    let provider = ArrowProvider::new(root(name)).with_rotation(Rotation {
        max_bytes: Some(4_096),
        max_age: None,
    });
    let db = open(&provider).await;
    let rows: Vec<(i64, f64)> = scrambled(1_000)
        .enumerate()
        .map(|(written, time)| (time / 10, written as f64))
        .collect();
    for batch in rows.chunks(37) {
        let points: Vec<Point> = batch
            .iter()
            .map(|&(time, value)| point("a", value, time))
            .collect();
        db.create_many(&points).await.unwrap();
    }
    (db, rows)
}

#[tokio::test]
async fn pages_are_a_stable_sort_of_disordered_ties_across_segments() {
    let (db, rows) = disordered("disorder").await;
    let mut ascending = rows.clone();
    ascending.sort_by_key(|&(time, _)| time);
    let mut descending = rows.clone();
    descending.sort_by_key(|&(time, _)| std::cmp::Reverse(time));
    let values = |rows: &[(i64, f64)]| rows.iter().map(|r| r.1).collect::<Vec<_>>();

    for size in [1, 7, 100, 10_000] {
        assert_eq!(read(&db, paged(size, false)).await, values(&ascending));
        assert_eq!(read(&db, paged(size, true)).await, values(&descending));
    }
}

#[tokio::test]
async fn a_filter_and_a_window_page_the_same_rows_a_sort_would() {
    let (db, rows) = disordered("filtered").await;
    let mut expected: Vec<(i64, f64)> = rows
        .into_iter()
        .filter(|&(time, value)| (20..80).contains(&time) && value < 500.0)
        .collect();
    expected.sort_by_key(|&(time, _)| time);
    let expected: Vec<f64> = expected.into_iter().map(|r| r.1).collect();

    let mut options = paged(9, false);
    options.start = 20;
    options.end = 80;
    options.filter = "value < 500".into();
    assert_eq!(read(&db, options.clone()).await, expected);

    let page = db.db().series.range(db.resource(), &options).await.unwrap();
    assert_eq!(page.total, expected.len() as i64, "the first page counts");
    options.page_token = page.next_page_token;
    let page = db.db().series.range(db.resource(), &options).await.unwrap();
    assert_eq!(page.total, expected.len() as i64, "later pages carry it");
}

#[tokio::test]
async fn rows_appended_between_pages_are_reached_and_the_total_is_the_first_pages() {
    let db = open(&ArrowProvider::new(root("growing"))).await;
    let batch: Vec<Point> = (0..4).map(|i| point("a", i as f64, i)).collect();
    db.create_many(&batch).await.unwrap();

    let mut options = paged(3, false);
    let first = db.db().series.range(db.resource(), &options).await.unwrap();
    assert_eq!(first.total, 4);
    let batch: Vec<Point> = (4..8).map(|i| point("a", i as f64, i)).collect();
    db.create_many(&batch).await.unwrap();

    options.page_token = first.next_page_token;
    let second = db.db().series.range(db.resource(), &options).await.unwrap();
    assert_eq!(second.total, 4, "the count of the first page");
    assert_eq!(second.items.len(), 3);
    options.page_token = second.next_page_token;
    let third = db.db().series.range(db.resource(), &options).await.unwrap();
    assert_eq!(third.items.len(), 2, "the appended rows are still reached");
    assert!(third.next_page_token.is_empty());
}

#[tokio::test]
async fn a_token_works_only_on_the_query_it_came_from() {
    let (db, _) = disordered("scoped").await;
    let mut options = paged(5, false);
    let page = db.db().series.range(db.resource(), &options).await.unwrap();
    options.page_token = page.next_page_token;
    options.descending = true;
    assert!(matches!(
        db.db().series.range(db.resource(), &options).await,
        Err(Error::Invalid(_))
    ));
    options.page_token = "not-a-token".into();
    assert!(matches!(
        db.db().series.range(db.resource(), &options).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn omitting_the_total_is_honoured_on_every_page() {
    let (db, _) = disordered("omitted").await;
    let mut options = ListOptions::default().page(400);
    options.omit_total = true;
    let (items, next, total) = db.list(&options).await.unwrap();
    assert_eq!((items.len(), total), (400, -1));
    options.page_token = next;
    assert_eq!(db.list(&options).await.unwrap().2, -1);
    assert_eq!(db.all(&ListOptions::default()).await.unwrap().len(), 1_000);
}

/// Seconds to page through `rows` rows at the largest page size, with the page count.
async fn export(rows: i64) -> (Duration, usize) {
    let db = open(&ArrowProvider::new(root(&format!("export-{rows}")))).await;
    let mut written = 0;
    while written < rows {
        let batch: Vec<Point> = (written..(written + 1_000).min(rows))
            .map(|i| point("analog-input:1", i as f64, i))
            .collect();
        written += batch.len() as i64;
        db.create_many(&batch).await.unwrap();
    }
    let mut options = paged(10_000, false);
    options.end = rows;
    let (started, mut seen, mut pages) = (Instant::now(), 0, 0);
    loop {
        let page = db.db().series.range(db.resource(), &options).await.unwrap();
        seen += page.items.len() as i64;
        pages += 1;
        if page.next_page_token.is_empty() {
            break;
        }
        options.page_token = page.next_page_token;
    }
    assert_eq!(seen, rows);
    (started.elapsed(), pages)
}

#[tokio::test]
#[ignore = "a timing measurement, meaningful only in release"]
async fn a_large_read_takes_time_linear_in_its_rows() {
    let mut last: Option<(i64, Duration)> = None;
    for rows in [250_000, 500_000, 1_000_000, 2_000_000, 5_310_000] {
        let (took, pages) = export(rows).await;
        let per_million = took.as_secs_f64() / (rows as f64 / 1e6);
        println!("{rows:>9} rows {pages:>4} pages {took:>10.2?} {per_million:.2} s/M rows");
        if let Some((rows_before, before)) = last {
            let size = rows as f64 / rows_before as f64;
            let time = took.as_secs_f64() / before.as_secs_f64();
            assert!(time < size * 1.6, "{size}x the rows took {time}x the time");
        }
        last = Some((rows, took));
    }
}
