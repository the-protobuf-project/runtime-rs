//! Listing: stable pages by key, either direction, with and without the total; a filter is
//! refused by name.

use database::Error;
use database::records::ListOptions;

use crate::support::{book, field, setup, teardown};

async fn fill(db: &database::records::DB, res: &database::records::Resource, n: usize) {
    for i in 0..n {
        let id = format!("{i:02}");
        db.create(res, &book(&id, &id)).await.unwrap();
    }
}

#[tokio::test]
async fn list_pages_stably() {
    let (db, res) = setup().await;
    let n = 25;
    fill(&db, &res, n).await;
    assert_eq!(
        db.count(&res, &ListOptions::default()).await.unwrap(),
        n as i64
    );

    let (mut seen, mut token) = (Vec::new(), String::new());
    for _ in 0..=n {
        let page = db
            .list(
                &res,
                &ListOptions::default().page_size(10).page_token(&token),
            )
            .await
            .unwrap();
        assert_eq!(page.total, n as i64);
        seen.extend(page.items.iter().map(|m| field(m, "name")));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(seen.len(), n);
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "out of order: {seen:?}"
    );
    teardown(&db, &res).await;
}

#[tokio::test]
async fn list_descending() {
    let (db, res) = setup().await;
    fill(&db, &res, 5).await;
    let page = db
        .list(&res, &ListOptions::default().order_by("name desc"))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 5);
    assert_eq!(field(&page.items[0], "name"), "books/04");
    // Another column orders by the key, as the contract permits.
    let page = db
        .list(&res, &ListOptions::default().order_by("title desc"))
        .await
        .unwrap();
    assert_eq!(field(&page.items[0], "name"), "books/00");
    teardown(&db, &res).await;
}

/// With the total skipped, one key past the page says whether another follows.
#[tokio::test]
async fn list_without_the_total() {
    let (db, res) = setup().await;
    fill(&db, &res, 5).await;
    let opts = ListOptions::default().page_size(2).omit_total(true);
    let first = db.list(&res, &opts).await.unwrap();
    assert_eq!((first.items.len(), first.total), (2, -1));
    assert_eq!(first.next_page_token, "2");
    let last = db.list(&res, &opts.clone().page_token("4")).await.unwrap();
    assert_eq!(last.items.len(), 1);
    assert!(last.next_page_token.is_empty());
    let past = db.list(&res, &opts.page_token("9")).await.unwrap();
    assert!(past.items.is_empty() && past.next_page_token.is_empty());
    teardown(&db, &res).await;
}

/// Redis has no query language; a filter is refused, never silently ignored.
#[tokio::test]
async fn a_filter_is_refused_by_name() {
    let (db, res) = setup().await;
    let opts = ListOptions::default().filter("pages > 3");
    for err in [
        db.list(&res, &opts).await.unwrap_err(),
        db.count(&res, &opts).await.unwrap_err(),
    ] {
        assert!(
            matches!(
                err,
                Error::Unimplemented {
                    backend: "redis",
                    ..
                }
            ),
            "{err}"
        );
    }
    teardown(&db, &res).await;
}
