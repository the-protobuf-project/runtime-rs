//! Paging, ordering and filters. Go's `TestListPagesAndFilters` and `TestListDescending`,
//! with `omit_total`, a filter on every column kind, and the shared AIP-160 table.

use database::records::{DB, ListOptions, Resource};
use prost_reflect::Value as P;

use crate::support::{book, string_field};
use crate::with_db;

/// `n` books `books/00`.. with `pages` equal to their index.
async fn seed(db: &DB, res: &Resource, n: i32) {
    for i in 0..n {
        let id = format!("{i:02}");
        let mut b = book(&id, &format!("Title {id}"), i);
        b.set_field_by_name("price", P::F64(f64::from(i) + 0.5));
        b.set_field_by_name("in_print", P::Bool(i % 2 == 0));
        b.set_field_by_name("status", P::EnumNumber(i % 3));
        b.set_field_by_name("isbn", P::U64(1000 + i as u64));
        db.create(res, &b).await.unwrap();
    }
}

async fn names(db: &DB, res: &Resource, opts: ListOptions) -> Vec<String> {
    let page = db.list(res, &opts).await.unwrap();
    page.items.iter().map(|m| string_field(m, "name")).collect()
}

#[tokio::test]
async fn list_pages_and_filters() {
    with_db(|f| async move {
        let (db, res) = (&f.db, &*f.res);
        seed(db, res, 25).await;
        assert_eq!(db.count(res, &ListOptions::default()).await.unwrap(), 25);

        let (mut seen, mut token) = (Vec::new(), String::new());
        for _ in 0..=25 {
            let opts = ListOptions::default().page_size(10).page_token(&token);
            let page = db.list(res, &opts).await.unwrap();
            assert_eq!(page.total, 25);
            seen.extend(page.items.iter().map(|m| string_field(m, "name")));
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        assert_eq!(seen.len(), 25, "paged over every record once");
        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "in key order: {seen:?}"
        );

        // The server filters, so a page size means what it says.
        let page = db
            .list(res, &ListOptions::default().filter("pages >= 20"))
            .await
            .unwrap();
        assert_eq!((page.items.len(), page.total), (5, 5));

        for bad in [
            "nosuch = 1",
            "pages < three",
            "pages = many",
            "a = 1 OR b = 2",
        ] {
            let opts = ListOptions::default().filter(bad);
            assert!(db.list(res, &opts).await.is_err(), "{bad} was accepted");
            assert!(db.count(res, &opts).await.is_err(), "{bad} was counted");
        }
    })
    .await;
}

#[tokio::test]
async fn list_descending() {
    with_db(|f| async move {
        seed(&f.db, &f.res, 5).await;
        let got = names(&f.db, &f.res, ListOptions::default().order_by("name desc")).await;
        assert_eq!(got.len(), 5);
        assert_eq!(got[0], "books/04");
        let by_pages = names(&f.db, &f.res, ListOptions::default().order_by("pages desc")).await;
        assert_eq!(by_pages[0], "books/04");
        // An unknown column orders by key rather than failing.
        let unknown = names(
            &f.db,
            &f.res,
            ListOptions::default().order_by("nosuch desc"),
        )
        .await;
        assert_eq!(unknown[0], "books/00");
    })
    .await;
}

/// Without a total, a page reads one past itself to know whether another follows.
#[tokio::test]
async fn list_omits_the_total() {
    with_db(|f| async move {
        seed(&f.db, &f.res, 5).await;
        let opts = ListOptions::default().page_size(2).omit_total(true);
        let first = f.db.list(&f.res, &opts).await.unwrap();
        assert_eq!((first.items.len(), first.total), (2, -1));
        assert_eq!(first.next_page_token, "2");
        let last =
            f.db.list(&f.res, &opts.clone().page_token("4"))
                .await
                .unwrap();
        assert_eq!(last.items.len(), 1);
        assert!(last.next_page_token.is_empty());
        let exact =
            f.db.list(&f.res, &opts.page_size(5).page_token(""))
                .await
                .unwrap();
        assert_eq!(exact.items.len(), 5);
        assert!(exact.next_page_token.is_empty(), "no phantom next page");
    })
    .await;
}

/// Each value is typed by its column's kind, so a comparison against a number is a number.
#[tokio::test]
async fn filters_are_typed_by_column_kind() {
    with_db(|f| async move {
        seed(&f.db, &f.res, 10).await;
        for (filter, want) in [
            ("name = \"books/03\"", 1),
            ("name != books/03", 9),
            ("title = \"Title 07\"", 1),
            ("pages < 3", 3),
            ("price > 4.5", 5),
            ("in_print = true", 5),
            ("in_print = F", 5),
            ("status = 2", 3),
            ("isbn >= 1008", 2),
            ("pages >= 2 AND pages < 5", 3),
            ("pages > 1 AND in_print = true AND status != 0", 3),
            ("create_time > \"2000-01-01T00:00:00Z\"", 10),
            ("create_time < 2000-01-01", 0),
        ] {
            let opts = ListOptions::default().filter(filter);
            assert_eq!(f.db.count(&f.res, &opts).await.unwrap(), want, "{filter}");
        }
        // Past i64::MAX a uint64 is stored as Decimal128, and its filter value encodes the same.
        let mut max = book("max", "Max", 1);
        max.set_field_by_name("isbn", P::U64(u64::MAX));
        f.db.create(&f.res, &max).await.unwrap();
        let opts = ListOptions::default().filter("isbn = 18446744073709551615");
        assert_eq!(f.db.count(&f.res, &opts).await.unwrap(), 1);
        for bad in ["price > cheap", "in_print = yes", "isbn = -1"] {
            let opts = ListOptions::default().filter(bad);
            assert!(
                f.db.count(&f.res, &opts).await.is_err(),
                "{bad} was accepted"
            );
        }
    })
    .await;
}

/// The shared AIP-160 table: OR, NOT, has and nulls answer as on SQL and the Tauri store.
#[tokio::test]
async fn aip160_conformance() {
    with_db(
        |f| async move { crate::filter_conformance::run("mongodb", &f.db, &f.res, book).await },
    )
    .await;
}
