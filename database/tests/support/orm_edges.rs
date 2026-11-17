//! The edges of the SQL backend's contract, on every engine: each column kind round-tripped
//! and filtered, the not-found and conflict outcomes, and what a listing does with an order,
//! filter or token it cannot use.

#![allow(dead_code)]

use database::Error;
use database::orm::Dialect;
use database::records::{DB, ListOptions, Resource};
use prost_reflect::Value as P;

use super::orm::{new_book, text};
use super::orm_scenarios::{drop, table};
use super::records::{book, books};

/// The shared `Book` (every kind, an optional field, audit times) on its own table, typed by
/// the dialect's defaults rather than the fixture's Postgres-flavoured SQL types.
pub async fn every_kind(db: &DB, dialect: Dialect) {
    let mut res: Resource = books();
    res.table = format!("kinds_{}_{}", std::process::id(), dialect.name());
    res.columns.iter_mut().for_each(|c| c.sql_type.clear());
    db.schema.ensure_schema(&res).await.unwrap();

    let dune = book("dune", "Dune", 412);
    let made = db.create(&res, &dune).await.unwrap().message;
    assert!(made.has_field_by_name("create_time") && made.has_field_by_name("update_time"));
    let got = db.get(&res, "books/dune").await.unwrap();
    for f in [
        "name", "title", "pages", "price", "in_print", "cover", "status", "isbn",
    ] {
        assert_eq!(got.get_field_by_name(f), dune.get_field_by_name(f), "{f}");
    }
    assert!(!got.has_field_by_name("subtitle"), "null reads back unset");
    assert!(got.has_field_by_name("create_time"));

    for (filter, want) in [
        ("pages = 412", 1),
        ("price >= 9.5", 1),
        ("in_print = true", 1),
        ("in_print = f", 0),
        ("cover = \"png\"", 1),
        ("status = 2", 1),
        ("isbn = 9780441013593", 1),
        ("title = \"Dune\" AND pages > 400", 1),
        ("create_time > \"2000-01-01T00:00:00Z\"", 1),
        ("subtitle = \"x\"", 0),
    ] {
        let n = db.count(&res, &ListOptions::default().filter(filter)).await;
        assert_eq!(n.unwrap(), want, "{filter}");
    }

    let mut next = got.clone();
    next.set_field_by_name("subtitle", P::String("Messiah".into()));
    db.update(&res, &next).await.unwrap();
    let after = db.get(&res, "books/dune").await.unwrap();
    assert_eq!(text(&after, "subtitle"), "Messiah");
    assert_eq!(
        after.get_field_by_name("create_time"),
        got.get_field_by_name("create_time")
    );

    let mut big = book("big", "Big", 1);
    big.set_field_by_name("isbn", P::U64(u64::MAX));
    let wide = db.create(&res, &big).await;
    match dialect {
        Dialect::MySql => assert!(wide.is_ok(), "BIGINT UNSIGNED holds it: {wide:?}"),
        _ => assert!(
            matches!(wide, Err(Error::Invalid(_))),
            "refused, not wrapped: {wide:?}"
        ),
    }
    drop(db, &res).await;
}

pub fn ids(page: &[prost_reflect::DynamicMessage]) -> Vec<String> {
    page.iter().map(|m| text(m, "id")).collect()
}

/// Not found, already exists, and listings given what they cannot use.
pub async fn edges(db: &DB) {
    let res = table(db).await;
    for (id, title, year, genre) in [
        ("a", "A", 2001, 1),
        ("b", "B", 2000, 0),
        ("c", "C", 2000, 1),
    ] {
        db.create(&res, &new_book(id, title, year, genre))
            .await
            .unwrap();
    }
    assert!(matches!(db.get(&res, "zz").await, Err(Error::NotFound)));
    assert!(matches!(
        db.update(&res, &new_book("zz", "Z", 1, 1)).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(db.delete(&res, "zz").await, Err(Error::NotFound)));
    assert!(!db.exists(&res, "zz").await.unwrap() && db.exists(&res, "a").await.unwrap());
    assert!(matches!(
        db.create(&res, &new_book("a", "Dup", 1, 1)).await,
        Err(Error::AlreadyExists)
    ));
    let taken = db.update(&res, &new_book("b", "A", 2000, 0)).await;
    assert!(
        matches!(taken, Err(Error::AlreadyExists)),
        "a taken unique title: {taken:?}"
    );

    for (bad, names) in [
        ("shelf = 3", "shelf"),
        ("published_year >= soon", "published_year"),
        ("title < A", "unsupported operator"),
        ("title LIKE A", "free-text search is not supported"),
    ] {
        let err = db
            .list(&res, &ListOptions::default().filter(bad))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Invalid(m) if m.contains(names)),
            "{bad}: {err}"
        );
        assert!(
            db.count(&res, &ListOptions::default().filter(bad))
                .await
                .is_err(),
            "{bad}"
        );
    }
    for (filter, want) in [
        ("title != \"A\"", vec!["b", "c"]),
        ("genre = 1", vec!["a", "c"]),
        ("published_year = 2000 AND genre = 0", vec!["b"]),
        ("title = \"x AND y\"", vec![]),
        ("title = A OR title = B", vec!["a", "b"]),
    ] {
        let page = db
            .list(&res, &ListOptions::default().filter(filter))
            .await
            .unwrap();
        assert_eq!(ids(&page.items), want, "{filter}");
    }

    for (order, want) in [
        ("", vec!["a", "b", "c"]),
        ("shelf desc", vec!["a", "b", "c"]),
        ("title DESC", vec!["c", "b", "a"]),
        ("published_year, id desc", vec!["c", "b", "a"]),
        ("genre desc, published_year desc", vec!["a", "c", "b"]),
    ] {
        let page = db
            .list(&res, &ListOptions::default().order_by(order))
            .await
            .unwrap();
        assert_eq!(ids(&page.items), want, "order_by {order:?}");
    }

    let junk = ListOptions::default().page_size(2).page_token("junk");
    let page = db.list(&res, &junk).await.unwrap();
    assert_eq!(
        (ids(&page.items), page.next_page_token.as_str(), page.total),
        (vec!["a".into(), "b".into()], "2", 3)
    );
    let page = db.list(&res, &ListOptions::default()).await.unwrap();
    assert_eq!(
        (page.items.len(), page.next_page_token.as_str()),
        (3, ""),
        "default page size"
    );
    drop(db, &res).await;
}

/// The shared AIP-160 conformance table on a table of its own, typed by the dialect's
/// defaults, so Postgres, MySQL and SQLite answer it as every other provider does.
pub async fn conformance(db: &DB, dialect: Dialect) {
    let mut res: Resource = books();
    res.table = format!("filters_{}_{}", std::process::id(), dialect.name());
    res.columns.iter_mut().for_each(|c| c.sql_type.clear());
    db.schema.ensure_schema(&res).await.unwrap();
    super::filter_conformance::run(dialect.name(), db, &res, book).await;
    drop(db, &res).await;
}
