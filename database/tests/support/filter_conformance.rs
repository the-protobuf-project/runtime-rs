//! The AIP-160 conformance table every records provider runs: five books, the filters, and
//! the books each must match, so SQL, MongoDB, the Tauri store and the in-memory reference
//! ([`database::records::core::filter::Expr::matches`]) are held to one answer.

#![allow(dead_code)]

use database::Error;
use database::records::{DB, ListOptions, Resource};
use prost_reflect::{DynamicMessage, Value as P};

/// How a suite builds a book: `support/records.rs`'s `book`, which each suite already loads.
pub type Book = fn(&str, &str, i32) -> DynamicMessage;

/// Filters and the books (`books/<id>`) each matches, ids sorted. Nulls: `a` and `d` have no
/// subtitle, so they match no comparison on it, `!=` and `NOT` included.
pub const CASES: &[(&str, &[&str])] = &[
    ("pages > 300", &["a", "d"]),
    ("pages >= 300", &["a", "b", "d"]),
    ("pages > 300 OR price < 5", &["a", "b", "d", "e"]),
    ("pages > 300 AND in_print = true", &["a"]),
    ("NOT pages > 300", &["b", "c", "e"]),
    ("-pages > 300", &["b", "c", "e"]),
    ("NOT NOT pages > 300", &["a", "d"]),
    // OR binds tighter than AND, as AIP-160 specifies.
    ("in_print = true AND pages > 300 OR status = 1", &["a", "e"]),
    (
        "(in_print = true AND pages > 300) OR status = 1",
        &["a", "b", "e"],
    ),
    ("in_print = true pages > 300", &["a"]),
    ("pages > 300 and in_print = false", &["d"]),
    (r#"subtitle = "A Novel""#, &["b"]),
    (r#"subtitle != "A Novel""#, &["c", "e"]),
    (r#"NOT subtitle = "A Novel""#, &["c", "e"]),
    (r#"NOT (subtitle = "A Novel" OR pages < 210)"#, &["c"]),
    ("NOT (pages < 250 OR subtitle = null)", &["b", "c"]),
    ("subtitle:*", &["b", "c", "e"]),
    ("NOT subtitle:*", &["a", "d"]),
    ("subtitle = null", &["a", "d"]),
    ("subtitle != null", &["b", "c", "e"]),
    (r#"title = "Dune" OR title = 'Hyperion'"#, &["a", "d"]),
    (r#"title = "Ubik \"Joe\"""#, &["e"]),
    (r#"title = 'Ubik "Joe"'"#, &["e"]),
    (r#"title < "F""#, &["a", "b"]),
    ("status = 2", &["a", "d"]),
    ("status != 2 AND isbn >= 1003", &["c", "e"]),
    ("price <= 7", &["b", "c", "e"]),
    ("price > 4.25 AND price < 12", &["a", "c"]),
    ("pages > -1", &["a", "b", "c", "d", "e"]),
    ("in_print = f", &["b", "d"]),
    (r#"cover = "gif""#, &["d"]),
    (r#"cover != "gif""#, &["a", "b", "c", "e"]),
    (
        r#"create_time > "2000-01-01T00:00:00Z""#,
        &["a", "b", "c", "d", "e"],
    ),
    (r#"create_time < "2000-01-01T00:00:00+02:00""#, &[]),
    (r#"name = "books/c""#, &["c"]),
    (r#"name > "books/c" AND NOT name = "books/e""#, &["d"]),
    (
        "(pages < 250 OR pages > 450) AND (in_print = true OR subtitle:*)",
        &["e"],
    ),
];

/// Filters every provider refuses as [`Error::Invalid`] before reaching its backend.
pub const REFUSED: &[&str] = &[
    "nosuch = 1",
    "pages = many",
    r#"title = "Dune*""#,
    "title = Dune*",
    r#"author.name = "x""#,
    "size(title) > 2",
    "Dune",
    "pages >",
    "(pages > 1",
    "pages > 1)",
    r#"title:"Dune""#,
    r#"cover < "a""#,
    "subtitle < null",
    "pages = 1.5s",
    r#"title = "Dune"#,
    "pages ! 1",
    "pages = 1 OR",
];

/// The five books: `(id, title, pages, price, in_print, status, isbn, cover, subtitle)`.
pub fn seed(book: Book) -> Vec<DynamicMessage> {
    [
        ("a", "Dune", 412, 9.5, true, 2, 1001, "png", None),
        (
            "b",
            "Emma",
            300,
            4.25,
            false,
            1,
            1002,
            "png",
            Some("A Novel"),
        ),
        (
            "c",
            "Foundation",
            255,
            7.0,
            true,
            0,
            1003,
            "png",
            Some("Book One"),
        ),
        ("d", "Hyperion", 482, 12.0, false, 2, 1004, "gif", None),
        (
            "e",
            "Ubik \"Joe\"",
            202,
            3.5,
            true,
            1,
            1005,
            "png",
            Some("x"),
        ),
    ]
    .into_iter()
    .map(
        |(id, title, pages, price, print, status, isbn, cover, sub)| {
            let mut b = book(id, title, pages);
            b.set_field_by_name("price", P::F64(price));
            b.set_field_by_name("in_print", P::Bool(print));
            b.set_field_by_name("status", P::EnumNumber(status));
            b.set_field_by_name("isbn", P::U64(isbn));
            b.set_field_by_name("cover", P::Bytes(cover.as_bytes().to_vec().into()));
            if let Some(s) = sub {
                b.set_field_by_name("subtitle", P::String(s.into()));
            }
            b
        },
    )
    .collect()
}

/// The id of a book named `books/<id>`.
pub fn id(m: &DynamicMessage) -> String {
    let name = m
        .get_field_by_name("name")
        .and_then(|v| v.as_str().map(str::to_owned));
    name.unwrap_or_default()
        .trim_start_matches("books/")
        .to_owned()
}

/// Seeds `res` (the `Book` of `support/records.rs`) on `db` and runs the whole table through
/// `list` and `count`, panicking once with every disagreement. `provider` names the run.
pub async fn run(provider: &str, db: &DB, res: &Resource, book: Book) {
    for b in seed(book) {
        db.create(res, &b).await.unwrap();
    }
    let mut wrong = Vec::new();
    for (filter, want) in CASES {
        let opts = ListOptions::default().filter(*filter).page_size(100);
        let got = match db.list(res, &opts).await {
            Ok(page) => {
                let mut ids: Vec<String> = page.items.iter().map(id).collect();
                ids.sort();
                ids
            }
            Err(e) => vec![format!("error: {e}")],
        };
        let count = db.count(res, &opts).await.ok();
        if got != *want || count != Some(want.len() as i64) {
            wrong.push(format!(
                "{filter}: want {want:?}, got {got:?} (count {count:?})"
            ));
        }
    }
    for filter in REFUSED {
        let opts = ListOptions::default().filter(*filter);
        match db.list(res, &opts).await {
            Err(Error::Invalid(_)) => {}
            other => wrong.push(format!("{filter}: want Invalid, got {other:?}")),
        }
    }
    assert!(
        wrong.is_empty(),
        "{provider} disagrees with the AIP-160 conformance table:\n{}",
        wrong.join("\n")
    );
    eprintln!(
        "{provider}: {} filters matched and {} refused as the table says",
        CASES.len(),
        REFUSED.len()
    );
}
