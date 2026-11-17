//! The SQL backend's CRUD on SQLite, in-process, over tables made by hand: runtime-go's
//! `orm/driver_test.go`.

mod support {
    #[path = "orm.rs"]
    pub mod orm;
    #[path = "records.rs"]
    pub mod records;
}

use database::Error;
use database::orm::{self, sqlx};
use database::records::{Column, DB, Generated, Kind, ListOptions, Resource};
use prost_reflect::{DynamicMessage, Value as P};
use support::orm::{book_res, new_book, sqlite, text};

/// Go's `setup`: a `books` table made by hand, and the driver over it with no selection.
async fn setup() -> (DB, Resource) {
    let pool = sqlite().await;
    sqlx::query(
        "CREATE TABLE books (id TEXT PRIMARY KEY, title TEXT NOT NULL, \
         published_year INTEGER, genre INTEGER NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    (orm::driver(pool), book_res())
}

#[tokio::test]
async fn driver_crud() {
    let (d, res) = setup().await;
    d.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    assert_eq!(
        text(&d.get(&res, "books/dune").await.unwrap(), "title"),
        "Dune"
    );
    assert!(d.exists(&res, "books/dune").await.unwrap());

    d.update(&res, &new_book("books/dune", "Dune (1965)", 1965, 1))
        .await
        .unwrap();
    assert_eq!(
        text(&d.get(&res, "books/dune").await.unwrap(), "title"),
        "Dune (1965)"
    );

    d.create(&res, &new_book("books/hobbit", "The Hobbit", 1937, 1))
        .await
        .unwrap();
    let opts = ListOptions::default().page_size(10).order_by("id");
    let lr = d.list(&res, &opts).await.unwrap();
    assert_eq!((lr.items.len(), lr.total), (2, 2));

    d.delete(&res, "books/dune").await.unwrap();
    assert!(matches!(
        d.get(&res, "books/dune").await,
        Err(Error::NotFound)
    ));
}

#[tokio::test]
async fn driver_error_mapping() {
    let (d, res) = setup().await;
    d.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    let dup = d
        .create(&res, &new_book("books/dune", "Dup", 1965, 1))
        .await;
    assert!(matches!(dup, Err(Error::AlreadyExists)), "{dup:?}");
    let missing = d
        .update(&res, &new_book("books/missing", "X", 2000, 1))
        .await;
    assert!(matches!(missing, Err(Error::NotFound)), "{missing:?}");
    assert!(matches!(
        d.delete(&res, "books/missing").await,
        Err(Error::NotFound)
    ));
}

/// Go's `TestDriverManagedColumns`: a ULID key and audit times the message does not carry.
#[tokio::test]
async fn driver_managed_columns() {
    let pool = sqlite().await;
    sqlx::query(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT NOT NULL, \
         created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let note_md = support::records::book_descriptor(); // any message with a `title` string
    let managed = |name: &str, kind| Column {
        field: String::new(),
        not_null: true,
        ..Column::new(name, kind)
    };
    let mut id = managed("id", Kind::String);
    id.primary_key = true;
    id.generated = Some(Generated::Ulid);
    let mut created = managed("created_at", Kind::Timestamp);
    created.auto_create = true;
    let mut updated = managed("updated_at", Kind::Timestamp);
    updated.auto_update = true;
    let body = Column {
        field: "title".into(),
        ..Column::new("body", Kind::String).not_null()
    };
    let res = Resource::new(
        note_md.clone(),
        "notes",
        "id",
        vec![id, body, created, updated],
    );

    let mut note = DynamicMessage::new(note_md);
    note.set_field_by_name("title", P::String("hello".into()));
    orm::driver(pool.clone()).create(&res, &note).await.unwrap();

    use sqlx::Row as _;
    let rows = sqlx::query("SELECT * FROM notes")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: String = rows[0].get("id");
    assert_eq!(id.len(), 26, "a ULID: {id}");
    let stamps: (Option<String>, Option<String>) =
        (rows[0].get("created_at"), rows[0].get("updated_at"));
    assert!(
        stamps.0.is_some() && stamps.1.is_some(),
        "audit times: {stamps:?}"
    );
}

#[tokio::test]
async fn driver_list_pagination() {
    let (d, res) = setup().await;
    for id in ["a", "b", "c"] {
        d.create(&res, &new_book(id, id, 2000, 1)).await.unwrap();
    }
    let first = d
        .list(&res, &ListOptions::default().page_size(2).order_by("id"))
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert!(!first.next_page_token.is_empty());
    let opts = ListOptions::default()
        .page_size(2)
        .order_by("id")
        .page_token(&first.next_page_token);
    let second = d.list(&res, &opts).await.unwrap();
    assert_eq!(
        (second.items.len(), second.next_page_token.as_str()),
        (1, "")
    );
}
