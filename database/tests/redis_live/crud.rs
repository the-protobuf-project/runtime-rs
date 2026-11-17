//! CRUD, the values JSON would corrupt, managed columns, bulk reads and the typed view.

use database::Error;
use database::records::{ListOptions, Typed};
use prost_reflect::Value;

use crate::support::{self, PbBook, book, book_with, field, setup, teardown};

#[tokio::test]
async fn crud() {
    let (db, res) = setup().await;
    db.create(&res, &book("ada", "Ada")).await.unwrap();
    let got = db.get(&res, "books/ada").await.unwrap();
    assert_eq!(field(&got, "title"), "Ada");
    assert!(db.exists(&res, "books/ada").await.unwrap());

    db.update(&res, &book("ada", "Ada 2")).await.unwrap();
    let got = db.get(&res, "books/ada").await.unwrap();
    assert_eq!(field(&got, "title"), "Ada 2");

    db.delete(&res, "books/ada").await.unwrap();
    assert!(matches!(
        db.get(&res, "books/ada").await,
        Err(Error::NotFound)
    ));
    assert!(!db.exists(&res, "books/ada").await.unwrap());
    assert!(matches!(
        db.delete(&res, "books/ada").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        db.update(&res, &book("ada", "x")).await,
        Err(Error::NotFound)
    ));
    teardown(&db, &res).await;
}

/// Why records are proto bytes: bytes that are not UTF-8, and a `uint64` past 2^53.
#[tokio::test]
async fn values_survive_the_round_trip() {
    let (db, res) = setup().await;
    let cover = [0x00, 0xff, 0xfe, 0x80, 0x01];
    let isbn = 9_007_199_254_740_993_u64;
    db.create(&res, &book_with("x", "X", &cover, isbn))
        .await
        .unwrap();
    let got = db.get(&res, "books/x").await.unwrap();
    assert_eq!(
        got.get_field_by_name("cover").unwrap().as_bytes().unwrap()[..],
        cover
    );
    assert_eq!(got.get_field_by_name("isbn").unwrap().as_u64(), Some(isbn));
    teardown(&db, &res).await;
}

#[tokio::test]
async fn create_refuses_a_duplicate_key() {
    let (db, res) = setup().await;
    db.create(&res, &book("ada", "A")).await.unwrap();
    let err = db.create(&res, &book("ada", "B")).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyExists), "{err}");
    let got = db.get(&res, "books/ada").await.unwrap();
    assert_eq!(
        field(&got, "title"),
        "A",
        "the refused write changed the record"
    );
    teardown(&db, &res).await;
}

#[tokio::test]
async fn an_empty_key_is_refused() {
    let (db, res) = setup().await;
    let mut m = book("x", "X");
    m.set_field_by_name("name", Value::String(String::new()));
    assert!(matches!(db.create(&res, &m).await, Err(Error::Invalid(_))));
    teardown(&db, &res).await;
}

/// Audit times are the driver's: both set on create, and on update the create time is the
/// stored one while the update time moves.
#[tokio::test]
async fn managed_columns_are_filled() {
    let (db, res) = setup().await;
    let created = db.create(&res, &book("t", "T")).await.unwrap().message;
    let create_time = created
        .get_field_by_name("create_time")
        .unwrap()
        .into_owned();
    assert!(created.has_field_by_name("update_time"));
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let updated = db.update(&res, &book("t", "T2")).await.unwrap().message;
    assert_eq!(
        updated
            .get_field_by_name("create_time")
            .unwrap()
            .into_owned(),
        create_time
    );
    let stored = db.get(&res, "books/t").await.unwrap();
    assert_eq!(stored, updated);
    assert_ne!(
        stored
            .get_field_by_name("update_time")
            .unwrap()
            .into_owned(),
        create_time
    );
    teardown(&db, &res).await;
}

#[tokio::test]
async fn get_many_returns_in_order_with_gaps() {
    let (db, res) = setup().await;
    for id in ["a", "c"] {
        db.create(&res, &book(id, id)).await.unwrap();
    }
    assert!(db.has_batch());
    let keys = ["books/a", "books/b", "books/c"].map(String::from);
    let got = db.batch.get_many(&res, &keys).await.unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(field(got[0].as_ref().unwrap(), "name"), "books/a");
    assert!(
        got[1].is_none(),
        "a missing record is a None entry, not an error"
    );
    assert_eq!(field(got[2].as_ref().unwrap(), "name"), "books/c");
    assert!(db.batch.get_many(&res, &[]).await.unwrap().is_empty());

    let made = db
        .batch
        .create_many(&res, &[book("d", "d"), book("e", "e")])
        .await
        .unwrap();
    assert_eq!(made.len(), 2);
    let err = db
        .batch
        .create_many(&res, &[book("f", "f"), book("a", "a")])
        .await;
    assert!(matches!(err, Err(Error::AlreadyExists)));
    assert!(
        db.exists(&res, "books/f").await.unwrap(),
        "stops at, not before, a failure"
    );
    teardown(&db, &res).await;
}

#[tokio::test]
async fn typed_view_over_redis() {
    let (db, res) = setup().await;
    let books = Typed::<PbBook>::new(&db, support::shared(&res)).unwrap();
    let ada = PbBook {
        name: "books/ada".into(),
        title: "Ada".into(),
        isbn: 7,
        ..Default::default()
    };
    books.create(&ada).await.unwrap();
    assert_eq!(books.get("books/ada").await.unwrap().isbn, 7);
    let all = books
        .all(&ListOptions::default().page_size(2))
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    teardown(&db, &res).await;
}
