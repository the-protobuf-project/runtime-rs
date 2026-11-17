//! Unique columns: enforced, moved on update, released on delete, and decided by the server
//! when writers race.

use database::Error;
use database::records::ListOptions;

use crate::support::{book, field, setup, teardown};

/// A column the descriptor marks unique has to be unique, or the descriptor is lying.
#[tokio::test]
async fn a_unique_column_is_enforced() {
    let (db, res) = setup().await;
    db.create(&res, &book("a", "Shared")).await.unwrap();
    let err = db.create(&res, &book("b", "Shared")).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyExists), "{err}");
    assert!(
        !db.exists(&res, "books/b").await.unwrap(),
        "a refused create left its record"
    );
    db.create(&res, &book("b", "Other")).await.unwrap();
    teardown(&db, &res).await;
}

/// Changing a unique value releases the old one, or nobody could use it again.
#[tokio::test]
async fn update_moves_a_unique_reservation() {
    let (db, res) = setup().await;
    db.create(&res, &book("a", "Old")).await.unwrap();
    db.update(&res, &book("a", "New")).await.unwrap();
    db.create(&res, &book("b", "Old")).await.unwrap();
    let err = db.create(&res, &book("c", "New")).await.unwrap_err();
    assert!(
        matches!(err, Error::AlreadyExists),
        "the claimed value was not held: {err}"
    );
    teardown(&db, &res).await;
}

/// An update onto a value another record holds is refused and changes nothing.
#[tokio::test]
async fn update_refuses_a_value_held_elsewhere() {
    let (db, res) = setup().await;
    db.create(&res, &book("a", "A")).await.unwrap();
    db.create(&res, &book("b", "B")).await.unwrap();
    let err = db.update(&res, &book("b", "A")).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyExists), "{err}");
    assert_eq!(field(&db.get(&res, "books/b").await.unwrap(), "title"), "B");
    // B's own reservation survived the refused move.
    let err = db.create(&res, &book("c", "B")).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyExists), "{err}");
    teardown(&db, &res).await;
}

/// Rewriting a record without changing its unique value is not a conflict with itself.
#[tokio::test]
async fn update_keeping_its_own_unique_value() {
    let (db, res) = setup().await;
    db.create(&res, &crate::records::book("a", "Same", 1))
        .await
        .unwrap();
    db.update(&res, &crate::records::book("a", "Same", 2))
        .await
        .unwrap();
    let got = db.get(&res, "books/a").await.unwrap();
    assert_eq!(got.get_field_by_name("pages").unwrap().as_i32(), Some(2));
    teardown(&db, &res).await;
}

/// Delete releases the reservation, escaped exactly as it was claimed.
#[tokio::test]
async fn delete_releases_a_unique_value() {
    let (db, res) = setup().await;
    let title = "a:b%c"; // the separator, and the escape character
    db.create(&res, &book("a", title)).await.unwrap();
    db.delete(&res, "books/a").await.unwrap();
    db.create(&res, &book("b", title)).await.unwrap();
    teardown(&db, &res).await;
}

/// Writers racing on one unique value: exactly one wins, and the losers leave nothing.
#[tokio::test]
async fn concurrent_unique_claims() {
    let (db, res) = setup().await;
    let writers = 20;
    let tasks: Vec<_> = (0..writers)
        .map(|i| {
            let (db, res) = (db.clone(), res.clone());
            tokio::spawn(async move { db.create(&res, &book(&i.to_string(), "Contested")).await })
        })
        .collect();
    let (mut wins, mut conflicts) = (0, 0);
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => wins += 1,
            Err(Error::AlreadyExists) => conflicts += 1,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert_eq!((wins, conflicts), (1, writers - 1));
    assert_eq!(db.count(&res, &ListOptions::default()).await.unwrap(), 1);
    teardown(&db, &res).await;
}
