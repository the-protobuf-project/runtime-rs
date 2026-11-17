//! Writes, reads and their outcomes, and uniqueness under concurrency.

use std::sync::Arc;

use database::Error;

use super::{db, on_disk, provider, support};

#[tokio::test]
async fn a_record_round_trips_and_reaches_the_file() {
    let (_app, p, path) = provider();
    let (db, res) = (db(&p).await, support::books());
    let stored = db
        .create(&res, &support::book("dune", "Dune", 412))
        .await
        .unwrap()
        .message;
    assert!(
        stored.has_field_by_name("create_time"),
        "audit times are filled"
    );
    let got = db.get(&res, "books/dune").await.unwrap();
    assert_eq!(support::string_field(&got, "title"), "Dune");
    assert!(
        on_disk(&path).contains_key("books:rec:books/dune"),
        "saved before returning"
    );
    assert!(db.exists(&res, "books/dune").await.unwrap());
}

#[tokio::test]
async fn outcomes_are_the_contract_s() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, support::books());
    db.create(&res, &support::book("dune", "Dune", 412))
        .await
        .unwrap();
    let again = db.create(&res, &support::book("dune", "Other", 1)).await;
    assert!(matches!(again, Err(Error::AlreadyExists)), "{again:?}");
    let taken = db.create(&res, &support::book("copy", "Dune", 1)).await;
    assert!(
        matches!(taken, Err(Error::AlreadyExists)),
        "a unique title: {taken:?}"
    );
    assert!(matches!(
        db.get(&res, "books/none").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        db.delete(&res, "books/none").await,
        Err(Error::NotFound)
    ));
    let missing = db.update(&res, &support::book("none", "None", 1)).await;
    assert!(matches!(missing, Err(Error::NotFound)), "{missing:?}");
}

#[tokio::test]
async fn an_update_moves_its_unique_reservation() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, support::books());
    db.create(&res, &support::book("dune", "Dune", 412))
        .await
        .unwrap();
    db.create(&res, &support::book("emma", "Emma", 300))
        .await
        .unwrap();
    let onto = db.update(&res, &support::book("emma", "Dune", 300)).await;
    assert!(matches!(onto, Err(Error::AlreadyExists)), "{onto:?}");
    db.update(&res, &support::book("dune", "Dune Messiah", 256))
        .await
        .unwrap();
    db.create(&res, &support::book("copy", "Dune", 1))
        .await
        .expect("the old title is free");
    db.delete(&res, "books/copy").await.unwrap();
    db.create(&res, &support::book("again", "Dune", 1))
        .await
        .expect("a delete frees it");
}

#[tokio::test]
async fn concurrent_claims_on_one_value_admit_one() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, Arc::new(support::books()));
    let claims = (0..20).map(|i| {
        let (db, res) = (db.clone(), res.clone());
        tokio::spawn(async move {
            db.create(&res, &support::book(&i.to_string(), "Dune", 1))
                .await
        })
    });
    let won = futures::future::join_all(claims)
        .await
        .into_iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    assert_eq!(won, 1);
}
