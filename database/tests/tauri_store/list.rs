//! Listing, counting, tenants, and saving without durability.

use database::Error;
use database::records::{ListOptions, Provider};
use prost_reflect::Value as P;

use super::{app, db, on_disk, provider, support};
use database::tauri_store::TauriStoreProvider;

#[tokio::test]
async fn listings_filter_order_and_page() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, support::books());
    for (id, pages) in [("a", 100), ("b", 300), ("c", 200), ("d", 400)] {
        db.create(&res, &support::book(id, &id.to_uppercase(), pages))
            .await
            .unwrap();
    }
    let opts = ListOptions::default()
        .filter("pages >= 200")
        .order_by("pages desc")
        .page_size(2);
    let first = db.list(&res, &opts).await.unwrap();
    let pages = |l: &database::records::ListResult| -> Vec<i32> {
        l.items
            .iter()
            .map(|m| m.get_field_by_name("pages").unwrap().as_i32().unwrap())
            .collect()
    };
    assert_eq!((pages(&first), first.total), (vec![400, 300], 3));
    let second = db
        .list(&res, &opts.clone().page_token(first.next_page_token))
        .await
        .unwrap();
    assert_eq!(
        (pages(&second), second.next_page_token.as_str()),
        (vec![200], "")
    );
    let skipped = db.list(&res, &opts.clone().omit_total(true)).await.unwrap();
    assert_eq!((skipped.total, skipped.next_page_token.as_str()), (-1, "2"));
    assert_eq!(
        db.count(&res, &ListOptions::default().filter("pages < 300"))
            .await
            .unwrap(),
        2
    );
    for bad in ["nope = 1", "pages = many", "a = 1 OR b = 2"] {
        assert!(
            matches!(
                db.list(&res, &ListOptions::default().filter(bad)).await,
                Err(Error::Invalid(_))
            ),
            "{bad}"
        );
    }
}

#[tokio::test]
async fn tenants_are_key_prefixes_of_one_file() {
    let (_app, p, path) = provider();
    let res = support::books();
    let (a, b) = (
        p.set_database("tenant_a").await.unwrap(),
        p.set_database("tenant_b").await.unwrap(),
    );
    a.create(&res, &support::book("dune", "Dune", 1))
        .await
        .unwrap();
    b.create(&res, &support::book("dune", "Dune", 1))
        .await
        .expect("its own unique values");
    assert!(on_disk(&path).contains_key("tenant_b:books:rec:books/dune"));
    a.schema.drop_schema(&res).await.unwrap();
    assert!(!a.exists(&res, "books/dune").await.unwrap());
    assert!(b.exists(&res, "books/dune").await.unwrap());
    assert!(p.set_database("a-b").await.is_err());
}

#[tokio::test]
async fn without_durability_the_file_waits_for_close() {
    let app = app();
    let path = std::env::temp_dir().join(format!("tpp-tauri-lazy-{}.json", uuid::Uuid::new_v4()));
    let p = TauriStoreProvider::open(app.handle(), &path)
        .unwrap()
        .durable(false);
    let (db, res) = (db(&p).await, support::books());
    let mut book = support::book("dune", "Dune", 1);
    book.set_field_by_name("subtitle", P::String("Book One".into()));
    db.create(&res, &book).await.unwrap();
    assert!(!path.exists(), "nothing saved yet");
    db.close().await.unwrap();
    assert!(on_disk(&path).contains_key("books:rec:books/dune"));
    assert!(matches!(
        db.watch.watch(&res, Default::default()).await,
        Err(Error::Unimplemented { .. })
    ));
}

/// The shared AIP-160 table, evaluated in memory, answers as SQL and MongoDB do.
#[tokio::test]
async fn aip160_conformance() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, support::books());
    crate::filter_conformance::run("tauri store", &db, &res, support::book).await;
}
