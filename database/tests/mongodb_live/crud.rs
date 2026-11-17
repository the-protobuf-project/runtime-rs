//! CRUD, round trips, uniqueness and tenant isolation. Go's `TestCRUD`,
//! `TestValuesSurviveTheRoundTrip`, `TestCreateRefusesADuplicateKey`,
//! `TestUniqueColumnIsEnforced` and `TestDatabasesAreIsolated`.

use database::Error;
use database::mongodb::MongoProvider;
use database::records::{Driver, Provider};
use prost_reflect::Value as P;

use crate::support::{book, books, string_field};
use crate::{client, unique_name, with_db};

#[tokio::test]
async fn crud() {
    with_db(|f| async move {
        let (db, res) = (&f.db, &f.res);
        let created = db.create(res, &book("ada", "Ada", 36)).await.unwrap();
        assert!(!created.pending);
        assert!(created.message.get_field_by_name("create_time").is_some());
        let got = db.get(res, "books/ada").await.unwrap();
        assert_eq!(string_field(&got, "title"), "Ada");
        assert!(db.exists(res, "books/ada").await.unwrap());

        let mut changed = book("ada", "Ada 2", 37);
        changed.set_field_by_name("subtitle", P::String("second".into()));
        db.update(res, &changed).await.unwrap();
        let got = db.get(res, "books/ada").await.unwrap();
        assert_eq!(string_field(&got, "title"), "Ada 2");
        assert_eq!(string_field(&got, "subtitle"), "second");
        assert_eq!(got.get_field_by_name("pages").unwrap().as_i32(), Some(37));

        db.delete(res, "books/ada").await.unwrap();
        assert!(matches!(
            db.get(res, "books/ada").await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            db.delete(res, "books/ada").await,
            Err(Error::NotFound)
        ));
        assert!(!db.exists(res, "books/ada").await.unwrap());
        assert!(matches!(
            db.update(res, &book("ada", "Ada", 1)).await,
            Err(Error::NotFound)
        ));
    })
    .await;
}

/// BSON has no unsigned 64-bit integer, so a large uint64 would come back negative if it
/// were handed to the driver as is.
#[tokio::test]
async fn values_survive_the_round_trip() {
    with_db(|f| async move {
        let cover = vec![0x00, 0xff, 0xfe, 0x80, 0x01];
        let mut b = book("x", "X", 1);
        b.set_field_by_name("cover", P::Bytes(cover.clone().into()));
        b.set_field_by_name("isbn", P::U64(u64::MAX));
        b.set_field_by_name("price", P::F64(12.25));
        b.set_field_by_name("in_print", P::Bool(false));
        b.set_field_by_name("status", P::EnumNumber(1));
        f.db.create(&f.res, &b).await.unwrap();
        let got = f.db.get(&f.res, "books/x").await.unwrap();
        let field = |n: &str| got.get_field_by_name(n).unwrap().into_owned();
        assert_eq!(field("cover").as_bytes().unwrap().as_ref(), &cover[..]);
        assert_eq!(field("isbn").as_u64(), Some(u64::MAX));
        assert_eq!(field("price").as_f64(), Some(12.25));
        assert_eq!(field("in_print").as_bool(), Some(false));
        assert_eq!(field("status").as_enum_number(), Some(1));
        assert!(
            !got.has_field_by_name("subtitle"),
            "unset optional stays unset"
        );
    })
    .await;
}

#[tokio::test]
async fn create_refuses_a_duplicate_key() {
    with_db(|f| async move {
        f.db.create(&f.res, &book("a", "A", 1)).await.unwrap();
        let again = f.db.create(&f.res, &book("a", "B", 2)).await;
        assert!(matches!(again, Err(Error::AlreadyExists)), "{again:?}");
    })
    .await;
}

/// A column the descriptor marks unique is only unique if `ensure_schema` built its index.
#[tokio::test]
async fn unique_column_is_enforced() {
    with_db(|f| async move {
        f.db.create(&f.res, &book("a", "Shared", 1)).await.unwrap();
        let clash = f.db.create(&f.res, &book("b", "Shared", 2)).await;
        assert!(matches!(clash, Err(Error::AlreadyExists)), "{clash:?}");
        f.db.create(&f.res, &book("c", "Other", 3)).await.unwrap();
        let moved = f.db.update(&f.res, &book("c", "Shared", 3)).await;
        assert!(matches!(moved, Err(Error::AlreadyExists)), "{moved:?}");
    })
    .await;
}

/// Two tenants, one descriptor, isolation by the server.
#[tokio::test]
async fn databases_are_isolated() {
    let provider = MongoProvider::new(client().await);
    let res = books();
    let (name_a, name_b) = (unique_name("a"), unique_name("b"));
    let a = provider.set_database(&name_a).await.unwrap();
    let b = provider.set_database(&name_b).await.unwrap();
    a.schema.ensure_schema(&res).await.unwrap();
    b.schema.ensure_schema(&res).await.unwrap();
    let outcome = async {
        a.create(&res, &book("ada", "Ada", 1)).await?;
        assert!(matches!(
            b.get(&res, "books/ada").await,
            Err(Error::NotFound)
        ));
        // The same unique value is free in the other database.
        b.create(&res, &book("ada", "Ada", 1)).await?;
        Ok::<_, Error>(())
    }
    .await;
    provider.drop_database(&name_a).await.unwrap();
    provider.drop_database(&name_b).await.unwrap();
    outcome.unwrap();
    assert_eq!(a.name, name_a);
    assert_eq!(a.backend, "mongodb");
    assert!(a.has_batch());
}

/// A database name goes where an identifier goes, so it is checked before the server is.
#[tokio::test]
async fn database_name_is_checked() {
    let provider = MongoProvider::new(client().await);
    for bad in ["a b", "a.b", "1abc", "a$b"] {
        let err = provider.set_database(bad).await.unwrap_err();
        assert!(matches!(err, Error::Invalid(_)), "{bad}: {err}");
    }
    assert!(provider.drop_database("").await.is_err());
    let res = books();
    let unselected = provider.set_database("").await.unwrap();
    let err = unselected.get(&res, "books/x").await.unwrap_err();
    assert!(err.to_string().contains("names no database"), "{err}");
    let _: &dyn Driver = &*unselected;
}
