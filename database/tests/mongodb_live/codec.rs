//! What lands on the server, read back raw: the BSON each kind is stored as, which Go's
//! encoding fixes and anything else reading the collection depends on.

use bson::{Bson, Document, doc};
use prost_reflect::Value as P;

use crate::support::book;
use crate::with_db;

#[tokio::test]
async fn documents_are_stored_as_go_stores_them() {
    with_db(|f| async move {
        let mut big = book("big", "Big", 7);
        big.set_field_by_name("isbn", P::U64(u64::MAX));
        big.set_field_by_name("cover", P::Bytes(Vec::new().into()));
        f.db.create(&f.res, &big).await.unwrap();
        let mut small = book("small", "Small", 8);
        small.set_field_by_name("subtitle", P::String("sub".into()));
        f.db.create(&f.res, &small).await.unwrap();

        let coll = f
            .client
            .unwrap()
            .database(&f.db.name)
            .collection::<Document>("books");
        let raw = |id: &'static str| {
            let coll = coll.clone();
            async move { coll.find_one(doc! { "_id": id }).await.unwrap().unwrap() }
        };
        let big = raw("books/big").await;
        let small = raw("books/small").await;

        // The key is `_id`, and only `_id`.
        assert!(!big.contains_key("name"), "{big}");
        assert_eq!(big.get_str("_id").unwrap(), "books/big");
        assert!(matches!(big.get("pages"), Some(Bson::Int64(7))), "{big}");
        assert!(matches!(big.get("status"), Some(Bson::Int32(2))), "{big}");
        assert!(matches!(big.get("price"), Some(Bson::Double(_))), "{big}");
        assert!(
            matches!(big.get("in_print"), Some(Bson::Boolean(true))),
            "{big}"
        );
        assert!(
            matches!(big.get("create_time"), Some(Bson::DateTime(_))),
            "{big}"
        );
        assert!(
            matches!(big.get("update_time"), Some(Bson::DateTime(_))),
            "{big}"
        );
        // uint64 past i64::MAX is a Decimal128; below it an int64.
        match big.get("isbn") {
            Some(Bson::Decimal128(d)) => assert_eq!(d.to_string(), u64::MAX.to_string()),
            other => panic!("isbn stored as {other:?}"),
        }
        assert!(matches!(
            small.get("isbn"),
            Some(Bson::Int64(9_780_441_013_593))
        ));
        // An unset optional field, and empty bytes as Go's nil slice, are null.
        assert_eq!(big.get("subtitle"), Some(&Bson::Null));
        assert_eq!(big.get("cover"), Some(&Bson::Null));
        assert_eq!(small.get_str("subtitle").unwrap(), "sub");
        match small.get("cover") {
            Some(Bson::Binary(b)) => assert_eq!(b.bytes, b"png"),
            other => panic!("cover stored as {other:?}"),
        }
        assert_eq!(
            small.get_datetime("create_time").unwrap(),
            small.get_datetime("update_time").unwrap()
        );

        // The unique index is named as Go names it.
        let indexes = coll.list_index_names().await.unwrap();
        assert!(indexes.contains(&"uniq_title".to_owned()), "{indexes:?}");
    })
    .await;
}
