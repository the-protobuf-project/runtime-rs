//! Offline checks of the codec, the filter translation and the client config: what the
//! live suite verifies against a server, pinned here without one so CI's default run keeps
//! them. Go's `TestDatabaseNameIsChecked` and `TestCredentialsAreEscaped` live here too.

#[path = "../../tests/support/records.rs"]
mod support;

use std::time::Duration;

use bson::{Bson, Decimal128, doc, oid::ObjectId};
use prost_reflect::Value as P;

use super::codec::{from_bson, from_document, key_bson, key_string, to_document};
use super::filter::{parse_filter, sort_of};
use super::{MongoClient, MongoConfig, MongoProvider};
use crate::Error;
use crate::records::{
    Column, Kind, ListOptions, Provider, Resource, Value, core, message_to_columns,
};

#[test]
fn documents_key_by_id_and_type_by_kind() {
    let res = support::books();
    let mut msg = support::book("dune", "Dune", 412);
    msg.set_field_by_name("isbn", P::U64(u64::MAX));
    let mut cols = message_to_columns(&res, &msg).unwrap();
    core::fill_managed(&res, &mut cols, true);
    let doc = to_document(&res, &cols).unwrap();
    assert_eq!(doc.keys().next().map(String::as_str), Some("_id"));
    assert_eq!(doc.get_str("_id").unwrap(), "books/dune");
    assert!(!doc.contains_key("name"));
    assert_eq!(doc.get("pages"), Some(&Bson::Int64(412)));
    assert_eq!(doc.get("status"), Some(&Bson::Int32(2)));
    assert_eq!(doc.get("subtitle"), Some(&Bson::Null));
    assert!(matches!(doc.get("isbn"), Some(Bson::Decimal128(_))));
    assert!(matches!(doc.get("create_time"), Some(Bson::DateTime(_))));

    let back = from_document(&res, doc).unwrap();
    for field in [
        "name", "title", "pages", "price", "in_print", "cover", "status", "isbn",
    ] {
        assert_eq!(
            back.get_field_by_name(field),
            msg.get_field_by_name(field),
            "{field}"
        );
    }
    assert!(!back.has_field_by_name("subtitle"));
}

#[test]
fn a_row_without_a_key_is_refused() {
    let res = support::books();
    let mut cols = message_to_columns(&res, &support::book("x", "X", 1)).unwrap();
    cols.remove("name");
    assert!(matches!(to_document(&res, &cols), Err(Error::Invalid(_))));
    cols.insert("name".into(), Value::Null);
    assert!(matches!(to_document(&res, &cols), Err(Error::Invalid(_))));
}

#[test]
fn stored_values_widen_back() {
    let big: Decimal128 = u64::MAX.to_string().parse().unwrap();
    assert_eq!(from_bson(Bson::Decimal128(big)), Value::Uint(u64::MAX));
    assert_eq!(from_bson(Bson::Int32(7)), Value::Int(7));
    assert_eq!(from_bson(Bson::Null), Value::Null);
    let id = ObjectId::new();
    assert_eq!(from_bson(Bson::ObjectId(id)), Value::String(id.to_hex()));
    assert_eq!(key_string(&Bson::Int64(42)), "42");
    assert_eq!(key_string(&Bson::String("books/a".into())), "books/a");
    assert_eq!(
        key_bson(&support::books(), "books/a"),
        Bson::String("books/a".into())
    );
}

#[test]
fn filters_become_typed_queries() {
    let res = support::books();
    let p = |expr: &str| parse_filter(&res, &ListOptions::default().filter(expr));
    let q = |expr: &str| p(expr).unwrap();
    assert_eq!(q(""), doc! {});
    assert_eq!(q("name = books/a"), doc! { "_id": { "$eq": "books/a" } });
    assert_eq!(q("pages >= 20"), doc! { "pages": { "$gte": 20_i64 } });
    assert_eq!(
        q("in_print = true AND price < 2.5"),
        doc! { "$and": [ { "in_print": { "$eq": true } }, { "price": { "$lt": 2.5 } } ] }
    );
    assert_eq!(
        q("status != PUBLISHED"),
        doc! { "status": { "$nin": [Bson::Int32(2), Bson::Null] } }
    );
    // NOT is `$nor`, which, as Go's NOT coalesce, holds where the field is null.
    assert_eq!(
        q("NOT pages < 9"),
        doc! { "$nor": [ { "pages": { "$lt": 9_i64 } } ] }
    );
    // Contains and free text are case-insensitive regexes of the escaped value.
    assert_eq!(
        q("title:a.b"),
        doc! { "title": { "$regex": "a\\.b", "$options": "i" } }
    );
    assert_eq!(
        q("dune"),
        doc! { "$or": [
            { "subtitle": { "$regex": "dune", "$options": "i" } },
            { "title": { "$regex": "dune", "$options": "i" } },
        ] }
    );
    // Bytes compare as the binary they are stored as.
    let cover = q("cover = \"png\"").get_document("cover").unwrap().clone();
    assert!(matches!(cover.get("$eq"), Some(Bson::Binary(_))), "{cover}");
    for bad in ["nosuch = 1", "pages = x", "title < a", "pages : 1"] {
        assert!(matches!(p(bad), Err(Error::Invalid(_))), "{bad}");
    }
}

#[test]
fn keys_encode_as_the_key_column_stores_them() {
    let ts = prost_reflect::DescriptorPool::global()
        .get_message_by_name("google.protobuf.Timestamp")
        .unwrap();
    let key = Column::new("seconds", Kind::Int).primary_key();
    let ints = Resource::new(ts, "t", "seconds", vec![key]);
    assert_eq!(key_bson(&ints, "42"), Bson::Int64(42));
    assert_eq!(key_bson(&ints, "x"), Bson::String("x".into()));
}

#[test]
fn orders_become_sorts() {
    let res = support::books();
    assert_eq!(sort_of(&res, ""), doc! { "_id": 1 });
    assert_eq!(sort_of(&res, "name desc"), doc! { "_id": -1 });
    assert_eq!(sort_of(&res, "NAME"), doc! { "_id": 1 });
    assert_eq!(sort_of(&res, "pages DESC"), doc! { "pages": -1, "_id": 1 });
    assert_eq!(sort_of(&res, "nosuch desc"), doc! { "_id": 1 });
}

#[tokio::test]
async fn config_needs_an_address_and_keeps_credentials_out_of_the_uri() {
    assert!(matches!(
        MongoConfig::default().options().await,
        Err(Error::Invalid(_))
    ));
    let config = MongoConfig {
        username: "user".into(),
        password: "p@ss/word:1".into(),
        auth_source: "admin".into(),
        max_pool_size: 7,
        connect_timeout: Duration::from_millis(250),
        ..MongoConfig::address("mongodb://127.0.0.1:1")
    };
    let opts = config.options().await.unwrap();
    assert_eq!(opts.hosts.len(), 1);
    assert_eq!(opts.hosts[0].to_string(), "127.0.0.1:1");
    let credential = opts.credential.unwrap();
    assert_eq!(credential.password.as_deref(), Some("p@ss/word:1"));
    assert_eq!(credential.source.as_deref(), Some("admin"));
    assert_eq!(opts.max_pool_size, Some(7));
    assert_eq!(
        opts.server_selection_timeout,
        Some(Duration::from_millis(250))
    );
    assert!(!format!("{config:?}").contains("word"));
}

/// A password holding `@` or `/` must fail reaching 127.0.0.1:1, not some host called
/// `word:1`, and not fail to parse.
#[tokio::test]
async fn credentials_are_escaped() {
    let err = MongoClient::connect(MongoConfig {
        username: "user".into(),
        password: "p@ss/word:1".into(),
        connect_timeout: Duration::from_millis(200),
        ..MongoConfig::address("127.0.0.1:1")
    })
    .await
    .expect_err("nothing listens on port 1");
    assert!(!err.to_string().contains("word"), "{err}");
}

/// The name is checked before the server is reached, so no server is needed to see it.
#[tokio::test]
async fn database_name_is_checked() {
    let opts = MongoConfig::address("127.0.0.1:1").options().await.unwrap();
    let lazy = mongodb::Client::with_options(opts).unwrap();
    let provider = MongoProvider::new(MongoClient::wrap(lazy));
    assert_eq!(provider.backend(), "mongodb");
    for bad in ["a b", "a-b", "9lives", &"x".repeat(64)] {
        let err = provider.set_database(bad).await.unwrap_err();
        assert!(matches!(err, Error::Invalid(_)), "{bad}: {err}");
    }
    assert!(matches!(
        provider.drop_database("").await,
        Err(Error::Invalid(_))
    ));
}
