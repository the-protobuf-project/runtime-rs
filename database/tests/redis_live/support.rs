//! A database over the live server, namespaced per test, and the `Book` fixture as a
//! generated type would carry it.

use std::sync::Arc;

use database::records::{DB, Provider, Resource};
use database::redis::RedisProvider;
use prost_reflect::{DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use redis::aio::ConnectionManager;

use crate::records;

/// The server's address.
pub fn address() -> String {
    std::env::var("RUNTIME_DATABASE_REDIS_ADDRESS").unwrap_or_else(|_| "127.0.0.1:16381".into())
}

/// A connection manager to the live server; failing to reach it fails the test.
pub async fn connect() -> ConnectionManager {
    let client = redis::Client::open(format!("redis://{}/", address())).expect("a valid address");
    ConnectionManager::new(client)
        .await
        .unwrap_or_else(|e| panic!("no Redis at {}: {e}", address()))
}

/// A prefix no other test or run shares.
pub fn prefix() -> String {
    format!(
        "itest-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    )
}

/// A provider under a fresh prefix.
pub async fn provider() -> RedisProvider {
    RedisProvider::new(connect().await).with_prefix(prefix())
}

/// Database `tenant_a` under a fresh prefix, and the books resource.
pub async fn setup() -> (DB, Resource) {
    let db = provider().await.set_database("tenant_a").await.unwrap();
    (db, records::books())
}

/// Drops what the test wrote.
pub async fn teardown(db: &DB, res: &Resource) {
    db.schema.drop_schema(res).await.unwrap();
    db.close().await.unwrap();
}

/// A book keyed `books/<id>` with unique title `title`.
pub fn book(id: &str, title: &str) -> DynamicMessage {
    records::book(id, title, 100)
}

/// A book with `cover` bytes and `isbn`: the values JSON would corrupt.
pub fn book_with(id: &str, title: &str, cover: &[u8], isbn: u64) -> DynamicMessage {
    let mut m = book(id, title);
    m.set_field_by_name("cover", Value::Bytes(cover.to_vec().into()));
    m.set_field_by_name("isbn", Value::U64(isbn));
    m
}

/// A string field.
pub fn field(m: &DynamicMessage, name: &str) -> String {
    records::string_field(m, name)
}

/// `test.v1.Book` as prost would generate it, for the typed view.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbBook {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub title: String,
    #[prost(int32, tag = "3")]
    pub pages: i32,
    #[prost(double, tag = "4")]
    pub price: f64,
    #[prost(bool, tag = "5")]
    pub in_print: bool,
    #[prost(bytes = "vec", tag = "6")]
    pub cover: Vec<u8>,
    #[prost(int32, tag = "7")]
    pub status: i32,
    #[prost(string, optional, tag = "8")]
    pub subtitle: Option<String>,
    #[prost(uint64, tag = "9")]
    pub isbn: u64,
    #[prost(message, optional, tag = "10")]
    pub create_time: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "11")]
    pub update_time: Option<prost_types::Timestamp>,
}

impl ReflectMessage for PbBook {
    fn descriptor(&self) -> MessageDescriptor {
        records::book_descriptor()
    }
}

/// The books resource, shared, for a typed view.
pub fn shared(res: &Resource) -> Arc<Resource> {
    Arc::new(res.clone())
}
