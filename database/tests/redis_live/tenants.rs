//! Databases and prefixes: tenants isolated by one descriptor, names checked, and one
//! connection shared with the cache.

use std::sync::Arc;

use cache::drivers::redis::{RedisClient, RedisConfig, RedisProvider as CacheProvider};
use database::Error;
use database::records::{ListOptions, Provider};
use database::redis::RedisProvider;

use crate::support::{self, book, teardown};

/// Two tenants, one descriptor: the reason `set_database` exists.
#[tokio::test]
async fn databases_are_isolated() {
    let p = support::provider().await;
    let res = crate::records::books();
    let a = p.set_database("tenant_a").await.unwrap();
    let b = p.set_database("tenant_b").await.unwrap();
    assert_eq!((a.backend, a.name.as_str()), ("redis", "tenant_a"));

    a.create(&res, &book("ada", "Ada")).await.unwrap();
    assert!(matches!(
        b.get(&res, "books/ada").await,
        Err(Error::NotFound)
    ));
    b.create(&res, &book("ada", "Ada")).await.unwrap();
    assert_eq!(a.count(&res, &ListOptions::default()).await.unwrap(), 1);
    teardown(&a, &res).await;
    teardown(&b, &res).await;
}

#[tokio::test]
async fn a_database_name_goes_where_an_identifier_goes() {
    let p = support::provider().await;
    assert!(matches!(
        p.set_database("a:b").await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(p.backend(), "redis");
}

/// The cache's connection manager serves the record store: one connection, two layers, as
/// Go's cache and database share one client.
#[tokio::test]
async fn the_cache_s_connection_serves_the_records() {
    let client = Arc::new(
        RedisClient::connect(RedisConfig {
            address: support::address(),
            ..RedisConfig::default()
        })
        .await
        .unwrap(),
    );
    let prefix = support::prefix();
    let caches = CacheProvider::new(client.clone(), cache::Config::default());
    let records =
        RedisProvider::new(client.connection_manager().await.unwrap()).with_prefix(prefix.as_str());

    let db = records.set_database("tenant_a").await.unwrap();
    let res = crate::records::books();
    db.create(&res, &book("ada", "Ada")).await.unwrap();
    let kv = cache::Provider::set_database(&caches, "kv").await.unwrap();
    kv.volatile
        .set("k", b"v", &cache::core::Options::default())
        .await
        .unwrap();
    assert!(db.exists(&res, "books/ada").await.unwrap());
    kv.volatile.delete("k").await.unwrap();
    kv.close().await.unwrap();

    client.close().await;
    assert!(
        client.connection_manager().await.is_err(),
        "a closed client lends nothing"
    );
    // The handle the records hold shares the connection, not the client's lifecycle.
    assert!(db.exists(&res, "books/ada").await.unwrap());
    teardown(&db, &res).await;
}
