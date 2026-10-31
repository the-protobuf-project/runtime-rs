//! Compile-time regression gate for the pinned async Memcached dependency.
//!
//! The production backend lives in the adjacent `memcached` module. This test
//! verifies that its experimental dependency still exposes the atomic
//! operations, byte values, per-server batching and pooling, and timeout
//! controls the private adapter uses. Production routes among single-server
//! clients with Go's CRC32 rule and must not adopt the dependency's
//! incompatible default router.

use std::time::Duration;

use memcache::exp::{AsyncMetaClient, Get, GetStatus, MutationStatus, Op, Set, SetMode, Ttl};

/// Proves cloned clients can be shared by concurrent Driver futures.
fn assert_shareable<T: Clone + Send + Sync>() {}

/// Proves the async meta client exposes every operation and control required
/// before the real adapter is implemented.
///
/// Construction resolves numeric loopback addresses but dials lazily. The test
/// therefore needs no Memcached service and performs no storage I/O.
#[tokio::test]
async fn test_memcached_async_client_required_api_compiles() {
    assert_shareable::<AsyncMetaClient>();

    let client = AsyncMetaClient::builder()
        .max_idle(16)
        .connect_timeout(Some(Duration::from_millis(200)))
        .io_timeout(None)
        .connect_async("127.0.0.1:11211")
        .await
        .unwrap();
    let second_server = AsyncMetaClient::builder()
        .max_idle(16)
        .connect_async("127.0.0.1:11212")
        .await
        .unwrap();
    drop(second_server);

    let get = client.get(b"get").into_operation();
    assert!(get.value);

    let set = client
        .set(b"set", b"value")
        .ttl(Ttl::secs(60))
        .into_operation();
    assert_eq!(set.value, b"value");
    assert_eq!(set.ttl, Some(Ttl::secs(60)));
    assert_eq!(set.mode, SetMode::Set);

    let add = client.set(b"add", b"value").add().into_operation();
    assert_eq!(add.mode, SetMode::Add);

    let replace = client.set(b"replace", b"value").replace().into_operation();
    assert_eq!(replace.mode, SetMode::Replace);

    let touch = client
        .get(b"touch")
        .without_value()
        .touch(Ttl::secs(60))
        .into_operation();
    assert!(!touch.value);
    assert_eq!(touch.touch, Some(Ttl::secs(60)));

    let delete = client.delete(b"delete").into_operation();
    assert_eq!(delete.key, b"delete");

    // One per-server batch is a single pipelined exchange whose results retain
    // operation order. The production router will group batches by server.
    let batch: Vec<Op> = vec![
        Get::new(b"bulk-get").into(),
        Set::new(b"bulk-set", b"value").into(),
    ];
    drop(client.run_batch(batch));

    // NOOP reaches this one server and is suitable for a targeted health check.
    drop(client.noop());

    // Pin the semantic outcomes needed for miss and conditional-write maps.
    let _get_statuses = [GetStatus::Hit, GetStatus::Miss];
    let _mutation_statuses = [
        MutationStatus::Applied,
        MutationStatus::NotFound,
        MutationStatus::AlreadyExists,
    ];
}
