//! Compile-time compatibility gate for the provisional async Memcached client.
//!
//! This is not the Memcached backend. It verifies that the pinned dependency
//! exposes the atomic operations, byte values, per-server batching and pooling,
//! and timeout controls required by the approved boundary. The production
//! adapter will route among these single-server clients with Go's CRC32 rule;
//! it must not use the dependency's incompatible jump-hash router. Keeping the
//! probe test-only prevents an incomplete driver from entering the public API.

use std::time::Duration;

use memcache::exp::{AsyncMetaClient, Get, GetStatus, MutationStatus, Op, Set, SetMode};

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

    let client = AsyncMetaClient::connect("127.0.0.1:11211")
        .await
        .unwrap()
        .with_max_idle(16)
        .with_connect_timeout(Some(Duration::from_millis(200)))
        .with_io_timeout(None);
    let second_server = AsyncMetaClient::connect("127.0.0.1:11212")
        .await
        .unwrap()
        .with_max_idle(16);
    drop(second_server);

    let get = client.get(b"get".to_vec()).into_operation();
    assert!(get.value);

    let set = client
        .set(b"set".to_vec(), b"value".to_vec())
        .ttl(60)
        .into_operation();
    assert_eq!(set.value, b"value");
    assert_eq!(set.ttl, Some(60));
    assert_eq!(set.mode, SetMode::Set);

    let add = client
        .set(b"add".to_vec(), b"value".to_vec())
        .add()
        .into_operation();
    assert_eq!(add.mode, SetMode::Add);

    let replace = client
        .set(b"replace".to_vec(), b"value".to_vec())
        .replace()
        .into_operation();
    assert_eq!(replace.mode, SetMode::Replace);

    let touch = client
        .get(b"touch".to_vec())
        .without_value()
        .touch(60)
        .into_operation();
    assert!(!touch.value);
    assert_eq!(touch.touch, Some(60));

    let delete = client.delete(b"delete".to_vec()).into_operation();
    assert_eq!(delete.key, b"delete");

    // One per-server batch is a single pipelined exchange whose results retain
    // operation order. The production router will group batches by server.
    let batch: Vec<Op> = vec![
        Get::new(b"bulk-get".to_vec()).into(),
        Set::new(b"bulk-set".to_vec(), b"value".to_vec()).into(),
    ];
    drop(client.run_batch(batch));

    // NOOP reaches this one server and is suitable for a targeted health check.
    drop(client.noop());

    // Pin the semantic outcomes needed for miss and conditional-write maps.
    let _get_statuses = [GetStatus::Hit, GetStatus::Miss];
    let _mutation_statuses = [
        MutationStatus::Stored,
        MutationStatus::NotFound,
        MutationStatus::AlreadyExists,
    ];
}
