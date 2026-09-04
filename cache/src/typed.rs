//! Generic JSON views over the object-safe byte-oriented Document contract.
//!
//! Strategies deliberately store encoded bytes so they remain object-safe and
//! independent of application models. This module adds type-safe JSON at the
//! outer API boundary without changing keyspace, TTL, capability, or backend
//! behavior. Multiple views may share one Document and therefore see the same
//! entries; isolate unrelated models with separate prefixes or databases.

use std::{any::type_name, marker::PhantomData, sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    CacheError, Result,
    core::{Document, Options},
};

/// A JSON-encoded view of one shared [`Document`] for model `T`.
///
/// The view owns only an [`Arc`] and a zero-sized type marker. It adds one JSON
/// encode or decode per value while every storage round trip, lease decision,
/// index update, and capability failure remains the underlying Document's.
///
/// **Trade-offs**: JSON is portable and inspectable but larger and slower than
/// a compact binary format. Views of different types over the same Document
/// can read each other's entries, so callers must isolate incompatible models.
///
/// **Scalability**: Clones share the same strategy and backend connections;
/// they introduce no locks, cache entries, or coordination state.
///
/// **Best for**: Application models that should cross the cache boundary as
/// typed Rust values while retaining language-neutral JSON storage.
pub struct Typed<T> {
    /// Shared byte-oriented storage contract.
    document: Arc<dyn Document>,
    /// Associates the view with `T` without storing or owning a model value.
    model: PhantomData<fn() -> T>,
}

impl<T> Clone for Typed<T> {
    fn clone(&self) -> Self {
        Self {
            document: self.document.clone(),
            model: PhantomData,
        }
    }
}

/// Creates a typed JSON view over `document`.
///
/// This is the Rust counterpart of runtime-go's `cache.For[T]`; `for` is a
/// reserved Rust keyword. Constructing a view performs no serialization,
/// allocation beyond the Arc clone, or backend I/O.
pub fn typed<T>(document: Arc<dyn Document>) -> Typed<T> {
    Typed::new(document)
}

impl<T> Typed<T> {
    /// Creates a typed JSON view over one shared byte-oriented Document.
    ///
    /// **Cost**: O(1) local ownership transfer. **Side effects**: None.
    /// **When to use**: Prefer this constructor in generic code; [`typed`] is
    /// the shorter application-facing spelling.
    pub fn new(document: Arc<dyn Document>) -> Self {
        Self {
            document,
            model: PhantomData,
        }
    }

    /// Returns a cloned handle to the underlying byte-oriented Document.
    ///
    /// **Cost**: One Arc increment and no backend I/O. **Side effects**: None.
    /// **When to use**: Applying middleware or performing a raw operation not
    /// represented by the typed facade.
    pub fn document(&self) -> Arc<dyn Document> {
        self.document.clone()
    }

    /// Removes one entry through the underlying Document.
    ///
    /// **Cost/side effects**: Exactly those of [`Document::delete`]; no codec
    /// work occurs. Deleting an absent entry remains successful.
    pub async fn delete(&self, id: &str) -> Result<()> {
        self.document.delete(id).await
    }

    /// Returns the IDs of all live enumerable entries.
    ///
    /// **Cost/side effects**: Exactly those of [`Document::keys`], including
    /// stale-member sweeping and `Unsupported` on backends without Sets.
    pub async fn keys(&self) -> Result<Vec<String>> {
        self.document.keys().await
    }

    /// Reports the remaining lease for one entry.
    ///
    /// **Cost/side effects**: Exactly those of [`Document::ttl`]; zero means a
    /// live permanent entry and unsupported backends remain unsupported.
    pub async fn ttl(&self, id: &str) -> Result<Duration> {
        self.document.ttl(id).await
    }
}

impl<T: Serialize> Typed<T> {
    /// JSON-encodes and stores `value`, returning its selected ID.
    ///
    /// **Cost**: One local JSON encoding plus [`Document::create`].
    /// **Side effects**: No Document call occurs if encoding fails; otherwise
    /// ID selection, index maintenance, and TTL resolution belong to Document.
    pub async fn create(&self, value: &T, options: &Options) -> Result<String> {
        let body = encode::<T>("create", value)?;
        self.document.create(&body, options).await
    }

    /// Stores `value` under an explicit caller-selected `id`.
    ///
    /// The ID argument overwrites any ID already present in `options`, matching
    /// Go's more-specific-argument precedence.
    ///
    /// **Cost**: One options clone, one JSON encoding, and
    /// [`Document::create`]. **Side effects**: Same as [`Typed::create`].
    pub async fn put(&self, id: &str, value: &T, options: &Options) -> Result<String> {
        let body = encode::<T>("put", value)?;
        let mut settled = options.clone();
        settled.id = Some(id.to_owned());
        self.document.create(&body, &settled).await
    }

    /// JSON-encodes `value` and replaces the live entry under `id`.
    ///
    /// **Cost**: One local JSON encoding plus [`Document::update`].
    /// **Side effects**: Encoding failure performs no write; Document preserves
    /// `NotFound`, TTL resolution, and conditional replacement semantics.
    pub async fn update(&self, id: &str, value: &T, options: &Options) -> Result<()> {
        let body = encode::<T>("update", value)?;
        self.document.update(id, &body, options).await
    }
}

impl<T: DeserializeOwned> Typed<T> {
    /// Reads and JSON-decodes the entry under `id` as `T`.
    ///
    /// **Cost**: One [`Document::get`] plus local JSON decoding.
    /// **Side effects**: None beyond Document's destination-independent read
    /// accounting. Cache misses propagate unchanged.
    pub async fn get(&self, id: &str) -> Result<T> {
        let mut body = Vec::new();
        self.document.get(id, &mut body).await?;
        decode::<T>("get", &body)
    }

    /// Returns every live Document entry decoded as `T`.
    ///
    /// Values are decoded in the order supplied by Document, although the
    /// Document contract does not promise a stable enumeration order. If any
    /// entry is malformed, the complete operation fails without returning a
    /// partial typed vector.
    ///
    /// **Cost**: [`Document::list`] plus one local JSON decode per value.
    /// **Side effects**: May inherit stale-member sweeping from Document.
    pub async fn list(&self) -> Result<Vec<T>> {
        self.document
            .list()
            .await?
            .into_iter()
            .enumerate()
            .map(|(index, body)| decode::<T>(&format!("list entry {index}"), &body))
            .collect()
    }
}

/// Encodes one model without allowing values into the stable error message.
fn encode<T: Serialize>(operation: &str, value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| {
        CacheError::Internal(format!(
            "typed Document {operation}: cannot encode {} as JSON",
            type_name::<T>()
        ))
    })
}

/// Decodes one model while reporting only safe type and location context.
fn decode<T: DeserializeOwned>(operation: &str, body: &[u8]) -> Result<T> {
    serde_json::from_slice(body).map_err(|error| {
        CacheError::Internal(format!(
            "typed Document {operation}: cannot decode {} as JSON at line {} column {}",
            type_name::<T>(),
            error.line(),
            error.column()
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize, Serializer};

    use super::*;
    use crate::{
        core::{
            Capabilities, DatabaseSpec, Driver, Leases, MemoryDriver, MemorySets, Sets,
            build_database,
        },
        error::CacheError,
    };

    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct User {
        name: String,
        active: bool,
    }

    struct EncodingFailure;

    impl Serialize for EncodingFailure {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom("secret serializer detail"))
        }
    }

    /// Builds the normal Document strategy with every capability needed here.
    fn document() -> Arc<dyn Document> {
        let memory = Arc::new(MemoryDriver::new());
        let driver: Arc<dyn Driver> = memory.clone();
        let leases: Arc<dyn Leases> = memory;
        let sets: Arc<dyn Sets> = Arc::new(MemorySets::new());
        build_database(
            driver,
            Capabilities::new().with_sets(sets).with_leases(leases),
            DatabaseSpec {
                prefix: "typed-tests".to_owned(),
                namespace: "models".to_owned(),
                default_ttl: Duration::from_secs(60),
                require_ttl: true,
                ..DatabaseSpec::default()
            },
        )
        .document
    }

    fn user(name: &str) -> User {
        User {
            name: name.to_owned(),
            active: true,
        }
    }

    #[tokio::test]
    async fn test_typed_create_get_round_trip() {
        let users = typed::<User>(document());

        let id = users
            .create(&user("Ada"), &Options::default().with_id("ada"))
            .await
            .unwrap();

        assert_eq!(id, "ada");
        assert_eq!(users.get("ada").await.unwrap(), user("Ada"));
    }

    #[tokio::test]
    async fn test_typed_put_explicit_id_wins() {
        let users = Typed::<User>::new(document());

        let id = users
            .put(
                "specific",
                &user("Grace"),
                &Options::default().with_id("ignored"),
            )
            .await
            .unwrap();

        assert_eq!(id, "specific");
        assert_eq!(users.get("specific").await.unwrap(), user("Grace"));
        assert!(matches!(
            users.get("ignored").await,
            Err(CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_typed_update_and_missing_propagate() {
        let users = typed::<User>(document());
        users
            .put("ada", &user("Ada"), &Options::default())
            .await
            .unwrap();

        users
            .update("ada", &user("Ada Lovelace"), &Options::default())
            .await
            .unwrap();

        assert_eq!(users.get("ada").await.unwrap(), user("Ada Lovelace"));
        assert!(matches!(
            users
                .update("missing", &user("Nobody"), &Options::default())
                .await,
            Err(CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_typed_delete_keys_and_clone_share_document() {
        let users = typed::<User>(document());
        let clone = users.clone();
        users
            .put("ada", &user("Ada"), &Options::default())
            .await
            .unwrap();

        assert_eq!(clone.keys().await.unwrap(), vec!["ada"]);
        clone.delete("ada").await.unwrap();
        assert!(matches!(users.get("ada").await, Err(CacheError::NotFound)));
    }

    #[tokio::test]
    async fn test_typed_list_decodes_values_without_partial_success() {
        let users = typed::<User>(document());
        users
            .put("ada", &user("Ada"), &Options::default())
            .await
            .unwrap();
        users
            .put("grace", &user("Grace"), &Options::default())
            .await
            .unwrap();

        let mut names: Vec<String> = users
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|user| user.name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["Ada", "Grace"]);

        users
            .document()
            .create(b"not-json-secret", &Options::default().with_id("broken"))
            .await
            .unwrap();
        let result = users.list().await;
        assert!(matches!(result, Err(CacheError::Internal(message))
            if message.contains("typed Document list entry")
                && !message.contains("not-json-secret")));
    }

    #[tokio::test]
    async fn test_typed_ttl_delegates_live_lease() {
        let users = typed::<User>(document());
        users
            .put(
                "leased",
                &user("Lease"),
                &Options::default().with_ttl(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        let ttl = users.ttl("leased").await.unwrap();
        assert!(ttl > Duration::ZERO);
        assert!(ttl <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_typed_serialization_failure_performs_no_write() {
        let raw = document();
        let values = typed::<EncodingFailure>(raw.clone());

        let result = values
            .put("failed", &EncodingFailure, &Options::default())
            .await;

        assert!(matches!(result, Err(CacheError::Internal(message))
            if message.contains("typed Document put")
                && message.contains("EncodingFailure")
                && !message.contains("secret serializer detail")));
        assert!(matches!(
            raw.get("failed", &mut Vec::new()).await,
            Err(CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_typed_deserialization_failure_has_safe_context() {
        let raw = document();
        raw.create(b"not-json-secret", &Options::default().with_id("broken"))
            .await
            .unwrap();
        let users = typed::<User>(raw);

        let result = users.get("broken").await;

        assert!(matches!(result, Err(CacheError::Internal(message))
            if message.contains("typed Document get")
                && message.contains("User")
                && !message.contains("not-json-secret")));
    }

    #[tokio::test]
    async fn test_typed_option_round_trips_json_null() {
        let raw = document();
        let optional = typed::<Option<User>>(raw.clone());
        optional
            .put("none", &None, &Options::default())
            .await
            .unwrap();

        let mut body = Vec::new();
        raw.get("none", &mut body).await.unwrap();
        assert_eq!(body, b"null");
        assert_eq!(optional.get("none").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_typed_document_exposes_shared_raw_contract() {
        let users = typed::<User>(document());
        let raw = users.document();
        raw.create(
            br#"{"name":"Raw","active":true}"#,
            &Options::default().with_id("raw"),
        )
        .await
        .unwrap();

        assert_eq!(users.get("raw").await.unwrap(), user("Raw"));
    }
}
