// The envelope is introduced one reviewed milestone before AsideImpl consumes
// it. Remove this temporary allowance when the strategy implementation lands.
#![cfg_attr(not(test), allow(dead_code))]

//! Internal envelope used by the read-through Aside strategy.
//!
//! This deliberately mirrors `runtime-go/cache/core/envelope.go`. The backend
//! understands only bytes and one hard expiry, while Aside also needs to know
//! whether those bytes remember an absence and when a value becomes stale before
//! backend expiry. Keeping that metadata here preserves the Driver/Strategy
//! boundary and makes Rust and Go Aside entries wire-compatible.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

use crate::core::{Driver, Keyspace, Loader, Options};
use crate::{CacheError, Result};

use super::flight::Flight;

/// Private read-through strategy under construction.
///
/// The Provider will eventually expose this through [`crate::core::Aside`]
/// rather than letting callers assemble coordination resources independently.
/// In particular, `flight` must be shared by every Aside view of one database,
/// matching the Go build architecture.
// This milestone exercises only Driver reads and invalidation. The remaining
// dependencies become live in the load/store and refresh milestones.
#[allow(dead_code)]
pub(crate) struct AsideImpl {
    driver: Arc<dyn Driver>,
    keyspace: Keyspace,
    loader: Loader,
    default_ttl: Duration,
    default_stale: Duration,
    require_ttl: bool,
    flight: Arc<Flight>,
    negative_ttl: Duration,
}

impl AsideImpl {
    /// Wires dependencies without performing I/O or allocating cache storage.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        driver: Arc<dyn Driver>,
        keyspace: Keyspace,
        loader: Loader,
        default_ttl: Duration,
        default_stale: Duration,
        require_ttl: bool,
        flight: Arc<Flight>,
        negative_ttl: Duration,
    ) -> Self {
        Self {
            driver,
            keyspace,
            loader,
            default_ttl,
            default_stale,
            require_ttl,
            flight,
            negative_ttl,
        }
    }

    /// Reads and decodes one Aside entry.
    ///
    /// **Cost**: Exactly one Driver GET and no index operations.
    /// **Side effects**: None. A stale value is only classified here; scheduling
    /// its refresh belongs to a later orchestration milestone.
    async fn read(&self, id: &str) -> Result<StoredEntry> {
        let key = self.keyspace.aside_entry(id);
        let frame = self.driver.get(&key).await?;
        unpack(&frame, unix_millis(SystemTime::now())?)
    }

    /// Deletes either a cached value or remembered absence.
    ///
    /// Both states use one entry key, so invalidation remains one Driver DELETE
    /// and makes a newly created source value visible immediately.
    async fn invalidate_entry(&self, id: &str) -> Result<()> {
        let key = self.keyspace.aside_entry(id);
        self.driver.delete(&[&key]).await
    }
}

/// Resolves the lease for a load that will write an Aside entry.
///
/// The ordering is the cache-wide safety contract: an operation-specific TTL
/// wins, then deliberate permanence, then the configured default, followed by
/// `NoTTL` when leases are mandatory, and finally implicit permanence when they
/// are not. This check happens before invoking a Loader so an invalid write does
/// not first perform expensive source work.
fn resolve_ttl(default_ttl: Duration, require_ttl: bool, opts: &Options) -> Result<Duration> {
    if let Some(ttl) = opts.ttl {
        return Ok(ttl);
    }

    if opts.permanent {
        return Ok(Duration::ZERO);
    }

    if !default_ttl.is_zero() {
        return Ok(default_ttl);
    }

    if require_ttl {
        return Err(CacheError::NoTTL);
    }

    Ok(Duration::ZERO)
}

/// Resolves the stale-serving window independently of the entry lease.
fn resolve_stale(default_stale: Duration, opts: &Options) -> Duration {
    match opts.stale {
        Some(stale) => stale,
        None => default_stale,
    }
}

/// Separates the deadline carried by the envelope from backend hard expiry.
#[derive(Debug, Eq, PartialEq)]
struct Freshness {
    /// A zero duration means no separate freshness deadline is stored.
    fresh_for: Duration,

    /// The TTL passed to Driver. With stale serving this is `TTL + Stale`.
    hard_ttl: Duration,
}

/// Calculates envelope freshness and backend expiry without reading a clock.
///
/// Without a stale window, the backend removes the value at TTL and no envelope
/// deadline is needed. With one, the envelope becomes stale at TTL while the
/// backend retains it through `TTL + Stale`. A permanent entry has neither
/// deadline because stale serving has no meaningful boundary without a TTL.
fn freshness(ttl: Duration, stale: Duration) -> Result<Freshness> {
    if ttl.is_zero() {
        return Ok(Freshness {
            fresh_for: Duration::ZERO,
            hard_ttl: Duration::ZERO,
        });
    }

    if stale.is_zero() {
        return Ok(Freshness {
            fresh_for: Duration::ZERO,
            hard_ttl: ttl,
        });
    }

    let hard_ttl = ttl.checked_add(stale).ok_or_else(|| {
        CacheError::Internal("Aside TTL and stale window overflow Duration".to_owned())
    })?;
    Ok(Freshness {
        fresh_for: ttl,
        hard_ttl,
    })
}

/// Converts a relative freshness window into Go's Unix-millisecond deadline.
///
/// Clock access is isolated here so the envelope codec remains deterministic.
/// All conversions are checked; an unrepresentable deadline is an explicit
/// error rather than a wrapped timestamp that could make stale data look fresh.
fn fresh_until_millis(now: SystemTime, fresh_for: Duration) -> Result<i64> {
    if fresh_for.is_zero() {
        return Ok(0);
    }

    let deadline = now.checked_add(fresh_for).ok_or_else(|| {
        CacheError::Internal("Aside freshness deadline exceeds SystemTime".to_owned())
    })?;
    unix_millis(deadline)
}

fn unix_millis(time: SystemTime) -> Result<i64> {
    let since_epoch = time.duration_since(UNIX_EPOCH).map_err(|error| {
        CacheError::Internal(format!("Aside freshness deadline predates Unix epoch: {error}"))
    })?;
    i64::try_from(since_epoch.as_millis()).map_err(|error| {
        CacheError::Internal(format!(
            "Aside freshness deadline does not fit Unix milliseconds: {error}"
        ))
    })
}

/// Go-compatible stored representation.
///
/// Field names and omission rules match the reference implementation:
/// `v` carries already-encoded JSON, `f` is Unix milliseconds, and `x` marks a
/// loader-reported absence. A legitimate JSON `null` remains distinct from an
/// absence because only `x` has absence semantics.
#[derive(Deserialize, Serialize)]
struct Envelope {
    #[serde(
        rename = "v",
        default,
        deserialize_with = "deserialize_present_raw_value",
        skip_serializing_if = "Option::is_none"
    )]
    value: Option<Box<RawValue>>,

    #[serde(rename = "f", default, skip_serializing_if = "is_zero")]
    fresh: i64,

    #[serde(rename = "x", default, skip_serializing_if = "is_false")]
    void: bool,
}

/// The result of decoding one Aside envelope.
#[derive(Debug, Eq, PartialEq)]
enum StoredEntry {
    Value { body: Vec<u8>, stale: bool },
    Void,
}

/// Wraps a JSON-encoded loader value in the Go-compatible Aside envelope.
///
/// The Loader contract returns bytes, so this boundary validates that they hold
/// one complete JSON value before storing them as Go's `json.RawMessage` would.
/// Callers with arbitrary binary data must JSON-encode it first.
fn pack_value(body: &[u8], fresh_until_ms: i64) -> Result<Vec<u8>> {
    let json = String::from_utf8(body.to_vec())
        .map_err(|error| invalid_envelope(format!("loader value is not UTF-8 JSON: {error}")))?;
    let value = RawValue::from_string(json)
        .map_err(|error| invalid_envelope(format!("loader value is not valid JSON: {error}")))?;

    encode(&Envelope {
        value: Some(value),
        fresh: fresh_until_ms,
        void: false,
    })
}

/// Frames a remembered absence under the same entry key as a normal value.
fn pack_void() -> Result<Vec<u8>> {
    encode(&Envelope {
        value: None,
        fresh: 0,
        void: true,
    })
}

fn encode(envelope: &Envelope) -> Result<Vec<u8>> {
    serde_json::to_vec(envelope)
        .map_err(|error| invalid_envelope(format!("cannot encode envelope: {error}")))
}

/// Decodes a Go-compatible envelope relative to the supplied Unix time.
///
/// Clock access stays outside this codec so boundary behavior is deterministic
/// in tests. As in Go, `void` takes precedence, zero means no separate freshness
/// deadline, and equality remains fresh until time advances past the deadline.
fn unpack(frame: &[u8], now_ms: i64) -> Result<StoredEntry> {
    let envelope: Envelope = serde_json::from_slice(frame)
        .map_err(|error| invalid_envelope(format!("cannot decode envelope: {error}")))?;

    if envelope.void {
        return Ok(StoredEntry::Void);
    }

    let body = match envelope.value {
        Some(value) => value.get().as_bytes().to_vec(),
        None => Vec::new(),
    };

    Ok(StoredEntry::Value {
        body,
        stale: envelope.fresh != 0 && now_ms > envelope.fresh,
    })
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Preserves the distinction between an absent `v` field and present JSON
/// `null`. Serde's ordinary `Option` decoder collapses both to `None`, whereas
/// Go's `json.RawMessage` retains `null` and uses `x` for remembered absence.
fn deserialize_present_raw_value<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Box<RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

fn invalid_envelope(reason: String) -> CacheError {
    CacheError::Internal(format!("invalid Aside envelope: {reason}"))
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use crate::core::MemoryDriver;

    use super::*;

    fn test_aside(driver: Arc<MemoryDriver>) -> AsideImpl {
        let loader: Loader = Arc::new(|_| async { Ok(b"null".to_vec()) }.boxed());
        AsideImpl::new(
            driver,
            Keyspace::new("test", "db", 0, false),
            loader,
            Duration::from_secs(60),
            Duration::ZERO,
            false,
            Arc::new(Flight::new(8, Duration::from_secs(1))),
            Duration::from_secs(30),
        )
    }

    #[tokio::test]
    async fn test_aside_read_fresh_value_returns_payload() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("id");
        let frame = pack_value(br#"{"name":"value"}"#, i64::MAX).unwrap();
        driver
            .set(&key, &frame, Duration::from_secs(60))
            .await
            .unwrap();

        let entry = aside.read("id").await.unwrap();

        assert_eq!(
            entry,
            StoredEntry::Value {
                body: br#"{"name":"value"}"#.to_vec(),
                stale: false,
            }
        );
    }

    #[tokio::test]
    async fn test_aside_read_expired_freshness_returns_stale_payload() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("id");
        let frame = pack_value(br#""old""#, 1).unwrap();
        driver
            .set(&key, &frame, Duration::from_secs(60))
            .await
            .unwrap();

        let entry = aside.read("id").await.unwrap();

        assert_eq!(
            entry,
            StoredEntry::Value {
                body: br#""old""#.to_vec(),
                stale: true,
            }
        );
    }

    #[tokio::test]
    async fn test_aside_read_remembered_absence_returns_void() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("missing");
        driver
            .set(&key, &pack_void().unwrap(), Duration::from_secs(30))
            .await
            .unwrap();

        let entry = aside.read("missing").await.unwrap();

        assert_eq!(entry, StoredEntry::Void);
    }

    #[tokio::test]
    async fn test_aside_read_driver_miss_returns_not_found() {
        let aside = test_aside(Arc::new(MemoryDriver::new()));

        let result = aside.read("missing").await;

        assert!(matches!(result, Err(CacheError::NotFound)));
    }

    #[tokio::test]
    async fn test_aside_read_malformed_envelope_returns_error() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("bad");
        driver
            .set(&key, b"not-json", Duration::from_secs(60))
            .await
            .unwrap();

        let result = aside.read("bad").await;

        assert!(matches!(result, Err(CacheError::Internal(_))));
    }

    #[tokio::test]
    async fn test_aside_invalidate_value_deletes_entry() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("id");
        let frame = pack_value(br#""value""#, 0).unwrap();
        driver
            .set(&key, &frame, Duration::from_secs(60))
            .await
            .unwrap();

        aside.invalidate_entry("id").await.unwrap();

        assert!(!driver.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn test_aside_invalidate_void_deletes_remembered_absence() {
        let driver = Arc::new(MemoryDriver::new());
        let aside = test_aside(driver.clone());
        let key = Keyspace::new("test", "db", 0, false).aside_entry("missing");
        driver
            .set(&key, &pack_void().unwrap(), Duration::from_secs(30))
            .await
            .unwrap();

        aside.invalidate_entry("missing").await.unwrap();

        assert!(!driver.exists(&key).await.unwrap());
    }

    #[test]
    fn test_aside_ttl_explicit_overrides_permanent_and_default() {
        let opts = Options::default()
            .with_ttl(Duration::from_secs(5))
            .permanent();

        let ttl = resolve_ttl(Duration::from_secs(30), true, &opts).unwrap();

        assert_eq!(ttl, Duration::from_secs(5));
    }

    #[test]
    fn test_aside_ttl_permanent_overrides_default() {
        let opts = Options::default().permanent();

        let ttl = resolve_ttl(Duration::from_secs(30), true, &opts).unwrap();

        assert_eq!(ttl, Duration::ZERO);
    }

    #[test]
    fn test_aside_ttl_default_applies_when_operation_omits_ttl() {
        let ttl = resolve_ttl(
            Duration::from_secs(30),
            true,
            &Options::default(),
        )
        .unwrap();

        assert_eq!(ttl, Duration::from_secs(30));
    }

    #[test]
    fn test_aside_ttl_required_without_lease_returns_error() {
        let result = resolve_ttl(Duration::ZERO, true, &Options::default());

        assert!(matches!(result, Err(CacheError::NoTTL)));
    }

    #[test]
    fn test_aside_ttl_optional_without_lease_is_permanent() {
        let ttl = resolve_ttl(Duration::ZERO, false, &Options::default()).unwrap();

        assert_eq!(ttl, Duration::ZERO);
    }

    #[test]
    fn test_aside_stale_explicit_overrides_default() {
        let opts = Options::default().with_stale(Duration::from_secs(5));

        let stale = resolve_stale(Duration::from_secs(30), &opts);

        assert_eq!(stale, Duration::from_secs(5));
    }

    #[test]
    fn test_aside_stale_default_applies_when_operation_omits_stale() {
        let stale = resolve_stale(Duration::from_secs(30), &Options::default());

        assert_eq!(stale, Duration::from_secs(30));
    }

    #[test]
    fn test_aside_freshness_without_stale_uses_ttl_as_hard_expiry() {
        let value = freshness(Duration::from_secs(10), Duration::ZERO).unwrap();

        assert_eq!(
            value,
            Freshness {
                fresh_for: Duration::ZERO,
                hard_ttl: Duration::from_secs(10),
            }
        );
    }

    #[test]
    fn test_aside_freshness_with_stale_extends_hard_expiry() {
        let value = freshness(Duration::from_secs(10), Duration::from_secs(5)).unwrap();

        assert_eq!(
            value,
            Freshness {
                fresh_for: Duration::from_secs(10),
                hard_ttl: Duration::from_secs(15),
            }
        );
    }

    #[test]
    fn test_aside_freshness_permanent_ignores_stale_window() {
        let value = freshness(Duration::ZERO, Duration::from_secs(5)).unwrap();

        assert_eq!(
            value,
            Freshness {
                fresh_for: Duration::ZERO,
                hard_ttl: Duration::ZERO,
            }
        );
    }

    #[test]
    fn test_aside_freshness_overflow_returns_error() {
        let result = freshness(Duration::MAX, Duration::from_nanos(1));

        assert!(matches!(result, Err(CacheError::Internal(_))));
    }

    #[test]
    fn test_aside_freshness_deadline_converts_to_unix_millis() {
        let now = UNIX_EPOCH + Duration::from_millis(1_000);

        let deadline = fresh_until_millis(now, Duration::from_millis(250)).unwrap();

        assert_eq!(deadline, 1_250);
    }

    #[test]
    fn test_aside_freshness_zero_has_no_envelope_deadline() {
        let deadline = fresh_until_millis(SystemTime::now(), Duration::ZERO).unwrap();

        assert_eq!(deadline, 0);
    }

    #[test]
    fn test_aside_envelope_value_round_trip_fresh() {
        let frame = pack_value(br#"{"name":"value"}"#, 200).unwrap();

        let decoded = unpack(&frame, 199).unwrap();

        assert_eq!(
            decoded,
            StoredEntry::Value {
                body: br#"{"name":"value"}"#.to_vec(),
                stale: false,
            }
        );
    }

    #[test]
    fn test_aside_envelope_value_at_deadline_remains_fresh() {
        let frame = pack_value(br#""value""#, 200).unwrap();

        let decoded = unpack(&frame, 200).unwrap();

        assert_eq!(
            decoded,
            StoredEntry::Value {
                body: br#""value""#.to_vec(),
                stale: false,
            }
        );
    }

    #[test]
    fn test_aside_envelope_value_past_deadline_is_stale() {
        let frame = pack_value(br#""value""#, 200).unwrap();

        let decoded = unpack(&frame, 201).unwrap();

        assert_eq!(
            decoded,
            StoredEntry::Value {
                body: br#""value""#.to_vec(),
                stale: true,
            }
        );
    }

    #[test]
    fn test_aside_envelope_value_without_deadline_stays_fresh() {
        let frame = pack_value(b"null", 0).unwrap();

        let decoded = unpack(&frame, i64::MAX).unwrap();

        assert_eq!(
            decoded,
            StoredEntry::Value {
                body: b"null".to_vec(),
                stale: false,
            }
        );
    }

    #[test]
    fn test_aside_envelope_void_matches_go_shape() {
        let frame = pack_void().unwrap();

        assert_eq!(frame, br#"{"x":true}"#);
        assert_eq!(unpack(&frame, i64::MAX).unwrap(), StoredEntry::Void);
    }

    #[test]
    fn test_aside_envelope_value_matches_go_shape() {
        let frame = pack_value(br#"{"name":"value"}"#, 200).unwrap();

        assert_eq!(frame, br#"{"v":{"name":"value"},"f":200}"#);
    }

    #[test]
    fn test_aside_envelope_invalid_json_frame_returns_error() {
        let result = unpack(b"not-json", 0);

        assert!(matches!(result, Err(CacheError::Internal(_))));
    }

    #[test]
    fn test_aside_envelope_invalid_loader_json_returns_error() {
        let result = pack_value(b"not-json", 0);

        assert!(matches!(result, Err(CacheError::Internal(_))));
    }

    #[test]
    fn test_aside_envelope_void_takes_precedence_like_go() {
        let decoded = unpack(br#"{"v":"ignored","f":200,"x":true}"#, 300).unwrap();

        assert_eq!(decoded, StoredEntry::Void);
    }
}
