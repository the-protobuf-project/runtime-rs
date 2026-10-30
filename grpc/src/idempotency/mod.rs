//! Makes an API's `request_id` field mean what AIP-155 promises: reusing an id returns
//! what the first call returned, instead of attempting the write a second time.
//!
//! It is one message-level interceptor rather than per-handler logic, because the rule is
//! identical for every RPC. The interceptor finds the field by reflection, so an RPC opts in
//! simply by declaring `string request_id` — there is nothing to register, and RPCs without
//! the field are untouched. A request with `validate_only` set is a dry run that writes
//! nothing and is not recorded: recording it would burn the caller's `request_id` on a
//! request that never happened.
//!
//! The key is claimed before the handler runs and settled after it returns:
//!
//! ```text
//! claim (create IN_FLIGHT)  ->  handler  ->  settle (DONE + response)
//! ```
//!
//! The claim is what makes this safe under concurrency. A key's name is a digest of
//! `(method, request_id)`, so two callers replaying the same id race on the store's unique
//! name and exactly one wins the create. The loser reads the winner's key: DONE is a replay
//! and gets the first response verbatim; IN_FLIGHT means the original call is still running,
//! a concurrent duplicate that cannot be answered yet (`ABORTED`, reason
//! [`REASON_IN_FLIGHT`]). A claim older than [`Options::stale_after`] was abandoned by a
//! process that died before settling; it is cleared (`ABORTED`, reason
//! [`REASON_ABANDONED`]) so the retry runs once. A handler that fails deletes its key, so a
//! retry after a real failure is free to proceed.
//!
//! # Differences from Go
//!
//! - Go records the response as a protojson `google.protobuf.Any`, so a replay can rebuild
//!   it without knowing which RPC recorded it. Here the interceptor replays on the very
//!   method that recorded it, whose output type it already knows, so the key holds the
//!   response's protobuf bytes and its type name ([`Key::response`],
//!   [`Key::response_type`]) — smaller, and with no JSON round trip to lose fidelity in.
//! - The request is read through the method's descriptor, registered with
//!   [`crate::HybridServer::with_reflection`]. A method whose descriptor is not registered
//!   cannot be inspected and passes through, as an RPC without `request_id` does.
//! - Failures the interceptor cannot return are logged with `tracing` rather than a
//!   telemetry logger.

mod memory;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use prost::Message as _;
use prost_reflect::{DynamicMessage, Kind, MessageDescriptor, ReflectMessage};
use sha2::{Digest, Sha256};
use tonic::{Code, Status};
use tonic_types::{ErrorDetail, ErrorInfo, StatusExt};

use crate::intercept::{Interceptor, Next, UnaryCall, UnaryReply};

pub use memory::MemoryStore;

/// The field an RPC declares to opt in.
pub const REQUEST_ID_FIELD: &str = "request_id";
/// The field that makes a request a dry run, which is never recorded.
pub const VALIDATE_ONLY_FIELD: &str = "validate_only";

/// The `ErrorInfo` reason of a duplicate that arrived while the first call was still
/// running — retryable, and distinct from a stale etag, the other `ABORTED` an API returns.
pub const REASON_IN_FLIGHT: &str = "REQUEST_IN_FLIGHT";
/// The `ErrorInfo` reason of a retry whose earlier claim was abandoned and has now been
/// cleared — retryable.
pub const REASON_ABANDONED: &str = "REQUEST_ABANDONED";

/// How long a claim may sit IN_FLIGHT before it is treated as abandoned, by default.
pub const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(10 * 60);

/// Where a key stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The first call is running.
    InFlight,
    /// The first call succeeded; [`Key::response`] holds what it returned.
    Done,
}

/// The record of one `(method, request_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    /// [`key_name`] of the method and request id — the store's unique name.
    pub name: String,
    /// The full method, `/package.Service/Method`.
    pub method: String,
    /// The id the caller sent.
    pub request_id: String,
    /// Where the key stands.
    pub state: State,
    /// The first call's response, protobuf-encoded. Empty until [`State::Done`].
    pub response: Bytes,
    /// The response's fully-qualified message type.
    pub response_type: String,
    /// When the key was claimed. Set by the store on [`Store::create`].
    pub create_time: Option<SystemTime>,
}

/// What a [`Store`] reports when it cannot do what was asked.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// [`Store::get`] found no key of that name.
    #[error("idempotency: key not found")]
    NotFound,
    /// Anything else: a lost race on [`Store::create`], or the backend failing.
    #[error("{0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StoreError {
    /// Wraps a backend's error.
    pub fn backend(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Backend(err.into())
    }
}

/// Keeps keys. Build it over whatever backend the service already writes to, so a key's
/// uniqueness is enforced where the data lives.
#[async_trait::async_trait]
pub trait Store: Send + Sync + 'static {
    /// Claims a key, stamping its [`Key::create_time`]. It fails when one of that name
    /// exists — with any error: the interceptor reads the key to tell a lost race from a
    /// fault.
    async fn create(&self, key: Key) -> Result<(), StoreError>;
    /// Reads a key: [`StoreError::NotFound`] when there is none.
    async fn get(&self, name: &str) -> Result<Key, StoreError>;
    /// Writes a claimed key's state and response.
    async fn settle(&self, key: Key) -> Result<(), StoreError>;
    /// Releases a key.
    async fn delete(&self, name: &str) -> Result<(), StoreError>;
}

/// Maps a claim failure no key explains onto the status the caller gets.
pub type CreateErrorFn = Arc<dyn Fn(&StoreError) -> Status + Send + Sync>;
/// The clock the interceptor judges staleness by.
pub type ClockFn = Arc<dyn Fn() -> SystemTime + Send + Sync>;

/// Tunes the interceptor. The default of each field is Go's.
#[derive(Clone, Default)]
pub struct Options {
    /// The `ErrorInfo` domain of the refusals.
    pub domain: String,
    /// How long a claim may sit IN_FLIGHT before it is treated as abandoned: far longer than
    /// any handler runs, so reclaiming a key this old cannot race a handler still working.
    /// Zero means [`DEFAULT_STALE_AFTER`].
    pub stale_after: Duration,
    /// The status returned when a claim failed and no key explains it — the store is unwell
    /// — given the claim's error. `None` means `INTERNAL`.
    pub create_error: Option<CreateErrorFn>,
    /// The clock. `None` means [`SystemTime::now`].
    pub now: Option<ClockFn>,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("domain", &self.domain)
            .field("stale_after", &self.stale_after)
            .field("create_error", &self.create_error.is_some())
            .field("now", &self.now.is_some())
            .finish()
    }
}

/// The interceptor that enforces `request_id` semantics against `store`. Go's
/// `idempotency.Interceptor`.
///
/// Register it with [`crate::HybridServer::with_interceptor`].
pub fn interceptor(store: impl Store, opts: Options) -> IdempotencyInterceptor {
    IdempotencyInterceptor::new(Arc::new(store), opts)
}

/// The interceptor [`interceptor`] builds.
#[derive(Clone)]
pub struct IdempotencyInterceptor {
    store: Arc<dyn Store>,
    domain: String,
    stale_after: Duration,
    create_error: CreateErrorFn,
    now: ClockFn,
}

impl std::fmt::Debug for IdempotencyInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdempotencyInterceptor")
            .field("domain", &self.domain)
            .field("stale_after", &self.stale_after)
            .finish_non_exhaustive()
    }
}

impl IdempotencyInterceptor {
    /// Enforces `request_id` semantics against a shared `store`.
    #[must_use]
    pub fn new(store: Arc<dyn Store>, opts: Options) -> Self {
        Self {
            store,
            domain: opts.domain,
            stale_after: if opts.stale_after.is_zero() {
                DEFAULT_STALE_AFTER
            } else {
                opts.stale_after
            },
            create_error: opts.create_error.unwrap_or_else(|| {
                Arc::new(|err: &StoreError| {
                    Status::internal(format!("idempotency: claim key: {err}"))
                })
            }),
            now: opts.now.unwrap_or_else(|| Arc::new(SystemTime::now)),
        }
    }

    /// Answers a caller whose claim lost the race. `create_err` is the failure that sent it
    /// here, returned when the key turns out not to explain it.
    async fn replay(
        &self,
        name: &str,
        create_err: &StoreError,
        output: Option<MessageDescriptor>,
    ) -> Result<UnaryReply, Status> {
        let existing = match self.store.get(name).await {
            Ok(key) => key,
            // The create failed for some reason other than a duplicate key, so idempotency
            // has nothing to say about it.
            Err(StoreError::NotFound) => return Err((self.create_error)(create_err)),
            Err(err) => return Err(Status::internal(format!("idempotency: read key: {err}"))),
        };

        if existing.state == State::Done {
            tracing::debug!(key = name, "idempotency: replaying the recorded response");
            return Ok(UnaryReply::new(existing.response, output));
        }

        // IN_FLIGHT. A claim older than any handler could run for was made by a process
        // that died before settling; drop it so the next attempt claims cleanly rather than
        // wedging this id forever.
        let stale = existing.create_time.is_some_and(|created| {
            (self.now)()
                .duration_since(created)
                .is_ok_and(|age| age > self.stale_after)
        });
        if stale {
            if let Err(err) = self.store.delete(name).await {
                return Err(Status::internal(format!(
                    "idempotency: reclaim abandoned key: {err}"
                )));
            }
            tracing::debug!(key = name, "idempotency: cleared an abandoned claim");
            return Err(self.aborted(
                REASON_ABANDONED,
                "An earlier request with this request_id stopped before it finished, and its \
                 claim has now been cleared. Retry the same request with the same request_id: \
                 it will run once",
            ));
        }
        Err(self.aborted(
            REASON_IN_FLIGHT,
            "A request with this request_id is still running. Wait a moment and retry with \
             the same request_id: once the first finishes, the retry returns its result \
             without running it again",
        ))
    }

    fn aborted(&self, reason: &str, message: &str) -> Status {
        Status::with_error_details_vec(
            Code::Aborted,
            format!("{message}."),
            vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
                reason,
                self.domain.clone(),
                std::collections::HashMap::new(),
            ))],
        )
    }

    /// Records what the handler returned, so a later retry replays it. A failure here is
    /// logged and swallowed: the write the caller asked for has happened, and an error would
    /// tell them otherwise. The cost is that a retry re-runs the handler and meets the
    /// underlying constraint instead of replaying.
    async fn settle(&self, mut key: Key, reply: &UnaryReply) {
        // Decoding re-encodes a compressed reply as plain bytes, which is what a replay
        // sends; it also names the type.
        let response = match reply.message() {
            Ok(message) => message,
            Err(status) => {
                tracing::error!(
                    method = %key.method,
                    request_id = %key.request_id,
                    error = %status.message(),
                    "idempotency: encode response"
                );
                return;
            }
        };
        key.state = State::Done;
        key.response = Bytes::from(response.encode_to_vec());
        key.response_type = response.descriptor().full_name().to_string();
        let (method, request_id) = (key.method.clone(), key.request_id.clone());
        if let Err(err) = self.store.settle(key).await {
            tracing::error!(%method, %request_id, error = %err, "idempotency: settle key");
        }
    }
}

#[async_trait::async_trait]
impl Interceptor for IdempotencyInterceptor {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        // The descriptor says whether the RPC offers idempotency at all, so an RPC without
        // the field is never decoded.
        let Some(method) = call.descriptor().cloned() else {
            return next.run(call).await;
        };
        if field(&method.input(), REQUEST_ID_FIELD, &Kind::String).is_none() {
            return next.run(call).await;
        }
        let request = match call.message() {
            Ok(request) => request,
            Err(status) => {
                tracing::debug!(error = %status.message(), "idempotency: request unreadable — passing through");
                return next.run(call).await;
            }
        };
        let request_id = string_field(&request, REQUEST_ID_FIELD);
        if request_id.is_empty() || bool_field(&request, VALIDATE_ONLY_FIELD) {
            // The caller declined idempotency, or it is a dry run that writes nothing.
            // Nothing to dedupe.
            return next.run(call).await;
        }

        let full_method = call.full_method().to_string();
        let key = Key {
            name: key_name(&full_method, &request_id),
            method: full_method.clone(),
            request_id: request_id.clone(),
            state: State::InFlight,
            response: Bytes::new(),
            response_type: String::new(),
            create_time: None,
        };
        if let Err(err) = self.store.create(key.clone()).await {
            // Lost the race to claim this id, or the store is unwell. Only the key itself
            // can tell which, so read it.
            return self.replay(&key.name, &err, Some(method.output())).await;
        }

        match next.run(call).await {
            Ok(reply) => {
                self.settle(key, &reply).await;
                Ok(reply)
            }
            Err(status) => {
                // The call failed and wrote nothing worth remembering. Release the id so the
                // caller may retry it for real.
                if let Err(err) = self.store.delete(&key.name).await {
                    tracing::error!(
                        method = %full_method,
                        %request_id,
                        error = %err,
                        "idempotency: release key after handler error"
                    );
                }
                Err(status)
            }
        }
    }
}

/// Addresses the key for `(method, request_id)`: `idempotencyKeys/` and a SHA-256. The
/// digest keeps the name a fixed, safe length whatever the caller sent, and makes the name
/// itself the uniqueness constraint that decides who wins a concurrent claim.
#[must_use]
pub fn key_name(method: &str, request_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(method.as_bytes());
    hasher.update([0u8]);
    hasher.update(request_id.as_bytes());
    let digest = hasher.finalize();
    let mut name = String::with_capacity(16 + 64);
    name.push_str("idempotencyKeys/");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(name, "{byte:02x}");
    }
    name
}

/// A singular field of `kind` on the message type, or `None`.
fn field(
    message: &MessageDescriptor,
    name: &str,
    kind: &Kind,
) -> Option<prost_reflect::FieldDescriptor> {
    message
        .get_field_by_name(name)
        .filter(|fd| !fd.is_list() && !fd.is_map() && fd.kind() == *kind)
}

fn string_field(message: &DynamicMessage, name: &str) -> String {
    field(&message.descriptor(), name, &Kind::String)
        .and_then(|fd| message.get_field(&fd).as_str().map(str::to_string))
        .unwrap_or_default()
}

fn bool_field(message: &DynamicMessage, name: &str) -> bool {
    field(&message.descriptor(), name, &Kind::Bool)
        .and_then(|fd| message.get_field(&fd).as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_names_are_fixed_length_digests_of_method_and_id() {
        let a = key_name("/m/A", "r");
        assert_eq!(a.len(), "idempotencyKeys/".len() + 64);
        assert_ne!(a, key_name("/m/B", "r"));
        assert_eq!(a, key_name("/m/A", "r"));
        // The separator keeps ("/m/A", "r") apart from ("/m/Ar", "").
        assert_ne!(a, key_name("/m/Ar", ""));
    }
}
