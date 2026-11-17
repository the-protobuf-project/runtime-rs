//! Repositories: the typed, AIP-shaped persistence a generated service calls, over any
//! [`records`](crate::records) provider. The Rust counterpart of what protoc-gen-store's
//! `target=repository` generates for Go (`<Model>Repository` and `repox`), shared here
//! rather than generated per package, so it is written and tested once.
//!
//! Generated code (protoc-gen-store `lang=rust target=repository`) supplies, per resource,
//! the [`Resource`](crate::records::Resource) descriptor and the [`Rules`], and binds them
//! with [`RecordsRepository::new`]. A generated cache decorator (protoc-gen-cache
//! `lang=rust`) implements the same [`Repository`] trait in front of it, so a service holds
//! a `Arc<dyn Repository<M>>` and never knows which it has.
//!
//! What the trait promises, as Go's repository does:
//!
//! - **Names.** A record is addressed by its resource name, its primary key.
//! - **Etags.** Every write mints a new etag. A write carrying the record's etag (`m`'s
//!   etag field, or `if_match` on delete and undelete) succeeds only if it still matches,
//!   checked in the write itself, else [`Error::Conflict`](crate::Error::Conflict); an
//!   empty etag carries no precondition.
//! - **Field masks.** `update` writes only `paths` (AIP-134), never an output-only field;
//!   `update_output_only` writes only output-only fields, for the service that owns them.
//! - **Soft delete (AIP-164).** For a resource with a `delete_time` rule, `delete` marks
//!   the record deleted (and sets `expire_time` to `delete_time` plus the retention),
//!   `undelete` clears both, `purge` removes the record, and `list` skips deleted records
//!   unless [`ListInput::show_deleted`]. Without the rule, `delete` removes the record and
//!   `undelete` is refused.
//! - **Outcomes.** [`Error::NotFound`](crate::Error::NotFound),
//!   [`Error::AlreadyExists`](crate::Error::AlreadyExists) (a taken name or unique value,
//!   or an undelete of a live record), [`Error::Conflict`](crate::Error::Conflict), and
//!   [`Error::Invalid`](crate::Error::Invalid) for a refused filter, mask or token.

mod atomic;
mod hooks;
mod mask;
mod read;
mod records_repository;
mod shape;
mod soft;
mod write;

use std::time::Duration;

pub use hooks::Hooks;
pub use records_repository::RecordsRepository;
pub use shape::new_etag;

use crate::Result;

/// The AIP-132 listing parameters. runtime-go's `repox.ListInput`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListInput {
    /// Records per page; `<= 0` means the default.
    pub page_size: i32,
    /// A token from a previous page; empty starts at the beginning.
    pub page_token: String,
    /// An AIP-132 order: `field [desc], other`.
    pub order_by: String,
    /// An AIP-160 filter.
    pub filter: String,
    /// Includes soft-deleted records (AIP-164); no effect without soft delete.
    pub show_deleted: bool,
}

/// One page of a listing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Page<M> {
    /// The records.
    pub items: Vec<M>,
    /// The next page's token; empty on the last page.
    pub next_page_token: String,
}

/// How a resource behaves beyond plain storage: which proto fields carry its etag and its
/// soft-delete times, and which fields only the service may write. Generated per resource
/// from the proto's field behaviors.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rules {
    /// The field holding the etag; `None` for a resource without one.
    pub etag: Option<String>,
    /// The `OUTPUT_ONLY` `delete_time` field: its presence turns on soft delete.
    pub delete_time: Option<String>,
    /// The `OUTPUT_ONLY` `expire_time` field beside `delete_time`.
    pub expire_time: Option<String>,
    /// How long a deleted record is kept: `expire_time` is `delete_time` plus this. Zero
    /// leaves `expire_time` unset. Go's `WithRetention`.
    pub retention: Duration,
    /// The `OUTPUT_ONLY` fields, which `update` ignores and `update_output_only` writes.
    pub output_only: Vec<String>,
}

/// The repository a generated service persists one resource through. runtime-go's
/// generated `<Model>Repository`, in the subset a service calls.
#[async_trait::async_trait]
pub trait Repository<M>: Send + Sync
where
    M: Send + Sync + 'static,
{
    /// Stores `m` as a new record and returns it as stored: etag minted, audit times set.
    async fn create(&self, m: &M) -> Result<M>;

    /// The record named `name`; a soft-deleted record is returned too.
    async fn get(&self, name: &str) -> Result<M>;

    /// A page of records matching `input`.
    async fn list(&self, input: &ListInput) -> Result<Page<M>>;

    /// How many records match `input`'s filter (and `show_deleted`).
    async fn count(&self, input: &ListInput) -> Result<i64>;

    /// Writes the fields `paths` names from `m` onto the stored record; an empty `paths`
    /// writes every mutable field. `m`'s etag, when set, guards the write.
    async fn update(&self, m: &M, paths: &[String]) -> Result<M>;

    /// Writes the output-only fields `paths` names, which `update` refuses; for the service
    /// that owns them. `m`'s etag, when set, guards the write.
    async fn update_output_only(&self, m: &M, paths: &[String]) -> Result<M>;

    /// Deletes the record: marks it deleted under soft delete, else removes it. `if_match`,
    /// when given and not empty, is the etag the record must still have.
    async fn delete(&self, name: &str, if_match: Option<&str>) -> Result<()>;

    /// Restores a soft-deleted record and returns it. `if_match` guards as in `delete`.
    async fn undelete(&self, name: &str, if_match: Option<&str>) -> Result<M>;

    /// Removes the record for good, deleted or not.
    async fn purge(&self, name: &str) -> Result<()>;
}
