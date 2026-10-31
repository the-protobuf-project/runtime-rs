//! The CRUD contract every backend implements, as runtime-go's `store.Driver`.

use super::{Record, Resource, Result};
use std::collections::BTreeMap;

/// Records addressed through a [`Resource`]. Read methods complete before they
/// return. A write returns a [`WriteResult`] whose `pending` flag lets a
/// buffered or asynchronous backend report an accepted but not yet durable
/// write instead of blocking. Backends refuse what they cannot do with
/// [`Error::Unimplemented`](super::Error::Unimplemented).
#[async_trait::async_trait]
pub trait Driver: Send + Sync {
    /// Stores a new record; AlreadyExists when its key is taken.
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult>;
    /// The record under `key`, or NotFound.
    async fn get(&self, res: &Resource, key: &str) -> Result<Record>;
    /// Replaces the record with the same key, or NotFound.
    async fn update(&self, res: &Resource, record: &Record) -> Result<WriteResult>;
    /// Removes the record under `key`, or NotFound.
    async fn delete(&self, res: &Resource, key: &str) -> Result<()>;
    /// One page of matching records with its continuation token and total.
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult>;
    /// How many records match `opts.filter`.
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64>;
    /// Whether a record is stored under `key`.
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool>;
}

/// Paging, ordering and filtering for list and count, after AIP-132.
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    /// Records per page; 0 or less means the default of 50.
    pub page_size: i32,
    /// Opaque token from the previous page's `next_page_token`.
    pub page_token: String,
    /// AIP-132 order such as `"observed_at desc"`; empty for the default.
    pub order_by: String,
    /// AIP-160 conjunction such as `"device = \"ahu1\" AND value > 30"`.
    pub filter: String,
    /// Skips computing `ListResult::total`, which is then -1.
    pub omit_total: bool,
}

impl ListOptions {
    /// Caps records per page.
    pub fn page(mut self, size: i32) -> Self {
        self.page_size = size;
        self
    }

    /// Narrows with an AIP-160 expression.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = filter.into();
        self
    }

    /// Sorts, for example `"observed_at desc"`.
    pub fn order_by(mut self, order: impl Into<String>) -> Self {
        self.order_by = order.into();
        self
    }
}

/// One page. `next_page_token` is empty on the last page; `total` counts all
/// matches, or is -1 when the backend cannot compute it cheaply.
#[derive(Clone, Debug, Default)]
pub struct ListResult {
    /// Records on this page.
    pub items: Vec<Record>,
    /// Token for the next page; empty when there is none.
    pub next_page_token: String,
    /// Matching records ignoring pagination, or -1.
    pub total: i64,
}

/// What a write produced. A pending write was accepted but not yet durable:
/// `record` is the submitted value and `metadata` carries backend handles.
#[derive(Clone, Debug, Default)]
pub struct WriteResult {
    /// The stored record, or the submitted one when pending.
    pub record: Record,
    /// True when the backend accepted the write but has not finished it.
    pub pending: bool,
    /// Backend handles for a pending write; empty otherwise.
    pub metadata: BTreeMap<String, String>,
}

impl WriteResult {
    /// A completed write of `record`.
    pub fn done(record: Record) -> Self {
        Self {
            record,
            ..Self::default()
        }
    }
}

/// Default page size when a caller names none, as runtime-go's core package.
pub const DEFAULT_PAGE_SIZE: i32 = 50;

/// Effective page size, refusing sizes no backend here can return in one page.
pub(crate) fn page_size(requested: i32) -> Result<usize> {
    match requested {
        n if n <= 0 => Ok(DEFAULT_PAGE_SIZE as usize),
        n if n <= 10_000 => Ok(n as usize),
        _ => Err(super::Error::Invalid(
            "page_size must be at most 10000".into(),
        )),
    }
}
