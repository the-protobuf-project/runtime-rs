//! The CRUD contract every backend implements, and its options and results. Go's
//! `store/driver.go` and `store/options.go`.

use std::collections::BTreeMap;

use prost_reflect::DynamicMessage;

use super::Resource;
use crate::Result;

/// The CRUD contract over proto messages. Drivers return [`crate::Error::NotFound`] and
/// [`crate::Error::AlreadyExists`] for those outcomes, so callers map them uniformly.
/// Go's `store.Driver`.
#[async_trait::async_trait]
pub trait Driver: Send + Sync {
    /// Stores `msg` as a new record; `AlreadyExists` when its key or a unique value is taken.
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult>;
    /// The record under `key`; `NotFound` when there is none.
    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage>;
    /// The record under `key`, locked against other writers until the transaction this
    /// driver is bound to ends, where the backend locks rows (`SELECT ... FOR UPDATE` on
    /// Postgres and MySQL). A read-check-write in a transaction uses it so two writers take
    /// turns. Elsewhere it is [`get`](Self::get): outside a transaction, and on a backend
    /// whose transactions already exclude a concurrent writer (SQLite, the Tauri store) or
    /// refuse one (MongoDB's write conflicts).
    async fn get_for_update(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.get(res, key).await
    }
    /// Overwrites the record identified by `msg`'s primary key; `NotFound` when there is none.
    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult>;
    /// Removes the record under `key`; `NotFound` when there is none.
    async fn delete(&self, res: &Resource, key: &str) -> Result<()>;
    /// A page of records matching `opts`.
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult>;
    /// How many records match `opts.filter`.
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64>;
    /// Whether a record with `key` exists.
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool>;
}

/// Paging, ordering and filtering for `list` and `count`, shaped after AIP-132 rather than
/// limit/offset. Go's `store.ListOptions`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListOptions {
    /// Caps the records returned; `<= 0` means the driver default.
    pub page_size: i32,
    /// An opaque continuation token from a previous `list`.
    pub page_token: String,
    /// An AIP-132 order expression, e.g. `title desc`.
    pub order_by: String,
    /// An AIP-160 filter expression, parsed by [`core::filter::parse`](super::core::filter::parse).
    /// Empty matches every record. Redis refuses any filter.
    pub filter: String,
    /// Restrictions added in code rather than written in `filter`, ANDed with it: what the
    /// filter grammar cannot say, such as [`Expr::Null`](super::core::filter::Expr::Null).
    /// Every provider that filters applies them, through
    /// [`core::filter::listing`](super::core::filter::listing); Redis refuses them as it
    /// refuses a filter. Columns are backend names.
    pub restrict: Vec<super::core::filter::Expr>,
    /// Skips counting the total, which is then -1: a backend reads one row past the page to
    /// learn whether another exists instead of scanning every match.
    pub omit_total: bool,
}

impl ListOptions {
    /// Sets the page size.
    pub fn page_size(mut self, size: i32) -> Self {
        self.page_size = size;
        self
    }

    /// Sets the page token.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = token.into();
        self
    }

    /// Sets the filter.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = filter.into();
        self
    }

    /// Sets the order.
    pub fn order_by(mut self, order: impl Into<String>) -> Self {
        self.order_by = order.into();
        self
    }

    /// Skips the total.
    pub fn omit_total(mut self, omit: bool) -> Self {
        self.omit_total = omit;
        self
    }
}

/// A page of records, the token to the next (empty on the last page), and the total
/// matching, or -1 when skipped. Go's `store.ListResult`.
#[derive(Clone, Debug, Default)]
pub struct ListResult {
    /// The records.
    pub items: Vec<DynamicMessage>,
    /// The continuation token; empty on the last page.
    pub next_page_token: String,
    /// The count of matching records ignoring paging, or -1.
    pub total: i64,
}

/// What `create` and `update` return. A synchronous backend sets `pending` false and
/// `message` to the stored record; an asynchronous one sets `pending` and carries handles in
/// `metadata`. Go's `store.WriteResult`.
#[derive(Clone, Debug)]
pub struct WriteResult {
    /// The resulting record; for a pending write, the value submitted.
    pub message: DynamicMessage,
    /// Whether the backend accepted but has not finalized the write.
    pub pending: bool,
    /// Backend handles for a pending write.
    pub metadata: BTreeMap<String, String>,
}

impl WriteResult {
    /// A finished write of `message`.
    pub fn done(message: DynamicMessage) -> Self {
        Self {
            message,
            pending: false,
            metadata: BTreeMap::new(),
        }
    }
}
