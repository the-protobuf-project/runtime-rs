//! A cache in front of a [`Repository`]: the generic half of what protoc-gen-cache
//! `lang=rust` generates. Go generates the whole decorator per resource; here the decorator
//! is written once and the generator emits only what differs between resources, as data: a
//! [`Plan`] naming the resource's keyspace, strategy, leases, index fields, invalidation
//! edges and list cache, with one plain function per proto field it has to read.
//!
//! ```ignore
//! static BOOK: Keyspace = Keyspace {
//!     resource: "bookstore.v1.Book",
//!     model: "Book",
//!     pattern: "authors/{author}/books/{book}",
//!     namespace: &[Part::Literal("bookstore.v1.book"), Part::Literal("author"), Part::Segment("author")],
//! };
//! let books = cached::wrap(inner, cache, Plan {
//!     name: |m: &pb::Book| m.name.as_str(),
//!     record: Some(Record { keyspace: &BOOK, strategy: Strategy::Indexed, ttl: Some(120s), .. }),
//!     list: None,
//! });
//! ```
//!
//! # Keys are Go's
//!
//! Every entry lives where protoc-gen-cache's Go output puts it, byte for byte:
//! `{prefix}:{namespace}:cache:aside:entry:{id}`, with the namespace derived as Go derives
//! it (the resource type, then each parent segment's name and value) and the id the
//! record's resource name. Go builds one decorator per scope over a cache database opened
//! under that scope's namespace; a Rust repository serves every parent at once, so the
//! namespace is derived from each record's name and the database is selected under it with
//! [`cache::DB::under`]. The [`cache::DB`] handed in supplies the connection, the prefix
//! and the policy; its own name is not used.
//!
//! # Values are the proto wire encoding
//!
//! A record is cached as its wire bytes inside a JSON string, base64, which is what Go's
//! cache codec makes of a `[]byte` and what [`crate::cached`] stores, so a Go reader of
//! `[]byte` decodes it. A value that does not decode, such as one a Go store decorator
//! wrote as its model's JSON under the same key, is dropped and read through rather than
//! returned.
//!
//! # Writes
//!
//! A write goes to the inner repository first; what follows runs after it committed, as in
//! Go: the entry is refreshed (create, update, undelete) or dropped (delete, purge), the
//! record refiled under its index fields, each declared edge's target entry dropped, and the
//! record's list versions moved on. A failure in any of these does not fail the write unless
//! [`Hooks::strict_writes`] says so; [`Hooks::write_errors`] observes it either way.
//!
//! # Lists
//!
//! As Go's list cache: a page is cached under its parent's version, and a write under a
//! parent replaces that version, so pages are never invalidated one by one. A list reads its
//! [`ListInput::parent`]'s version, or the version every write moves on for a parent with a
//! `-` (AIP-159) or, on a nested resource, an empty one (every parent). The page key is Go's
//! digest of the request, the parent beside `repox.ListInput` as Go's `List(parent, in)`
//! puts it, so pages are shared with Go.

mod keyspace;
mod lists;
mod plan;
mod records;
mod repo;
mod wire;

#[cfg(test)]
mod tests;

use std::sync::{Arc, OnceLock};

use prost::Message;

use super::Repository;
use crate::Error;

pub use keyspace::{Keyspace, Part};
pub use lists::page_key;
pub use plan::{DeclaredEdge, Edge, Index, List, Plan, Record, Strategy};
pub use wire::go_json_string;

/// Observes a cache failure that did not fail the call: `(resource, operation, error)`.
pub type Observer = Arc<dyn Fn(&str, &str, &Error) + Send + Sync>;

/// How cache failures are handled. The default absorbs them all.
#[derive(Clone, Default)]
pub struct Hooks {
    /// Every failure after a committed write: Go's `WithWriteErrors`.
    pub write_errors: Option<Observer>,
    /// Fails the write when the work after it fails, rather than serving stale entries:
    /// Go's `WithStrictWrites`. The write has committed either way.
    pub strict_writes: bool,
    /// Every list-cache failure; none fails a call: Go's `WithListErrors`.
    pub list_errors: Option<Observer>,
}

/// `inner` with `plan`'s caches in front of it, over `cache`'s connection and policy.
pub fn wrap<M>(
    inner: Arc<dyn Repository<M>>,
    cache: Arc<cache::DB>,
    plan: Plan<M>,
) -> Arc<dyn Repository<M>>
where
    M: Message + Default + Send + Sync + 'static,
{
    wrap_with(inner, cache, plan, Hooks::default())
}

/// [`wrap`], with `hooks` deciding what a cache failure does.
pub fn wrap_with<M>(
    inner: Arc<dyn Repository<M>>,
    cache: Arc<cache::DB>,
    plan: Plan<M>,
    hooks: Hooks,
) -> Arc<dyn Repository<M>>
where
    M: Message + Default + Send + Sync + 'static,
{
    Arc::new(CachedRepository {
        inner,
        cache,
        plan,
        hooks,
        fixed: OnceLock::new(),
        lists: OnceLock::new(),
    })
}

/// The decorator [`wrap`] returns.
pub struct CachedRepository<M> {
    inner: Arc<dyn Repository<M>>,
    cache: Arc<cache::DB>,
    plan: Plan<M>,
    hooks: Hooks,
    /// The record database, once, for a namespace no name parameterizes.
    fixed: OnceLock<std::result::Result<Arc<cache::DB>, String>>,
    /// The list database, once.
    lists: OnceLock<std::result::Result<Arc<cache::DB>, String>>,
}

/// The version scope every write to a nested resource moves on: Go's `listAnyParent`.
const ANY_PARENT: &str = "*";
