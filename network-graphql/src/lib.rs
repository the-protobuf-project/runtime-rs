//! Small helpers and scalar types for generated GraphQL clients: predicate/filter
//! building for Hasura-style `where` clauses, typed column-handle helpers, an
//! update-patch three-state [`Nullable`] type, cursor pagination, and generic CRUD
//! handler traits.
//!
//! Optional GraphQL arguments are represented natively as `Option<T>`; this crate does
//! not provide pointer-constructor helpers for them.

#![warn(missing_docs)]

pub mod columns;
pub mod error;
pub mod fields_number;
pub mod fields_other;
pub mod fields_string;
pub mod handlers;
pub mod keyset;
pub mod nullable;
pub mod predicate;
pub mod requests;
pub mod scalars;

pub use columns::{ColumnPatch, OrderBy, OrderTerm};
pub use error::Error;
pub use fields_number::{FloatField, Int64Field};
pub use fields_other::{BoolField, EnumField, JSONField};
pub use fields_string::StringField;
pub use handlers::{MutationHandler, QueryHandler};
pub use keyset::after;
pub use nullable::Nullable;
pub use predicate::{and, not, or, relation, Predicate};
pub use requests::{CreateRequest, DeleteRequest, ListRequest, UpdateRequest};
pub use scalars::{Bigdecimal, Int64, Variable};
