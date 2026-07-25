//! Generic CRUD surfaces that generated resource handlers satisfy.
//!
//! [`QueryHandler`] and [`MutationHandler`] are the generic read/write surfaces that
//! every generated resource handler satisfies (alongside its resource-specific methods,
//! such as an aggregate query). They let a generic adapter — e.g. a Hasura engine —
//! drive reads and writes for any entity through one pair of traits instead of a
//! hand-written, copy-pasted handler per entity. These traits are implemented by
//! generated code and used via static dispatch (generic functions bounded by the
//! trait), never as trait objects.

use crate::requests::{CreateRequest, DeleteRequest, ListRequest, UpdateRequest};

/// The generic read surface for a resource whose row model is `M`.
// `async fn` in a public trait normally warns because it can't express a `Send` bound on
// the returned future; that bound is unneeded here since these traits are only ever used
// via static dispatch (generic functions bounded by the trait), never as trait objects.
#[allow(async_fn_in_trait)]
pub trait QueryHandler<M> {
    /// The error type returned by this handler's operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the row with the given id, or `None` when absent.
    async fn get(&self, id: &str) -> Result<Option<M>, Self::Error>;

    /// Returns the rows matching the request.
    async fn list(&self, req: Option<&ListRequest>) -> Result<Vec<M>, Self::Error>;

    /// Returns the first row matching the request, or `None` when none match.
    async fn find(&self, req: Option<&ListRequest>) -> Result<Option<M>, Self::Error>;
}

/// The generic write surface for a resource. `C` is the create input and `U` the update
/// patch; `IR`, `UR`, and `DR` are the insert, update, and delete response models (an
/// engine like Hasura returns a distinct mutation-response type per verb, so they are
/// separate type parameters rather than one shared response type).
#[allow(async_fn_in_trait)]
pub trait MutationHandler<C, U, IR, UR, DR> {
    /// The error type returned by this handler's operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Inserts `obj` and returns the insert response.
    async fn create(&self, obj: C, req: Option<&CreateRequest>) -> Result<IR, Self::Error>;

    /// Applies `patch` to the row with the given id and returns the update response.
    async fn update(
        &self,
        id: &str,
        patch: U,
        req: Option<&UpdateRequest>,
    ) -> Result<UR, Self::Error>;

    /// Removes the row with the given id and returns the delete response.
    async fn delete(&self, id: &str, req: Option<&DeleteRequest>) -> Result<DR, Self::Error>;
}
