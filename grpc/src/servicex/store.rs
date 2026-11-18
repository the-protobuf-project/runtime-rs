//! store=true: the standard methods' handlers over runtime-rs repositories
//! (`database::repository::Repository`; servicex's own `Repository` is the
//! feeds' and adapters' read contract, a different trait). Each generated `<S>Handlers` gets a
//! `with_repositories` that answers every standard method from a
//! `<S>Repositories` of these; the server's checks and hooks run around them
//! as around any handler.
use std::sync::Arc;
use tonic::Status;

/// The repository a store=true server persists one resource through:
/// protoc-gen-store's `RecordsRepository`, protoc-gen-cache's decorator in
/// front of it, or anything else implementing the contract.
pub type Repo<M> = Arc<dyn ::database::repository::Repository<M>>;

/// A repository error as a status whose code carries the standard reason the
/// server attaches (see `Call::mapped`), as the Go servers map repox's
/// sentinels: NotFound is NOT_FOUND, AlreadyExists ALREADY_EXISTS, Conflict
/// ABORTED (CONCURRENT_MODIFICATION), Invalid INVALID_ARGUMENT. A timeout is
/// DEADLINE_EXCEEDED, a capability the backend lacks UNIMPLEMENTED, anything
/// else INTERNAL.
pub fn store_status(e: ::database::Error, resource_type: &str, name: &str) -> Status {
    let what = if name.is_empty() {
        format!("the {resource_type}")
    } else {
        format!("{name:?}")
    };
    match e {
        ::database::Error::NotFound => {
            Status::not_found(format!("{what} does not exist, or is deleted"))
        }
        ::database::Error::AlreadyExists => {
            Status::already_exists(format!("{what} already exists"))
        }
        ::database::Error::Conflict => Status::aborted(format!(
            "the etag sent is not the current etag of {what}; read it again and retry"
        )),
        ::database::Error::Invalid(message) => Status::invalid_argument(message),
        e @ ::database::Error::Timeout => Status::deadline_exceeded(e.to_string()),
        e @ ::database::Error::Unimplemented { .. } => Status::unimplemented(e.to_string()),
        e => Status::internal(e.to_string()),
    }
}

/// The etag a delete or undelete is guarded with: none when empty.
pub fn if_match(etag: &str) -> Option<&str> {
    (!etag.is_empty()).then_some(etag)
}

/// The update paths a repository takes: the canonical mask, with `*` (every
/// mutable field) as the empty mask.
pub fn store_paths(mask: Option<prost_types::FieldMask>) -> Vec<String> {
    let paths = mask.map(|m| m.paths).unwrap_or_default();
    if paths == ["*"] { Vec::new() } else { paths }
}
