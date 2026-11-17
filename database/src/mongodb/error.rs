//! Driver failures as the crate's [`Error`]: duplicate keys become `AlreadyExists`, everything
//! else a [`Error::Backend`] that says what was being attempted.

use mongodb::error::{Error as MongoError, ErrorKind, WriteFailure};

use super::BACKEND;
use crate::Error;

/// The codes MongoDB reports a unique-index violation with: 11000, and the legacy 11001 and
/// 12582. Go's `mongo.IsDuplicateKeyError`.
const DUPLICATE_KEY: [i32; 3] = [11000, 11001, 12582];

/// A driver failure, with what was being attempted.
#[derive(Debug, thiserror::Error)]
#[error("{what}: {source}")]
struct Failed {
    what: String,
    #[source]
    source: MongoError,
}

/// `source`, as an [`Error::Backend`] that names what failed.
pub(super) fn backend(what: impl Into<String>, source: MongoError) -> Error {
    Error::Backend {
        backend: BACKEND,
        source: Box::new(Failed {
            what: what.into(),
            source,
        }),
    }
}

/// The code a transaction's write reports when another transaction wrote the document
/// since this one's snapshot.
const WRITE_CONFLICT: i32 = 112;

/// `source` as `AlreadyExists` when it is a unique-index violation, as `Conflict` when a
/// concurrent transaction wrote the document first (another writer won; a retry against
/// fresh state is meaningful, as Go maps a serialization failure), else as [`backend`].
pub(super) fn write(what: impl Into<String>, source: MongoError) -> Error {
    if is_duplicate_key(&source) {
        Error::AlreadyExists
    } else if code_of(&source) == Some(WRITE_CONFLICT) {
        Error::Conflict
    } else {
        backend(what, source)
    }
}

/// The server's code for a single write's or a command's failure.
fn code_of(err: &MongoError) -> Option<i32> {
    match err.kind.as_ref() {
        ErrorKind::Write(WriteFailure::WriteError(e)) => Some(e.code),
        ErrorKind::Command(e) => Some(e.code),
        _ => None,
    }
}

/// Whether `err` is a unique-index violation, wherever the driver put the code: a single
/// write, an ordered insert's write errors, or a command.
pub(super) fn is_duplicate_key(err: &MongoError) -> bool {
    match err.kind.as_ref() {
        ErrorKind::Write(WriteFailure::WriteError(e)) => DUPLICATE_KEY.contains(&e.code),
        ErrorKind::InsertMany(e) => e
            .write_errors
            .iter()
            .flatten()
            .any(|e| DUPLICATE_KEY.contains(&e.code)),
        ErrorKind::Command(e) => DUPLICATE_KEY.contains(&e.code),
        _ => false,
    }
}

/// A refusal of a `res`-less or otherwise malformed request.
pub(super) fn invalid(msg: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("{BACKEND}: {msg}"))
}
