//! Building blocks for providers.
//!
//! Applications never need this module. Providers use it so the frame layout,
//! subject rules, id format, batch semantics, and delivery metrics are one
//! implementation rather than six that drift.

mod batch;
mod envelope;
mod metrics;
mod subject;

pub use batch::{batch_result, check_batch, publish_each};
pub use envelope::{pack, pack_with, unpack, unpack_shared};
pub use metrics::Metrics;
pub use subject::{declares, declares_pattern, matches, unknown_subject};

use crate::{Headers, Options, Result, Stream};

/// How many messages a provider holds ahead of the reader when the caller does
/// not say.
pub const DEFAULT_PREFETCH: usize = 64;

/// The prefetch a subscription should use: the caller's, or [`DEFAULT_PREFETCH`].
pub fn prefetch(opts: &Options) -> usize {
    opts.prefetch.unwrap_or(DEFAULT_PREFETCH)
}

/// A new message id: a 26-character ULID, so lexicographic order is creation
/// order, the same format runtime-go mints.
pub fn new_id() -> String {
    ulid::Ulid::new().to_string()
}

/// A new stream id: a ULID with the stream's name appended after a colon, as in
/// runtime-go, so a listing reads as something a person can recognise.
pub fn new_stream_id(name: &str) -> String {
    let id = new_id();
    if name.is_empty() {
        id
    } else {
        format!("{id}:{name}")
    }
}

/// The id a publish should go out under: the caller's, or a new one.
pub fn publish_id(opts: &Options) -> String {
    opts.id.clone().unwrap_or_else(new_id)
}

/// The headers a publish sends: `stream`'s defaults with `opts`' own on top,
/// validated. Every publish and batch path calls this once per call.
pub fn outgoing(stream: &Stream, opts: &Options) -> Result<Headers> {
    let headers = stream.headers.merged(&opts.headers);
    headers.validate()?;
    Ok(headers)
}
