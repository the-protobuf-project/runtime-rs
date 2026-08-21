//! Small value wrappers shared by core-facing APIs.

/// Owned encoded cache bytes.
///
/// This newtype carries no encoding policy or I/O behavior; strategies currently
/// use raw byte slices/vectors directly, so it is primarily a typed extension
/// point for callers that want to distinguish cached bytes from other buffers.
pub struct CacheValue(pub Vec<u8>);
