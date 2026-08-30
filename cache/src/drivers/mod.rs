//! Concrete storage backend boundaries.
//!
//! Backend modules own connection lifecycle and direct protocol-to-capability
//! mappings. They do not own cache strategy policy, application value storage,
//! or ad-hoc key construction; those remain in [`crate::core`] and
//! [`crate::strategies`].

/// Standalone Redis client, Provider, and primitive capability adapters.
pub mod redis;

/// Async Memcached client, Go-compatible router, Driver, and Bulk adapter.
pub mod memcached;

// This test-only module pins the experimental dependency surface the private
// production adapter relies on, catching minor-line API drift at compile time.
#[cfg(test)]
mod memcached_compatibility;
