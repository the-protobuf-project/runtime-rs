//! Concrete storage backend boundaries.
//!
//! Backend modules own connection lifecycle and direct protocol-to-capability
//! mappings. They do not own cache strategy policy, application value storage,
//! or ad-hoc key construction; those remain in [`crate::core`] and
//! [`crate::strategies`].

/// Standalone Redis client, Provider, and primitive capability adapters.
pub mod redis;
