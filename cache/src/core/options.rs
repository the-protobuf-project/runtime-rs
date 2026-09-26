//! Per-operation cache policy overrides.

use std::time::Duration;

/// Typed operation settings shared by all cache strategies.
///
/// Strategies combine these values with DB defaults. Lease priority is:
/// explicit `ttl`, explicit `permanent`, configured default, required-TTL
/// rejection, then permanent storage. Unused fields are ignored by strategies
/// to which they do not apply.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Explicit ID for Document or Indexed create; absence invokes `NewId`.
    pub id: Option<String>,

    /// Explicit freshness lease; takes precedence over permanence and defaults.
    pub ttl: Option<Duration>,

    /// Additional Aside stale-serving window after the fresh deadline.
    pub stale: Option<Duration>,

    /// Secondary field/value memberships requested by Indexed operations.
    pub indexes: Option<std::collections::HashMap<String, String>>,

    /// Explicit intent to store permanently when no explicit TTL is present.
    pub permanent: bool,
}
