//! How long a client may treat a result as fresh, and who may share it.
//!
//! Protocol revision 2026-07-28 made list and read results cacheable. A hint is a declaration
//! the proto carries, so generated code states it and this is where it is held.

use std::collections::HashMap;

use rmcp::model::ReadResourceResult;

/// Who may hold a cached response, matching HTTP `Cache-Control`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheScope {
    /// Any cache, including one shared between users.
    #[default]
    Public,
    /// Only the requesting client's own cache.
    ///
    /// Say this explicitly for anything user-specific: a shared cache holding a per-user
    /// response leaks it to whoever asks next.
    Private,
}

impl CacheScope {
    /// The wire name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Private => "private",
        }
    }
}

impl From<CacheScope> for rmcp::model::CacheScope {
    fn from(scope: CacheScope) -> Self {
        match scope {
            CacheScope::Public => Self::Public,
            CacheScope::Private => Self::Private,
        }
    }
}

/// How long a response stays fresh, and who may share it.
///
/// A TTL of zero means immediately stale, which is also what a client assumes when no hint is
/// sent — so a zero hint is indistinguishable from none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheHint {
    /// Freshness lifetime in milliseconds.
    pub ttl_ms: u64,
    /// Who may hold it.
    pub scope: CacheScope,
}

impl CacheHint {
    /// A hint good for `ttl_ms`, cacheable by anyone.
    pub fn public(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            scope: CacheScope::Public,
        }
    }

    /// A hint good for `ttl_ms`, cacheable only by the requesting client.
    pub fn private(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            scope: CacheScope::Private,
        }
    }
}

/// The hints a service declares: one for its list results, plus per-resource overrides.
#[derive(Debug, Clone, Default)]
pub struct CacheHints {
    /// Applied to `tools/list`, `prompts/list`, `resources/list` and friends.
    pub list: Option<CacheHint>,
    /// Applied to `resources/read`, keyed by URI. A resource with no entry falls back to
    /// [`CacheHints::list`], so a declared TTL still covers what it does not override.
    pub resources: HashMap<String, CacheHint>,
}

impl CacheHints {
    /// Whether anything was declared. Nothing declared means nothing to stamp.
    pub fn is_empty(&self) -> bool {
        self.list.is_none() && self.resources.is_empty()
    }

    /// The hint that applies to reading `uri`.
    pub fn for_resource(&self, uri: &str) -> Option<CacheHint> {
        self.resources.get(uri).copied().or(self.list)
    }

    /// Stamps the hint for `uri` onto a read result.
    ///
    /// The scope is written even when the declared value is the default, because the SDK has
    /// already defaulted it to public by this point — leaving a private resource alone would
    /// publish a per-user response to any shared cache.
    ///
    /// Go does this from server middleware, since its SDK builds list results internally and
    /// leaves no field to set. rmcp's handlers return their results, so the honest place is
    /// wherever generated code builds one — no interception required.
    #[must_use]
    pub fn apply_to_read(&self, uri: &str, result: ReadResourceResult) -> ReadResourceResult {
        match self.for_resource(uri) {
            None => result,
            Some(hint) => result
                .with_ttl_ms(hint.ttl_ms)
                .with_cache_scope(hint.scope.into()),
        }
    }
}
