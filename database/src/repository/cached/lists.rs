//! The list cache: per-parent versions and Go's page keys and encoding.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cache::CacheError;
use cache::core::Options;
use prost::Message;
use sha2::{Digest, Sha256};

use super::wire::{decoding, go_json_string, hex};
use super::{ANY_PARENT, CachedRepository, List};
use crate::cached::aside::cache_error;
use crate::repository::{ListInput, Page};
use crate::{Error, Result};

impl<M> CachedRepository<M>
where
    M: Message + Default + Send + Sync + 'static,
{
    /// Moves the list versions a write to `name` reaches on.
    pub(super) async fn rotate(&self, name: &str) {
        let Some(list) = &self.plan.list else { return };
        let Some(parent) = list_parent(name, list.collections) else {
            return;
        };
        let db = match self.list_db(list) {
            Ok(db) => db,
            Err(err) => return self.report_list(list, "rotate", &err),
        };
        let mut scopes = vec![list_scope(&parent)];
        if list.parented {
            scopes.push(ANY_PARENT.to_owned());
        }
        let opts = Options::default().with_ttl(version_ttl(list.ttl));
        for scope in scopes {
            let version = uuid::Uuid::new_v4().simple().to_string();
            let body = go_json_string(&version).into_bytes();
            if let Err(err) = db.volatile.set(&version_key(&scope), &body, &opts).await {
                self.report_list(list, "rotate", &cache_error(err));
            }
        }
    }

    pub(super) fn report_list(&self, list: &List, op: &str, err: &Error) {
        if let Some(observe) = &self.hooks.list_errors {
            observe(list.resource, op, err);
        }
    }

    /// The key of the page `input` reads, or `None` when the version cannot be read.
    pub(super) async fn page_key(
        &self,
        list: &List,
        db: &cache::DB,
        input: &ListInput,
    ) -> Option<String> {
        // Go's List(ctx, parent, in) reads listScope(parent). An empty parent of a nested
        // resource is every parent, which Go spells with `-`: the version every write moves.
        let scope = if list.parented && input.parent.is_empty() {
            ANY_PARENT.to_owned()
        } else {
            list_scope(&input.parent)
        };
        let mut body = Vec::new();
        let version = match db.volatile.get(&version_key(&scope), &mut body).await {
            Ok(()) => match serde_json::from_slice::<String>(&body) {
                Ok(version) => version,
                Err(err) => {
                    self.report_list(list, "version", &err.into());
                    return None;
                }
            },
            Err(CacheError::NotFound) => "0".to_owned(),
            Err(err) => {
                self.report_list(list, "version", &cache_error(err));
                return None;
            }
        };
        Some(page_key(&version, input))
    }
}

/// How long a list version is kept: a day, or twice a page's ttl when that is longer.
pub(super) fn version_ttl(page: Duration) -> Duration {
    (2 * page).max(Duration::from_secs(24 * 60 * 60))
}

pub(super) fn version_key(scope: &str) -> String {
    format!("version:{scope}")
}

/// The parent of the record `name` when it has the pattern of `collections`.
pub(super) fn list_parent(name: &str, collections: &[&str]) -> Option<String> {
    let segs: Vec<&str> = name.split('/').collect();
    if segs.len() != 2 * collections.len() {
        return None;
    }
    for (i, c) in collections.iter().enumerate() {
        if segs[2 * i] != *c || segs[2 * i + 1].is_empty() {
            return None;
        }
    }
    Some(segs[..segs.len() - 2].join("/"))
}

/// The version scope a list under `parent` reads: the parent, or `*` for a `-` in it.
pub(super) fn list_scope(parent: &str) -> String {
    let wildcard = parent.split('/').skip(1).step_by(2).any(|id| id == "-");
    if wildcard {
        ANY_PARENT.to_owned()
    } else {
        parent.to_owned()
    }
}

/// A page's key: the version, then the SHA-256 of the request exactly as Go's
/// `json.Marshal(struct{Parent string; In repox.ListInput})` writes it. Go's
/// `repox.ListInput` has no parent; `input.parent` is the `Parent` beside it.
pub fn page_key(version: &str, input: &ListInput) -> String {
    let body = format!(
        "{{\"Parent\":{},\"In\":{{\"PageSize\":{},\"PageToken\":{},\"OrderBy\":{},\"Filter\":{},\"ShowDeleted\":{},\"Where\":null,\"ReadMask\":null}}}}",
        go_json_string(&input.parent),
        input.page_size,
        go_json_string(&input.page_token),
        go_json_string(&input.order_by),
        go_json_string(&input.filter),
        input.show_deleted,
    );
    let sum = Sha256::digest(body.as_bytes());
    format!("page:{version}:{}", hex(&sum))
}

/// A page as Go's `listPage` encodes: each record's wire bytes, base64, and the next token.
pub(super) fn encode_page<M: Message>(page: &Page<M>) -> String {
    let items: Vec<String> = page
        .items
        .iter()
        .map(|m| go_json_string(&BASE64.encode(m.encode_to_vec())))
        .collect();
    format!(
        "{{\"items\":[{}],\"next\":{}}}",
        items.join(","),
        go_json_string(&page.next_page_token)
    )
}

pub(super) fn decode_page<M: Message + Default>(body: &[u8]) -> Result<Page<M>> {
    #[derive(serde::Deserialize)]
    struct Wire {
        items: Option<Vec<String>>,
        next: String,
    }
    let wire: Wire = serde_json::from_slice(body)?;
    let mut items = Vec::new();
    for item in wire.items.unwrap_or_default() {
        let bytes = BASE64.decode(item).map_err(|e| Error::Backend {
            backend: "cache",
            source: Box::new(e),
        })?;
        items.push(M::decode(bytes.as_slice()).map_err(decoding)?);
    }
    Ok(Page {
        items,
        next_page_token: wire.next,
    })
}
