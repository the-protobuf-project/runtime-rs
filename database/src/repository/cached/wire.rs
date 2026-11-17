//! Encodings shared with Go, and small helpers.

use std::sync::{Arc, OnceLock};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cache::CacheError;
use cache::core::Options;
use prost::Message;

use super::Record;
use crate::cached::aside::cache_error;
use crate::{Error, Result};

/// Initializes `cell` with `select`'s database once, remembering a failure as text.
pub(super) fn once(
    cell: &OnceLock<std::result::Result<Arc<cache::DB>, String>>,
    select: impl FnOnce() -> cache::Result<cache::DB>,
) -> Result<Arc<cache::DB>> {
    cell.get_or_init(|| select().map(Arc::new).map_err(|e| e.to_string()))
        .clone()
        .map_err(|e| Error::Invalid(format!("cache: {e}")))
}

pub(super) fn record_options<M>(record: &Record<M>) -> Options {
    Options {
        ttl: record.ttl,
        stale: record.stale,
        ..Options::default()
    }
}

/// Writes the record into the Indexed strategy under its index fields, replacing what it
/// was filed under: update first, because only update refiles; create when it is not there.
pub(super) async fn refile<M: Message>(
    db: &cache::DB,
    record: &Record<M>,
    name: &str,
    m: &M,
) -> Result<()> {
    let value = wire(&m.encode_to_vec()).map_err(cache_error)?;
    let mut opts = record_options(record);
    for index in &record.indexes {
        if let Some(v) = (index.value)(m) {
            opts = opts.with_index(index.field, v);
        }
    }
    match db.indexed.update(name, &value, &opts).await {
        Err(CacheError::NotFound) => db
            .indexed
            .create(&value, &opts.with_id(name))
            .await
            .map(|_| ())
            .map_err(cache_error),
        other => other.map_err(cache_error),
    }
}

/// Record bytes as Go's codec stores a `[]byte`: a base64 JSON string.
pub(super) fn wire(bytes: &[u8]) -> cache::Result<Vec<u8>> {
    serde_json::to_vec(&BASE64.encode(bytes)).map_err(|e| CacheError::Internal(e.to_string()))
}

pub(super) fn unwire(body: &[u8]) -> Result<Vec<u8>> {
    let text: String = serde_json::from_slice(body)?;
    BASE64.decode(text).map_err(|e| Error::Backend {
        backend: "cache",
        source: Box::new(e),
    })
}

pub(super) fn decoding(err: prost::DecodeError) -> Error {
    Error::Backend {
        backend: "cache",
        source: Box::new(err),
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    pub(super) const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

/// A string as Go's `encoding/json` writes it: HTML-safe, so `<`, `>` and `&` are escaped,
/// as are U+2028 and U+2029, with lowercase hex.
pub fn go_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' | '>' | '&' => out.push_str(&format!("\\u00{:02x}", c as u32)),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u00{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
