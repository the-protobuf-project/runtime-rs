//! The order rows are read in, and the page token that holds a place in it.

use super::errors::invalid;
use crate::store::window::Range;
use crate::store::{Record, Resource, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt::Write as _;

/// One position in a table's read order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Key {
    pub time: i64,
    pub segment: String,
    pub row: u64,
}

/// `(time, segment, row)` compared in read order: time reversed when descending, ties always
/// in write order, as a stable sort leaves them.
pub(super) fn order(a: (i64, &str, u64), b: (i64, &str, u64), descending: bool) -> Ordering {
    let time = a.0.cmp(&b.0);
    let time = if descending { time.reverse() } else { time };
    time.then_with(|| a.1.cmp(b.1)).then(a.2.cmp(&b.2))
}

pub(super) fn parts(key: &Key) -> (i64, &str, u64) {
    (key.time, &key.segment, key.row)
}

/// A kept row, ordered so the heap's top is the worst.
pub(super) struct Entry {
    pub key: Key,
    pub descending: bool,
    pub record: Record,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        order(parts(&self.key), parts(&other.key), self.descending)
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Entry {}

/// What a token holds. The scope stops a token being replayed against another query.
#[derive(Serialize, Deserialize)]
pub(super) struct Cursor {
    pub scope: String,
    pub total: i64,
    pub after: Key,
}

impl Cursor {
    pub(super) fn encode(&self) -> Result<String> {
        let json = serde_json::to_vec(self).map_err(super::errors::backend)?;
        Ok(URL_SAFE_NO_PAD.encode(json))
    }

    pub(super) fn decode(token: &str, scope: &str) -> Result<Self> {
        let bad = || invalid("page_token is not from this backend");
        let json = URL_SAFE_NO_PAD.decode(token).map_err(|_| bad())?;
        let cursor: Self = serde_json::from_slice(&json).map_err(|_| bad())?;
        if cursor.scope == scope {
            Ok(cursor)
        } else {
            Err(invalid("page_token belongs to another query"))
        }
    }
}

pub(super) fn scope(resource: &Resource, range: &Range) -> String {
    let input = format!(
        "{:?}{:?}{:?}{:?}{:?}",
        resource.table(),
        range.start,
        range.end,
        range.descending,
        range.predicates
    );
    let mut scope = String::with_capacity(32);
    for byte in &Sha256::digest(input.as_bytes())[..16] {
        write!(scope, "{byte:02x}").expect("writing to a String cannot fail");
    }
    scope
}

pub(super) fn owned((time, segment, row): (i64, &str, u64)) -> Key {
    Key {
        time,
        segment: segment.to_owned(),
        row,
    }
}
