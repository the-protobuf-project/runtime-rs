//! A listing under a parent ([`ListInput::parent`](super::ListInput::parent)), as Go's
//! generated `parentScope` narrows one: by the parent column when the parent's id is known,
//! across every parent when it is all `-` (AIP-159), else by the records' names.

use super::names::Names;
use crate::{Error, Result};

/// How a listing is narrowed to a parent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Scope {
    /// The parent's segments; empty for every record (no parent, or ids all `-`).
    pub(super) parent: Vec<String>,
    /// The parent column and the parent's id, `("publisher_id", "p")`, when that id is given:
    /// Go's `Where("<parent>_id = ?", id)`, restricted in code. Without one, a non-empty
    /// `parent` is matched against the records' names, `-` ids matching any: Go's
    /// `name LIKE`, which no restriction here says.
    pub(super) clause: Option<(String, String)>,
}

impl Names {
    /// The scope of a listing under `parent`. A parent that does not fit the pattern's
    /// parent collections is `Invalid`, as Go's `SplitName` refuses it.
    pub(super) fn scope(&self, parent: &str) -> Result<Scope> {
        if parent.is_empty() {
            return Ok(Scope::default());
        }
        let parts: Vec<String> = parent.split('/').map(str::to_owned).collect();
        let bad = || Error::Invalid(format!("malformed parent {parent:?}"));
        if !self.collections.is_empty() {
            let above = &self.collections[..self.collections.len() - 1];
            let fits = parts.len() == 2 * above.len()
                && parts
                    .chunks(2)
                    .zip(above)
                    .all(|(p, c)| p[0] == *c && !p[1].is_empty());
            if !fits {
                return Err(bad());
            }
        } else if parts.iter().any(String::is_empty) {
            return Err(bad());
        }
        let ids: Vec<&String> = parts.iter().skip(1).step_by(2).collect();
        let last = ids.last().map(|s| s.as_str()).unwrap_or("-");
        if ids.iter().all(|id| *id == "-") {
            return Ok(Scope::default());
        }
        let clause = match (&self.parent_column, last) {
            (Some(column), id) if id != "-" => Some((column.clone(), id.to_owned())),
            _ => None,
        };
        Ok(Scope {
            parent: parts,
            clause,
        })
    }
}

/// Whether `name` is directly under `parent` (its segments, `-` ids matching any): the
/// parent's segments, then one collection and one id.
pub(super) fn under(parent: &[String], name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() == parent.len() + 2
        && parent
            .iter()
            .zip(&parts)
            .enumerate()
            .all(|(i, (p, n))| p == n || (i % 2 == 1 && p == "-"))
}
