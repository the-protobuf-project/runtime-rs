//! Resource names as Go's repository stores them: checked against the pattern
//! (`repox.SplitName`), minted for a root resource created without one, the row's id and its
//! parent's id taken from the name, and references to top-level resources stored as their
//! bare id and read back as names.

use prost_reflect::{DynamicMessage, FieldDescriptor, Kind as ProtoKind, Value as P};

use super::shape::new_etag;
use super::{Reference, Rules};
use crate::records::{Resource, Row, Value};
use crate::{Error, Result};

/// [`Rules::pattern`], [`Rules::parent_column`] and [`Rules::references`], resolved.
#[derive(Clone, Debug, Default)]
pub(super) struct Names {
    /// The pattern's collections, `["publishers", "books"]`; empty checks nothing.
    pub(super) collections: Vec<String>,
    /// A root resource's field-less generated key, set to its name's last id.
    id_column: Option<String>,
    /// The field-less column holding the parent's id.
    pub(super) parent_column: Option<String>,
    /// Each reference field and the `collection/` its bare id is read back under.
    refs: Vec<(FieldDescriptor, String)>,
}

/// The collections of `pattern`: `a/{a}/b/{b}` is `[a, b]`.
fn parse(pattern: &str) -> Result<Vec<String>> {
    let parts: Vec<&str> = pattern.split('/').collect();
    let ok = parts.len().is_multiple_of(2)
        && parts.chunks(2).all(|p| {
            !p[0].is_empty() && !p[0].contains('{') && p[1].starts_with('{') && p[1].ends_with('}')
        });
    if !ok {
        return Err(Error::Invalid(format!(
            "{pattern:?} is not a resource name pattern"
        )));
    }
    Ok(parts.chunks(2).map(|p| p[0].to_owned()).collect())
}

impl Names {
    pub(super) fn new(res: &Resource, rules: &Rules) -> Result<Self> {
        let collections = match rules.pattern.as_str() {
            "" => Vec::new(),
            p => parse(p)?,
        };
        let fieldless = |name: &str| {
            res.lookup_column(name)
                .filter(|c| c.field.is_empty())
                .ok_or_else(|| {
                    Error::Invalid(format!("{} has no field-less column {name:?}", res.name))
                })
        };
        let parent_column = match &rules.parent_column {
            Some(c) if collections.len() != 2 => {
                return Err(Error::Invalid(format!(
                    "parent column {c:?} needs a pattern of two collections; a parent that is \
                     itself nested is keyed by a surrogate the repository cannot derive"
                )));
            }
            Some(c) => Some(fieldless(c)?.name.clone()),
            None => None,
        };
        let id_column = (collections.len() == 1)
            .then(|| {
                res.columns
                    .iter()
                    .find(|c| c.field.is_empty() && c.generated.is_some())
            })
            .flatten()
            .map(|c| c.name.clone());
        let mut refs = Vec::new();
        for Reference { field, pattern } in &rules.references {
            let f = res
                .message
                .get_field_by_name(field)
                .filter(|f| matches!(f.kind(), ProtoKind::String) && !f.is_list() && !f.is_map());
            let f = f.ok_or_else(|| {
                Error::Invalid(format!("{} has no string field {field:?}", res.name))
            })?;
            match parse(pattern)?.as_slice() {
                [one] => refs.push((f, format!("{one}/"))),
                _ => {
                    return Err(Error::Invalid(format!(
                        "reference {field:?} names {pattern:?}: only a top-level resource is \
                         stored by its bare id; a nested one is stored whole and needs no rule"
                    )));
                }
            }
        }
        Ok(Self {
            collections,
            id_column,
            parent_column,
            refs,
        })
    }

    /// The ids of `name`, refused as `Invalid` unless it fits the pattern. Go's `SplitName`.
    pub(super) fn split(&self, name: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Err(Error::Invalid("a resource name is required".into()));
        }
        if self.collections.is_empty() {
            return Ok(vec![name.to_owned()]);
        }
        let parts: Vec<&str> = name.split('/').collect();
        let fits = parts.len() == 2 * self.collections.len()
            && parts
                .chunks(2)
                .zip(&self.collections)
                .all(|(p, c)| p[0] == c && !p[1].is_empty());
        if !fits {
            return Err(Error::Invalid(format!("malformed resource name {name:?}")));
        }
        Ok(parts.chunks(2).map(|p| p[1].to_owned()).collect())
    }

    /// The name a create stores, and the field-less columns it supplies. A root resource
    /// without a name gets a minted ULID, as Go's `Create`; so does a nested one named by its
    /// parent and collection with an empty id (`publishers/p/books/`). A root's key column is
    /// its id; a nested resource's is the generated surrogate, as Go keys it.
    pub(super) fn create(&self, name: &str) -> Result<(String, Row)> {
        if self.collections.is_empty() {
            return Ok((name.to_owned(), Row::new()));
        }
        let minted = match (name, self.collections.len()) {
            ("", 1) => Some(format!("{}/{}", self.collections[0], new_etag())),
            (n, _) if n.ends_with('/') => Some(format!("{n}{}", new_etag())),
            _ => None,
        };
        let name = minted.unwrap_or_else(|| name.to_owned());
        let ids = self.split(&name)?;
        let mut cols = Row::new();
        if let Some(c) = &self.id_column {
            cols.insert(c.clone(), Value::String(ids[ids.len() - 1].clone()));
        }
        if let Some(c) = &self.parent_column {
            cols.insert(c.clone(), Value::String(ids[ids.len() - 2].clone()));
        }
        Ok((name, cols))
    }

    /// `msg` as stored: each reference its bare id, Go's `repox.LastSegment`.
    pub(super) fn as_stored(&self, msg: &mut DynamicMessage) {
        for (f, _) in &self.refs {
            let v = msg.get_field(f).as_str().unwrap_or_default().to_owned();
            if let Some((_, id)) = v.rsplit_once('/') {
                msg.set_field(f, P::String(id.to_owned()));
            }
        }
    }

    /// `msg` as read: each stored id back under its collection, as Go's `toProto`.
    pub(super) fn as_read(&self, msg: &mut DynamicMessage) {
        for (f, prefix) in &self.refs {
            let v = msg.get_field(f).as_str().unwrap_or_default().to_owned();
            if !v.is_empty() && !v.contains('/') {
                msg.set_field(f, P::String(format!("{prefix}{v}")));
            }
        }
    }
}
