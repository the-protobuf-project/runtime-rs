//! Immutable resource descriptions shared by every backend.

use super::{Error, Result};
use std::collections::BTreeSet;

/// Scalar domain of a stored column. Timestamps are signed epoch nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// UTF-8 string.
    String,
    /// Signed 64-bit integer.
    Int,
    /// Unsigned 64-bit integer.
    Uint,
    /// Finite 64-bit floating-point number.
    Float,
    /// Boolean.
    Bool,
    /// Signed nanoseconds since the Unix epoch.
    Timestamp,
    /// A protobuf enum, stored as its i32 number.
    Enum,
}

/// How a column participates in a time-series point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// String lookup key for record stores; not a time-series uniqueness claim.
    Key,
    /// String dimension participating in point identity.
    Tag,
    /// Stored observation value.
    Field,
    /// Observation time participating in point identity.
    Timestamp,
}

/// A named scalar and its storage role. Columns are required on writes.
#[derive(Clone, Debug)]
pub struct Column {
    /// Stored name; Serde models use matching names or serde rename attributes.
    pub name: String,
    /// Scalar domain.
    pub kind: Kind,
    /// Point identity or value role.
    pub role: Role,
}

impl Column {
    /// Describes a column; Resource construction validates its combination.
    pub fn new(name: impl Into<String>, kind: Kind, role: Role) -> Self {
        Self {
            name: name.into(),
            kind,
            role,
        }
    }
}

/// Validated table description, independent of any backend SDK.
///
/// `name` is the message name a [`Registry`](super::Registry) looks the
/// resource up by, as runtime-go's `Resource.Name`; it defaults to the table.
#[derive(Clone, Debug)]
pub struct Resource {
    name: String,
    table: String,
    columns: Vec<Column>,
}

impl Resource {
    /// Validates names, uniqueness, and scalar/role combinations.
    /// Requires a record key or timestamp, and at least one value field.
    pub fn new(table: impl Into<String>, columns: Vec<Column>) -> Result<Self> {
        let table = table.into();
        check_name(&table)?;
        let mut names = BTreeSet::new();
        let mut times = 0;
        let mut fields = 0;
        let mut keys = 0;
        for col in &columns {
            check_name(&col.name)?;
            if !names.insert(&col.name) {
                return Err(Error::Invalid(format!("duplicate column {}", col.name)));
            }
            match (col.role, col.kind) {
                (Role::Timestamp, Kind::Timestamp) => times += 1,
                (Role::Key, Kind::String) => keys += 1,
                (Role::Tag, Kind::String) => (),
                (Role::Field, kind) if kind != Kind::Timestamp => fields += 1,
                _ => {
                    return Err(Error::Invalid(format!(
                        "invalid role/type for {}",
                        col.name
                    )));
                }
            }
        }
        if times > 1 || keys > 1 || (times == 0 && keys == 0) || fields == 0 {
            return Err(Error::Invalid(
                "one key or timestamp and at least one field required".into(),
            ));
        }
        Ok(Self {
            name: table.clone(),
            table,
            columns,
        })
    }

    /// Sets the message name a registry looks this resource up by.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Message name, as generated from the proto.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Physical table or measurement name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// All columns in declaration order.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Looks up a declared column, rejecting unknown query identifiers.
    pub fn column(&self, name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| Error::Invalid(format!("unknown column {name}")))
    }

    /// Observation-time column, or an error for a record-only resource.
    pub fn time_column(&self) -> Result<&str> {
        self.columns
            .iter()
            .find(|c| c.role == Role::Timestamp)
            .map(|c| c.name.as_str())
            .ok_or_else(|| {
                Error::Invalid("time-series operations require a timestamp column".into())
            })
    }

    /// Lookup key declared for a record-store backend, when present.
    pub fn key_column(&self) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.role == Role::Key)
            .map(|c| c.name.as_str())
    }
}

/// Portable identifiers deliberately use a conservative ASCII subset.
pub(crate) fn check_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
    {
        return Err(Error::Invalid(format!("invalid identifier {name:?}")));
    }
    Ok(())
}
