//! Resource descriptors to and from self-describing Arrow schemas.
//!
//! The table name and each column's role travel as schema and field metadata,
//! so a file read back without the original descriptor still knows which
//! columns are tags, fields and the observation time.

use super::errors::invalid;
use crate::store::{Column, Kind, Resource, Result, Role};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;

/// Schema metadata key holding the resource's table name.
pub(super) const TABLE_KEY: &str = "tpp.table";
/// Field metadata key holding a column's role.
pub(super) const ROLE_KEY: &str = "tpp.role";

/// Arrow type for each portable kind. Timestamps carry UTC so viewers render
/// them as instants rather than naive integers.
pub(super) fn data_type(kind: Kind) -> DataType {
    match kind {
        Kind::String => DataType::Utf8,
        Kind::Int => DataType::Int64,
        Kind::Enum => DataType::Int32,
        Kind::Uint => DataType::UInt64,
        Kind::Float => DataType::Float64,
        Kind::Bool => DataType::Boolean,
        Kind::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Key => "key",
        Role::Tag => "tag",
        Role::Field => "field",
        Role::Timestamp => "timestamp",
    }
}

/// Every column is required on writes, so no Arrow field is nullable.
pub(super) fn to_schema(resource: &Resource) -> Schema {
    let fields: Vec<Field> = resource
        .columns()
        .iter()
        .map(|col| {
            Field::new(&col.name, data_type(col.kind), false).with_metadata(HashMap::from([(
                ROLE_KEY.to_owned(),
                role_name(col.role).to_owned(),
            )]))
        })
        .collect();
    Schema::new(fields).with_metadata(HashMap::from([(
        TABLE_KEY.to_owned(),
        resource.table().to_owned(),
    )]))
}

/// Rebuilds a descriptor from a file written by this driver.
pub(super) fn from_schema(schema: &Schema) -> Result<Resource> {
    let table = schema
        .metadata()
        .get(TABLE_KEY)
        .ok_or_else(|| invalid("arrow file has no tpp.table metadata"))?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let kind = match field.data_type() {
                DataType::Utf8 => Kind::String,
                DataType::Int64 => Kind::Int,
                DataType::Int32 => Kind::Enum,
                DataType::UInt64 => Kind::Uint,
                DataType::Float64 => Kind::Float,
                DataType::Boolean => Kind::Bool,
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Kind::Timestamp,
                other => return Err(invalid(format!("unsupported arrow type {other}"))),
            };
            let role = match field.metadata().get(ROLE_KEY).map(String::as_str) {
                Some("key") => Role::Key,
                Some("tag") => Role::Tag,
                Some("field") => Role::Field,
                Some("timestamp") => Role::Timestamp,
                _ => return Err(invalid(format!("column {} has no role", field.name()))),
            };
            Ok(Column::new(field.name(), kind, role))
        })
        .collect::<Result<_>>()?;
    Resource::new(table, columns)
}

/// True when a stored file holds exactly the columns a caller asked for.
pub(super) fn matches(stored: &Resource, wanted: &Resource) -> bool {
    stored.table() == wanted.table()
        && stored.columns().len() == wanted.columns().len()
        && stored
            .columns()
            .iter()
            .zip(wanted.columns())
            .all(|(a, b)| a.name == b.name && a.kind == b.kind && a.role == b.role)
}
