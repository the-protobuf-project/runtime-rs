//! The statements a write is built from: an insert from a message, and the key predicate.

use prost_reflect::DynamicMessage;

use super::sql::{bind, placeholders, quote};
use super::wire::Statement;
use crate::records::{self, Resource, Row, core};
use crate::{Error, Result};

/// The statement that stores `msg`, and the columns it stores. A null column is left out
/// rather than bound: the endpoint binds text and NULL has none, and an omitted column is
/// NULL all the same.
pub(crate) fn insert(res: &Resource, msg: &DynamicMessage) -> Result<(Statement, Row)> {
    let mut cols = records::message_to_columns(res, msg)?;
    core::fill_managed(res, &mut cols, true);
    let (mut names, mut args) = (Vec::new(), Vec::new());
    for (name, v) in &cols {
        if let Some(text) = bind(v) {
            names.push(quote(name));
            args.push(text);
        }
    }
    if names.is_empty() {
        return Err(Error::Invalid(format!(
            "d1: {} has nothing to insert",
            res.name
        )));
    }
    let text = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote(&res.table),
        names.join(", "),
        placeholders(names.len())
    );
    Ok((Statement::new(text, args), cols))
}

pub(crate) fn by_key(res: &Resource) -> String {
    format!("{} = ?", quote(&res.pk_column))
}
