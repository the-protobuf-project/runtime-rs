//! Reading a row back by the descriptor: where a column is, whether it is `NULL`, and the
//! Rust types an engine may return it as. What Go gets for free scanning into `map[string]any`.

use super::engine::map;
use crate::records::Column;
use crate::{Error, Result};

/// The first of several Rust types a column decodes as, converted to a [`Value`]: engines
/// differ in which integer width or time type a declared column comes back as.
macro_rules! first_of {
    ($row:expr, $i:expr; $($t:ty => $f:expr),+ $(,)?) => {{
        let mut out: Option<$crate::records::Value> = None;
        $(if out.is_none() {
            if let Ok(v) = sqlx::Row::try_get::<$t, _>($row, $i) {
                let conv: fn($t) -> $crate::records::Value = $f;
                out = Some(conv(v));
            }
        })+
        out
    }};
}
pub(crate) use first_of;

/// The column's index in `row`, or `None` when the row has no such column, and whether the
/// value there is `NULL`.
pub(crate) fn locate<R: sqlx::Row>(row: &R, name: &str) -> Result<Option<(usize, bool)>>
where
    usize: sqlx::ColumnIndex<R>,
{
    use sqlx::{Column as _, ValueRef as _};
    let Some(i) = row.columns().iter().position(|c| c.name() == name) else {
        return Ok(None);
    };
    let null = row.try_get_raw(i).map_err(map)?.is_null();
    Ok(Some((i, null)))
}

/// The error for a column no candidate type could read.
pub(crate) fn undecodable(col: &Column) -> Error {
    Error::Invalid(format!(
        "column {:?} cannot be read as {}: the table's type does not match the descriptor",
        col.name, col.kind
    ))
}
