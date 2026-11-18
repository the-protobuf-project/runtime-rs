//! The shared runtime protoc-gen-store's time-series stores (`lang=rust
//! target=influxdb`) build on, as runtime-go's storex is for gorm:
//! [`Query`], the window-and-tag query a store's `query()` starts, the
//! [`Column`] its tag and field enums implement, and strict record decoding.
//!
//! It was generated into every crate as `seriesx`; the generated crates now
//! re-export it from here (`pub use database::seriesx;`), so a fix reaches
//! every project with runtime-rs rather than with a regeneration.

pub mod decode;
mod query;

pub use query::Query;

use crate::store::{Reducer, Reduction};

/// A stored column a query names: a store's `Tag` or `Field` variant.
pub trait Column: Copy {
    /// The column's stored name.
    fn column(self) -> &'static str;
}

/// Reduces `column` with `func`, reported as `<func>_<column>` ("avg_temperature").
pub fn reduce(func: Reducer, column: impl Column) -> Reduction {
    let name = match func {
        Reducer::Count => "count",
        Reducer::Sum => "sum",
        Reducer::Avg => "avg",
        Reducer::Min => "min",
        Reducer::Max => "max",
    };
    Reduction::new(func, column.column(), format!("{name}_{}", column.column()))
}

/// A filter literal for `value`. The runtime's filters have no escapes, so
/// the quote is whichever the value does not contain.
fn literal(value: &str) -> std::result::Result<String, String> {
    if !value.contains('"') {
        Ok(format!("\"{value}\""))
    } else if !value.contains('\'') {
        Ok(format!("'{value}'"))
    } else {
        Err(format!(
            "filter value {value:?} holds both quote characters"
        ))
    }
}
