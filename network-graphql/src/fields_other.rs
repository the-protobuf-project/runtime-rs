//! Generated handles for boolean, JSON, and enum columns.

use std::marker::PhantomData;

use serde::Serialize;

use crate::columns::{OrderBy, OrderTerm};
use crate::predicate::{pred, Predicate};

/// A generated handle for a boolean column.
pub struct BoolField {
    /// The column's wire name.
    pub col: &'static str,
}

impl BoolField {
    /// Matches rows where the column equals `v`.
    pub fn eq(&self, v: bool) -> Predicate {
        pred(self.col, "_eq", v)
    }

    /// Matches rows where the column does not equal `v`.
    pub fn neq(&self, v: bool) -> Predicate {
        pred(self.col, "_neq", v)
    }

    /// Matches rows where the column is (`v = true`) or is not (`v = false`) null.
    pub fn is_null(&self, v: bool) -> Predicate {
        pred(self.col, "_is_null", v)
    }

    /// Orders results by this column ascending.
    pub fn asc(&self) -> OrderTerm {
        OrderTerm {
            col: self.col.to_string(),
            dir: OrderBy::Asc,
        }
    }

    /// Orders results by this column descending.
    pub fn desc(&self) -> OrderTerm {
        OrderTerm {
            col: self.col.to_string(),
            dir: OrderBy::Desc,
        }
    }
}

/// A generated handle for a JSON/JSONB column. Filtering is limited to equality and
/// null checks.
pub struct JSONField {
    /// The column's wire name.
    pub col: &'static str,
}

impl JSONField {
    /// Matches rows where the column equals the JSON value `v`.
    pub fn eq(&self, v: serde_json::Value) -> Predicate {
        pred(self.col, "_eq", v)
    }

    /// Matches rows where the column is (`v = true`) or is not (`v = false`) null.
    pub fn is_null(&self, v: bool) -> Predicate {
        pred(self.col, "_is_null", v)
    }
}

/// A generated handle for an enum column, parameterized by the enum type so operators
/// take typed values.
pub struct EnumField<E> {
    /// The column's wire name.
    pub col: &'static str,
    pub(crate) _marker: PhantomData<E>,
}

impl<E: Serialize> EnumField<E> {
    /// Matches rows where the column equals `v`.
    pub fn eq(&self, v: E) -> Predicate {
        pred(self.col, "_eq", v)
    }

    /// Matches rows where the column does not equal `v`.
    pub fn neq(&self, v: E) -> Predicate {
        pred(self.col, "_neq", v)
    }

    /// Matches rows where the column is one of `vs`.
    pub fn is_in(&self, vs: impl IntoIterator<Item = E>) -> Predicate {
        pred(self.col, "_in", vs.into_iter().collect::<Vec<E>>())
    }

    /// Matches rows where the column is (`v = true`) or is not (`v = false`) null.
    pub fn is_null(&self, v: bool) -> Predicate {
        pred(self.col, "_is_null", v)
    }

    /// Orders results by this column ascending.
    pub fn asc(&self) -> OrderTerm {
        OrderTerm {
            col: self.col.to_string(),
            dir: OrderBy::Asc,
        }
    }

    /// Orders results by this column descending.
    pub fn desc(&self) -> OrderTerm {
        OrderTerm {
            col: self.col.to_string(),
            dir: OrderBy::Desc,
        }
    }
}
