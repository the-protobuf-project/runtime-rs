//! Generated handles for 64-bit integer and floating-point columns.

use crate::columns::{OrderBy, OrderTerm};
use crate::predicate::{pred, Predicate};
use crate::scalars::Int64;

/// A generated handle for a 64-bit integer column.
pub struct Int64Field {
    /// The column's wire name.
    pub col: &'static str,
}

impl Int64Field {
    /// Matches rows where the column equals `v`.
    pub fn eq(&self, v: Int64) -> Predicate {
        pred(self.col, "_eq", v)
    }

    /// Matches rows where the column does not equal `v`.
    pub fn neq(&self, v: Int64) -> Predicate {
        pred(self.col, "_neq", v)
    }

    /// Matches rows where the column is greater than `v`.
    pub fn gt(&self, v: Int64) -> Predicate {
        pred(self.col, "_gt", v)
    }

    /// Matches rows where the column is greater than or equal to `v`.
    pub fn gte(&self, v: Int64) -> Predicate {
        pred(self.col, "_gte", v)
    }

    /// Matches rows where the column is less than `v`.
    pub fn lt(&self, v: Int64) -> Predicate {
        pred(self.col, "_lt", v)
    }

    /// Matches rows where the column is less than or equal to `v`.
    pub fn lte(&self, v: Int64) -> Predicate {
        pred(self.col, "_lte", v)
    }

    /// Matches rows where the column is one of `vs`.
    pub fn is_in(&self, vs: impl IntoIterator<Item = Int64>) -> Predicate {
        pred(self.col, "_in", vs.into_iter().collect::<Vec<Int64>>())
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

/// A generated handle for a floating-point column.
pub struct FloatField {
    /// The column's wire name.
    pub col: &'static str,
}

impl FloatField {
    /// Matches rows where the column equals `v`.
    pub fn eq(&self, v: f64) -> Predicate {
        pred(self.col, "_eq", v)
    }

    /// Matches rows where the column does not equal `v`.
    pub fn neq(&self, v: f64) -> Predicate {
        pred(self.col, "_neq", v)
    }

    /// Matches rows where the column is greater than `v`.
    pub fn gt(&self, v: f64) -> Predicate {
        pred(self.col, "_gt", v)
    }

    /// Matches rows where the column is greater than or equal to `v`.
    pub fn gte(&self, v: f64) -> Predicate {
        pred(self.col, "_gte", v)
    }

    /// Matches rows where the column is less than `v`.
    pub fn lt(&self, v: f64) -> Predicate {
        pred(self.col, "_lt", v)
    }

    /// Matches rows where the column is less than or equal to `v`.
    pub fn lte(&self, v: f64) -> Predicate {
        pred(self.col, "_lte", v)
    }

    /// Matches rows where the column is one of `vs`.
    pub fn is_in(&self, vs: impl IntoIterator<Item = f64>) -> Predicate {
        pred(self.col, "_in", vs.into_iter().collect::<Vec<f64>>())
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
