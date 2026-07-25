//! A generated handle for a string-like column (text, id, timestamp).

use crate::columns::{OrderBy, OrderTerm};
use crate::predicate::{pred, Predicate};

/// A generated handle for a string-like column (text, id, timestamp). Its methods build
/// a [`Predicate`] for the column; ordered comparisons treat the value lexically, which
/// is correct for ISO-8601 timestamps too.
pub struct StringField {
    /// The column's wire name.
    pub col: &'static str,
}

impl StringField {
    /// Matches rows where the column equals `v`.
    pub fn eq(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_eq", v.into())
    }

    /// Matches rows where the column does not equal `v`.
    pub fn neq(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_neq", v.into())
    }

    /// Matches rows where the column is greater than `v`.
    pub fn gt(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_gt", v.into())
    }

    /// Matches rows where the column is greater than or equal to `v`.
    pub fn gte(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_gte", v.into())
    }

    /// Matches rows where the column is less than `v`.
    pub fn lt(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_lt", v.into())
    }

    /// Matches rows where the column is less than or equal to `v`.
    pub fn lte(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_lte", v.into())
    }

    /// Matches rows where the column is one of `vs`.
    pub fn is_in(&self, vs: impl IntoIterator<Item = impl Into<String>>) -> Predicate {
        pred(
            self.col,
            "_in",
            vs.into_iter().map(Into::into).collect::<Vec<String>>(),
        )
    }

    /// Matches rows where the column matches the SQL LIKE pattern `v`.
    pub fn like(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_like", v.into())
    }

    /// Matches rows where the column matches the case-insensitive LIKE pattern `v`.
    pub fn ilike(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_ilike", v.into())
    }

    /// Matches rows where the column matches the regular expression `v`.
    pub fn regex(&self, v: impl Into<String>) -> Predicate {
        pred(self.col, "_regex", v.into())
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
