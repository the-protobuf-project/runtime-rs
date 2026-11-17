//! An AIP-132 `order_by` resolved against a resource's columns. Go's `orderClause` in
//! `orm/filter.go`, shared by every backend that orders in its own terms.

use crate::records::Resource;

/// An AIP-132 order expression, `field [desc], other`, as `(column, descending)` pairs. The
/// key always ends the order, so an offset page is stable. Any field `res` does not declare
/// orders by the key alone instead of failing, as Go's `orderClause` does: ordering is
/// presentation, and the right records in an unexpected order beat an error. A direction
/// other than `desc` is ascending.
pub fn order_by(res: &Resource, expr: &str) -> Vec<(String, bool)> {
    let key = (res.pk_column.clone(), false);
    let mut out = Vec::new();
    for item in expr.split(',').filter(|s| !s.trim().is_empty()) {
        let mut words = item.split_whitespace();
        let Some(col) = words.next().and_then(|f| res.lookup_column(f)) else {
            return vec![key];
        };
        let dir = matches!(words.next(), Some(w) if w.eq_ignore_ascii_case("desc"));
        out.push((col.name.clone(), dir));
    }
    if !out.iter().any(|(c, _)| *c == res.pk_column) {
        out.push(key);
    }
    out
}
