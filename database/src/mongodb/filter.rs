//! AIP-160 filters and AIP-132 orders as Mongo queries and sorts. Go's `mongodb/filter.go`
//! and the `parseFilter`/`sortOf` halves of `codec.go` and `driver.go`.

use bson::{Binary, Bson, Document, doc, spec::BinarySubtype};

use super::codec::to_bson;
use super::error::invalid;
use crate::Result;
use crate::records::core::filter::{self, Comparison, Expr, Op};
use crate::records::{Resource, Value};

/// The Mongo query for `expr`: [`filter::parse`]'s tree, each column mapped to its field
/// (`_id` for the key) and each value encoded as the column stores it, so a number compares
/// as a number. An unknown column is refused by name. Go's `parseFilter`.
///
/// `AND` and `OR` are `$and` and `$or`. A `NOT` is pushed down to the comparisons by
/// [`Expr::negated`] rather than sent as `$nor` or `$not`, which match a document whose
/// field is null or missing: SQL's `NOT (a = 1)` does not, and every provider agrees with
/// SQL. For the same reason `!=` is `$nin: [value, null]`, not `$ne`. Has is
/// `$exists: true, $ne: null`, so a stored null is absent, as SQL's `IS NOT NULL` reads it.
pub(super) fn parse_filter(res: &Resource, expr: &str) -> Result<Document> {
    match filter::parse(res, expr).map_err(|e| invalid(e.to_string()))? {
        None => Ok(Document::new()),
        Some(e) => query(res, &e.without_not()),
    }
}

/// `e`, which holds no [`Expr::Not`], as a query document.
fn query(res: &Resource, e: &Expr) -> Result<Document> {
    let field = |c: &str| {
        if *c == res.pk_column {
            "_id".to_owned()
        } else {
            c.to_owned()
        }
    };
    let all = |es: &[Expr]| -> Result<Vec<Bson>> {
        es.iter()
            .map(|e| query(res, e).map(Bson::Document))
            .collect()
    };
    Ok(match e {
        Expr::And(es) => doc! { "$and": all(es)? },
        Expr::Or(es) => doc! { "$or": all(es)? },
        Expr::Not(inner) => query(res, &inner.negated())?,
        Expr::Compare(c) => doc! { field(&c.column): comparison(c)? },
        Expr::Has(c) => doc! { field(c): { "$exists": true, "$ne": Bson::Null } },
        Expr::Null(c) => doc! { field(c): Bson::Null },
    })
}

fn comparison(c: &Comparison) -> Result<Document> {
    let v = match &c.value {
        // Stored bytes are binary; an empty value is stored null, but compares as bytes here.
        Value::Bytes(b) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        v => to_bson(c.kind, v)?,
    };
    Ok(match c.op {
        Op::Eq => doc! { "$eq": v },
        Op::Ne => doc! { "$nin": [v, Bson::Null] },
        Op::Lt => doc! { "$lt": v },
        Op::Le => doc! { "$lte": v },
        Op::Gt => doc! { "$gt": v },
        Op::Ge => doc! { "$gte": v },
    })
}

/// The Mongo sort for an AIP-132 order: its first column, `desc` or ascending, the key as
/// `_id`. An unknown column sorts by `_id` rather than failing: order is presentation, and
/// the right records in an unexpected order beat an error. A column other than the key is
/// followed by `_id`, so equal values page in a stable order. Go's `sortOf`.
pub(super) fn sort_of(res: &Resource, order_by: &str) -> Document {
    let mut fields = order_by.split_whitespace();
    let Some(column) = fields.next() else {
        return doc! { "_id": 1 };
    };
    let direction = match fields.next() {
        Some(d) if d.eq_ignore_ascii_case("desc") => -1,
        _ => 1,
    };
    if column.eq_ignore_ascii_case(&res.pk_column) {
        return doc! { "_id": direction };
    }
    if res.lookup_column(column).is_none() {
        return doc! { "_id": 1 };
    }
    doc! { column: direction, "_id": 1 }
}
