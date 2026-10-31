//! Bucketed reductions run on the server with descriptor-checked identifiers.

use super::sql::{self, Query, quote};
use crate::store::check_name;
use crate::store::window::{Aggregate, Reduction};
use crate::store::{Error, Kind, Resource, Result, Role};
use std::collections::BTreeSet;

pub(super) fn aggregate(resource: &Resource, options: &Aggregate) -> Result<Query> {
    if options.every_seconds == 0 || options.reduce.is_empty() {
        return Err(Error::Invalid(
            "aggregation needs a positive bucket width and reductions".into(),
        ));
    }
    let mut query = sql::bounds(resource, &options.range)?;
    let time = quote(resource.time_column()?);
    let bucket = format!(
        "date_bin(INTERVAL '{} seconds', \"time\")",
        options.every_seconds
    );
    let mut select = vec![format!("{bucket} AS {time}")];
    let mut groups = vec![bucket];
    let mut order = vec![format!(
        "{time} {}",
        if options.range.descending {
            "DESC"
        } else {
            "ASC"
        }
    )];
    let mut names = BTreeSet::from([resource.time_column()?.to_owned()]);
    for name in &options.group_by {
        if resource.column(name)?.role != Role::Tag || !names.insert(name.clone()) {
            return Err(Error::Invalid(format!(
                "group must be a distinct tag: {name}"
            )));
        }
        select.push(quote(name));
        groups.push(quote(name));
        order.push(format!("{} ASC", quote(name)));
    }
    for reduction in &options.reduce {
        check_name(&reduction.alias)?;
        let col = resource.column(&reduction.column)?;
        if col.role != Role::Field
            || !matches!(col.kind, Kind::Int | Kind::Uint | Kind::Float)
            || !names.insert(reduction.alias.clone())
        {
            return Err(Error::Invalid(
                "reductions need numeric fields and unique output names".into(),
            ));
        }
        let func = match reduction.function {
            Reduction::Count => "count",
            Reduction::Mean => "avg",
            Reduction::Sum => "sum",
            Reduction::Min => "min",
            Reduction::Max => "max",
        };
        select.push(format!(
            "{func}({}) AS {}",
            quote(&col.name),
            quote(&reduction.alias)
        ));
    }
    query.sql = format!(
        "SELECT {} FROM {} WHERE {} GROUP BY {} ORDER BY {} LIMIT {}",
        select.join(", "),
        quote(resource.table()),
        query.sql,
        groups.join(", "),
        order.join(", "),
        options.range.limit
    );
    Ok(query)
}
