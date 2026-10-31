//! Validated bucketed reductions built from AggregateOptions.

use super::filter::predicates;
use super::timeseries::DEFAULT_BUCKET_LIMIT;
use super::window::{Range, time_column};
use super::{AggregateOptions, Bucket, Error, Record, Reducer, Resource, Result, Value};

/// Reduction function, named as the SQL backends spell it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Reduction {
    Count,
    Mean,
    Sum,
    Min,
    Max,
}

#[derive(Clone, Debug)]
pub(crate) struct Reduce {
    pub column: String,
    pub function: Reduction,
    pub alias: String,
}

/// Bucketed reduction over a window. Buckets are ordered oldest first.
#[derive(Clone, Debug)]
pub(crate) struct Aggregate {
    pub range: Range,
    pub every_seconds: u32,
    pub reduce: Vec<Reduce>,
    pub group_by: Vec<String>,
}

impl Aggregate {
    /// The reduction of an [`AggregateOptions`] read against `res`.
    pub fn of(res: &Resource, opts: &AggregateOptions) -> Result<Self> {
        time_column(res, &opts.time_column)?;
        let every = opts.every;
        let every_seconds = u32::try_from(every.as_secs())
            .ok()
            .filter(|&s| s > 0 && every.subsec_nanos() == 0)
            .ok_or_else(|| {
                Error::Invalid("aggregation needs a whole-second bucket width".into())
            })?;
        let range = Range {
            start: opts.start,
            end: opts.end,
            limit: if opts.limit == 0 {
                DEFAULT_BUCKET_LIMIT
            } else {
                opts.limit
            },
            descending: false,
            predicates: predicates(res, &opts.filter)?,
        };
        range.validate()?;
        let reduce = opts
            .reduce
            .iter()
            .map(|r| Reduce {
                column: r.column.clone(),
                alias: r.alias.clone(),
                function: match r.func {
                    Reducer::Count => Reduction::Count,
                    Reducer::Sum => Reduction::Sum,
                    Reducer::Avg => Reduction::Mean,
                    Reducer::Min => Reduction::Min,
                    Reducer::Max => Reduction::Max,
                },
            })
            .collect();
        Ok(Self {
            range,
            every_seconds,
            reduce,
            group_by: opts.group_by.clone(),
        })
    }

    /// Backend output rows (time column, group tags, aliases) as buckets.
    pub fn buckets(&self, res: &Resource, rows: Vec<Record>) -> Result<Vec<Bucket>> {
        let time = res.time_column()?;
        Ok(rows
            .into_iter()
            .map(|row| Bucket {
                start: row.get(time).and_then(Value::as_i64).unwrap_or_default(),
                group: self
                    .group_by
                    .iter()
                    .map(|g| (g.clone(), text(row.get(g))))
                    .collect(),
                values: self
                    .reduce
                    .iter()
                    .map(|r| {
                        (
                            r.alias.clone(),
                            row.get(&r.alias).and_then(Value::as_f64).unwrap_or(0.0),
                        )
                    })
                    .collect(),
            })
            .collect())
    }
}

fn text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}
