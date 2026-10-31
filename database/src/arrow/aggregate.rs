//! Bucketed reductions over captured rows, with the InfluxDB driver's rules.
//!
//! Buckets are aligned to the Unix epoch. Output rows hold the bucket start in
//! the resource's timestamp column, the group tags and each reduction alias,
//! ordered by time (descending when asked) then tags ascending. Empty buckets
//! are not produced. Count and Mean are Int and Float; Sum, Min and Max keep
//! the column's kind, with an integer sum outside i64/u64 reported as Float.

use crate::store::check_name;
use crate::store::window::{Aggregate, Reduction};
use crate::store::{Error, Kind, Record, Resource, Result, Role, Value};
use std::collections::{BTreeSet, HashMap};

/// Rejects the same requests the InfluxDB driver rejects, before any I/O.
pub(super) fn validate(resource: &Resource, options: &Aggregate) -> Result<()> {
    if options.every_seconds == 0 || options.reduce.is_empty() {
        return Err(Error::Invalid(
            "aggregation needs a positive bucket width and reductions".into(),
        ));
    }
    let mut names = BTreeSet::from([resource.time_column()?.to_owned()]);
    for name in &options.group_by {
        if resource.column(name)?.role != Role::Tag || !names.insert(name.clone()) {
            return Err(Error::Invalid(format!(
                "group must be a distinct tag: {name}"
            )));
        }
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
    }
    Ok(())
}

/// Running totals for one reduction in one bucket. Integers accumulate in
/// i128, which holds any sum of u64 or i64 values this driver can be given.
#[derive(Clone, Default)]
struct Acc {
    count: i64,
    int_sum: i128,
    float_sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    int_min: Option<i128>,
    int_max: Option<i128>,
}

impl Acc {
    fn add(&mut self, kind: Kind, value: &Value) {
        let as_int = match kind {
            Kind::Int => value.as_i64().map(i128::from),
            Kind::Uint => value.as_u64().map(i128::from),
            _ => None,
        };
        let Some(float) = value.as_f64() else { return };
        self.count += 1;
        self.float_sum += float;
        self.min = Some(self.min.map_or(float, |m| m.min(float)));
        self.max = Some(self.max.map_or(float, |m| m.max(float)));
        if let Some(int) = as_int {
            self.int_sum += int;
            self.int_min = Some(self.int_min.map_or(int, |m| m.min(int)));
            self.int_max = Some(self.int_max.map_or(int, |m| m.max(int)));
        }
    }

    fn finish(&self, kind: Kind, function: Reduction) -> Value {
        let int = |v: i128| match kind {
            Kind::Uint => u64::try_from(v).map_or(Value::from(v as f64), Value::from),
            _ => i64::try_from(v).map_or(Value::from(v as f64), Value::from),
        };
        let float = |v: Option<f64>| v.map_or(Value::Null, Value::from);
        match (function, kind) {
            (Reduction::Count, _) => self.count.into(),
            (Reduction::Mean, _) => float(Some(self.float_sum / self.count as f64)),
            (Reduction::Sum, Kind::Float) => self.float_sum.into(),
            (Reduction::Min, Kind::Float) => float(self.min),
            (Reduction::Max, Kind::Float) => float(self.max),
            (Reduction::Sum, _) => int(self.int_sum),
            (Reduction::Min, _) => self.int_min.map_or(Value::Null, int),
            (Reduction::Max, _) => self.int_max.map_or(Value::Null, int),
        }
    }
}

/// Running reduction over rows already restricted to the window and
/// predicates. Memory grows with the number of buckets, not rows.
pub(super) struct Reducer {
    options: Aggregate,
    time: String,
    width: i64,
    kinds: Vec<Kind>,
    groups: HashMap<(i64, Vec<String>), Vec<Acc>>,
}

impl Reducer {
    pub(super) fn new(resource: &Resource, options: &Aggregate) -> Result<Self> {
        let kinds = options
            .reduce
            .iter()
            .map(|r| resource.column(&r.column).map(|c| c.kind))
            .collect::<Result<_>>()?;
        Ok(Self {
            options: options.clone(),
            time: resource.time_column()?.to_owned(),
            width: i64::from(options.every_seconds) * 1_000_000_000,
            kinds,
            groups: HashMap::new(),
        })
    }

    pub(super) fn add(&mut self, row: &Record) {
        let Some(at) = row.get(&self.time).and_then(Value::as_i64) else {
            return;
        };
        let tags = self
            .options
            .group_by
            .iter()
            .map(|t| {
                row.get(t)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect();
        let accs = self
            .groups
            .entry((at.div_euclid(self.width) * self.width, tags))
            .or_insert_with(|| vec![Acc::default(); self.kinds.len()]);
        for ((acc, reduction), &kind) in accs.iter_mut().zip(&self.options.reduce).zip(&self.kinds)
        {
            if let Some(value) = row.get(&reduction.column) {
                acc.add(kind, value);
            }
        }
    }

    pub(super) fn finish(self) -> Vec<Record> {
        let Self {
            options,
            time,
            kinds,
            groups,
            ..
        } = self;
        let mut keyed: Vec<_> = groups.into_iter().collect();
        keyed.sort_by(
            |((ta, ga), _), ((tb, gb), _)| match options.range.descending {
                true => tb.cmp(ta).then_with(|| ga.cmp(gb)),
                false => ta.cmp(tb).then_with(|| ga.cmp(gb)),
            },
        );
        keyed.truncate(options.range.limit);
        keyed
            .into_iter()
            .map(|((start, tags), accs)| {
                let mut out = Record::from([(time.clone(), Value::from(start))]);
                let tags = tags.into_iter().map(Value::from);
                out.extend(options.group_by.iter().cloned().zip(tags));
                for ((acc, reduction), &kind) in accs.iter().zip(&options.reduce).zip(&kinds) {
                    out.insert(
                        reduction.alias.clone(),
                        acc.finish(kind, reduction.function),
                    );
                }
                out
            })
            .collect()
    }
}
