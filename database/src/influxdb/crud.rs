//! Points through the CRUD and bulk contracts. Points have no key, so keyed
//! reads and writes are refused by name.

use super::{InfluxDriver, sql};
use crate::store::window::Range;
use crate::store::{
    Batcher, Driver, Error, ListOptions, ListResult, Record, Resource, Result, WriteResult,
    page_size,
};

/// The widest window InfluxDB can plan: its timestamps are nanoseconds that
/// must stay inside 1677-09-21T00:12:44Z..2262-04-11T23:47:16Z, which
/// i64::MIN and i64::MAX overflow.
const ALL_TIME: (i64, i64) = (-9_223_372_035_000_000_000, 9_223_372_035_000_000_000);

/// Every point ever written: list and count have no time bounds of their own.
fn all_time(resource: &Resource, opts: &ListOptions) -> Result<Range> {
    let time = resource.time_column()?;
    let descending = match opts.order_by.split_whitespace().collect::<Vec<_>>()[..] {
        [] => false,
        [column] | [column, "asc"] if column == time => false,
        [column, "desc"] if column == time => true,
        _ => return Err(Error::Invalid(format!("InfluxDB orders only by {time}"))),
    };
    let range = Range {
        start: ALL_TIME.0,
        end: ALL_TIME.1,
        limit: page_size(opts.page_size)?,
        descending,
        predicates: crate::store::filter::predicates(resource, &opts.filter)?,
    };
    range.validate()?;
    Ok(range)
}

#[async_trait::async_trait]
impl Driver for InfluxDriver {
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        self.write(res, std::slice::from_ref(record)).await?;
        Ok(WriteResult::done(record.clone()))
    }
    async fn get(&self, _: &Resource, _: &str) -> Result<Record> {
        self.refuse("get points by key; points have no key")
    }
    async fn update(&self, _: &Resource, _: &Record) -> Result<WriteResult> {
        self.refuse("update points by key; rewrite the point to merge fields")
    }
    async fn delete(&self, _: &Resource, _: &str) -> Result<()> {
        self.refuse("delete points by key; drop the table instead")
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let range = all_time(res, opts)?;
        let (items, next_page_token) = self.page(res, &range, &opts.page_token).await?;
        let total = match opts.omit_total {
            true => -1,
            false => self.count(res, opts).await?,
        };
        Ok(ListResult {
            items,
            next_page_token,
            total,
        })
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.validate_resource(res)?;
        let rows = self.query(sql::count(res, &all_time(res, opts)?)?).await?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(|n| n.as_i64())
            .unwrap_or(0))
    }
    async fn exists(&self, _: &Resource, _: &str) -> Result<bool> {
        self.refuse("check points by key; points have no key")
    }
}

#[async_trait::async_trait]
impl Batcher for InfluxDriver {
    async fn create_many(&self, res: &Resource, records: &[Record]) -> Result<Vec<WriteResult>> {
        self.write(res, records).await?;
        Ok(records.iter().cloned().map(WriteResult::done).collect())
    }
    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Record>> {
        self.refuse("get points by key; points have no key")
    }
}
