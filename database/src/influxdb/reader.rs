//! SDK-backed streaming and keyset paging without OFFSET scans.
use super::{InfluxDriver, codec, cursor, errors::sdk, sql};
use crate::store::filter::predicates;
use crate::store::window::Range;
use crate::store::{Error, Record, RecordStream, Resource, Result, Scan, SeriesReader};
use futures::{TryStreamExt, stream};
use influxdb3_client::{BatchStream, QueryResult};
use std::collections::VecDeque;
use tokio::time::{Instant, timeout_at};

struct State {
    batches: BatchStream,
    pending: VecDeque<Record>,
    deadline: Instant,
    remaining: usize,
}

async fn next(mut state: State) -> Result<Option<(Record, State)>> {
    if state.remaining == 0 {
        return Ok(None);
    }
    if Instant::now() >= state.deadline {
        return Err(Error::Timeout);
    }
    loop {
        if let Some(row) = state.pending.pop_front() {
            state.remaining -= 1;
            return Ok(Some((row, state)));
        }
        let batch = timeout_at(state.deadline, state.batches.try_next())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(sdk)?;
        let Some(batch) = batch else {
            return Ok(None);
        };
        let result = QueryResult::new(batch.schema(), vec![batch]);
        state.pending = result
            .rows()
            .map_err(sdk)?
            .into_iter()
            .map(codec::row)
            .collect::<Result<_>>()?;
    }
}

#[async_trait::async_trait]
impl SeriesReader for InfluxDriver {
    async fn stream(&self, resource: &Resource, range: &Scan) -> Result<RecordStream> {
        self.validate_resource(resource)?;
        range.validate()?;
        let bounded = Range {
            start: range.start,
            end: range.end,
            descending: range.descending,
            predicates: predicates(resource, &range.filter)?,
            limit: 1,
        };
        let mut query = sql::range(resource, &bounded)?;
        query
            .sql
            .truncate(query.sql.rfind(" LIMIT ").expect("range SQL has LIMIT"));
        if let Some(limit) = range.limit {
            query.sql.push_str(&format!(" LIMIT {limit}"));
        }
        let deadline = Instant::now() + self.timeout;
        let request = self.client.sql(query.sql).params(query.params);
        let batches = timeout_at(deadline, request.stream())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(sdk)?;
        let state = State {
            batches,
            pending: VecDeque::new(),
            deadline,
            remaining: range.limit.unwrap_or(usize::MAX),
        };
        Ok(Box::pin(stream::try_unfold(state, next)))
    }
}

impl InfluxDriver {
    /// One keyset page continuing after `token`; the next token is empty at
    /// the end. Tokens are scoped to this database, descriptor and query.
    pub(super) async fn page(
        &self,
        resource: &Resource,
        range: &Range,
        token: &str,
    ) -> Result<(Vec<Record>, String)> {
        self.validate_resource(resource)?;
        let mut query = sql::range(resource, range)?;
        let config = self.client.config();
        let scope = cursor::scope(config.host_url(), &config.database, resource, &query);
        if !token.is_empty() {
            cursor::apply(&mut query, resource, range, &scope, token)?;
        }
        let limit = query.sql.rfind(" LIMIT ").expect("range SQL has LIMIT");
        query.sql.truncate(limit);
        query.sql.push_str(&format!(" LIMIT {}", range.limit + 1));
        let mut items = self.query(query).await?;
        let next = if items.len() > range.limit {
            items.truncate(range.limit);
            let last = items.last().expect("positive page limit");
            cursor::encode(resource, last, scope)?
        } else {
            String::new()
        };
        Ok((items, next))
    }
}
