//! One operation guard around each awaited records call.
use futures::stream::BoxStream;
use prost_reflect::DynamicMessage;
use std::future::Future;
use telemetry::{FutureExt, SpanKind};

use super::Observed;
use crate::Result;
use crate::observe::outcome;
use crate::records::{
    Batcher, Change, Driver, ListOptions, ListResult, Migrator, Resource, Transactional, TxWork,
    WatchOptions, Watcher, WriteResult,
};

impl<T: ?Sized> Observed<T> {
    /// Runs `future` as the operation `name`, counting what it read and wrote.
    async fn run<R>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<R>>,
        written: usize,
        read: fn(&R) -> usize,
    ) -> Result<R> {
        let mut operation = self.observer.start(self.backend, name, SpanKind::Client);
        let result = future.with_context(operation.context()).await;
        if let Ok(value) = &result {
            operation.read_rows(read(value));
            operation.written_rows(written);
        }
        operation.finish(outcome(result.as_ref().err()));
        result
    }
}

#[async_trait::async_trait]
impl Driver for Observed<dyn Driver> {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.run("create", self.next.create(res, msg), 1, |_| 0)
            .await
    }
    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.run("get", self.next.get(res, key), 0, |_| 1).await
    }
    async fn get_for_update(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.run(
            "get_for_update",
            self.next.get_for_update(res, key),
            0,
            |_| 1,
        )
        .await
    }
    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.run("update", self.next.update(res, msg), 1, |_| 0)
            .await
    }
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.run("delete", self.next.delete(res, key), 1, |_| 0)
            .await
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.run("list", self.next.list(res, opts), 0, |r| r.items.len())
            .await
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.run("count", self.next.count(res, opts), 0, |_| 0)
            .await
    }
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.run("exists", self.next.exists(res, key), 0, |_| 0)
            .await
    }
}

#[async_trait::async_trait]
impl Batcher for Observed<dyn Batcher> {
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        let n = msgs.len();
        self.run("create_many", self.next.create_many(res, msgs), n, |_| 0)
            .await
    }
    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        self.run("get_many", self.next.get_many(res, keys), 0, |r| {
            r.iter().flatten().count()
        })
        .await
    }
}

#[async_trait::async_trait]
impl Transactional for Observed<dyn Transactional> {
    async fn run(&self, work: TxWork) -> Result<()> {
        // The transaction is one operation; the calls inside it run on the
        // transaction's own DB, which the backend hands `work`.
        Observed::run(self, "transaction", self.next.run(work), 0, |_| 0).await
    }
}

#[async_trait::async_trait]
impl Migrator for Observed<dyn Migrator> {
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        self.run("ensure_schema", self.next.ensure_schema(res), 0, |_| 0)
            .await
    }
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        self.run("drop_schema", self.next.drop_schema(res), 0, |_| 0)
            .await
    }
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        self.run("has_schema", self.next.has_schema(res), 0, |_| 0)
            .await
    }
}

#[async_trait::async_trait]
impl Watcher for Observed<dyn Watcher> {
    async fn watch(
        &self,
        res: &Resource,
        opts: WatchOptions,
    ) -> Result<BoxStream<'static, Result<Change>>> {
        self.run("watch", self.next.watch(res, opts), 0, |_| 0)
            .await
    }
}
