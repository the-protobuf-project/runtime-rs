//! The refusal every capability a backend lacks falls back to, naming the backend. Go's
//! `refuses` in `store/provider.go`.

use futures::stream::BoxStream;
use prost_reflect::DynamicMessage;

use super::{
    Batcher, Change, Migrator, Resource, Transactional, TxWork, WatchOptions, Watcher, WriteResult,
};
use crate::{Error, Result};

/// Every capability, refused, naming the backend.
pub(super) struct Refuses(pub(super) &'static str);

impl Refuses {
    fn no(&self, capability: &'static str) -> Error {
        Error::Unimplemented {
            backend: self.0,
            capability,
        }
    }
}

#[async_trait::async_trait]
impl Transactional for Refuses {
    async fn run(&self, _: TxWork) -> Result<()> {
        Err(self.no("run a transaction"))
    }
}

#[async_trait::async_trait]
impl Migrator for Refuses {
    async fn ensure_schema(&self, _: &Resource) -> Result<()> {
        Err(self.no("create a schema"))
    }

    async fn drop_schema(&self, _: &Resource) -> Result<()> {
        Err(self.no("drop a schema"))
    }

    async fn has_schema(&self, _: &Resource) -> Result<bool> {
        Err(self.no("inspect a schema"))
    }
}

#[async_trait::async_trait]
impl Batcher for Refuses {
    async fn create_many(&self, _: &Resource, _: &[DynamicMessage]) -> Result<Vec<WriteResult>> {
        Err(self.no("write in bulk"))
    }

    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Option<DynamicMessage>>> {
        Err(self.no("read in bulk"))
    }
}

#[async_trait::async_trait]
impl Watcher for Refuses {
    async fn watch(
        &self,
        _: &Resource,
        _: WatchOptions,
    ) -> Result<BoxStream<'static, Result<Change>>> {
        Err(self.no("watch for changes"))
    }
}
