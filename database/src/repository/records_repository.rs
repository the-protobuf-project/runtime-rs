//! [`Repository`] over any [`records`](crate::records) provider.
//!
//! The contract is fixed here so generators can target it; the method bodies are the
//! implementation still to come and refuse until then.

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use prost_reflect::ReflectMessage;

use super::{ListInput, Page, Repository, Rules};
use crate::records::{DB, Resource};
use crate::{Error, Result};

/// A [`Repository`] of `M` over a records [`DB`], whichever provider is behind it.
pub struct RecordsRepository<M> {
    db: DB,
    resource: Arc<Resource>,
    rules: Rules,
    _m: PhantomData<fn() -> M>,
}

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// `M`'s repository over `db`, stored as `resource` describes and behaving as `rules`
    /// says. Fails when `resource` does not describe `M` or `rules` names a field `M` lacks.
    pub fn new(db: &DB, resource: Arc<Resource>, rules: Rules) -> Result<Self> {
        let want = M::default().descriptor();
        if want.full_name() != resource.message.full_name() {
            return Err(Error::Invalid(format!(
                "resource {:?} describes {}, not {}",
                resource.name,
                resource.message.full_name(),
                want.full_name()
            )));
        }
        let fields = [&rules.etag, &rules.delete_time, &rules.expire_time];
        for field in fields.into_iter().flatten().chain(&rules.output_only) {
            if want.get_field_by_name(field).is_none() {
                return Err(Error::Invalid(format!(
                    "{} has no field {field:?}",
                    want.full_name()
                )));
            }
        }
        Ok(Self {
            db: db.clone(),
            resource,
            rules,
            _m: PhantomData,
        })
    }

    /// The database, descriptor and rules, for what the trait does not cover.
    pub fn parts(&self) -> (&DB, &Resource, &Rules) {
        (&self.db, &self.resource, &self.rules)
    }
}

fn pending(capability: &'static str) -> Error {
    Error::Unimplemented {
        backend: "repository",
        capability,
    }
}

#[async_trait::async_trait]
impl<M> Repository<M> for RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    async fn create(&self, _: &M) -> Result<M> {
        Err(pending("create"))
    }

    async fn get(&self, _: &str) -> Result<M> {
        Err(pending("get"))
    }

    async fn list(&self, _: &ListInput) -> Result<Page<M>> {
        Err(pending("list"))
    }

    async fn count(&self, _: &ListInput) -> Result<i64> {
        Err(pending("count"))
    }

    async fn update(&self, _: &M, _: &[String]) -> Result<M> {
        Err(pending("update"))
    }

    async fn update_output_only(&self, _: &M, _: &[String]) -> Result<M> {
        Err(pending("update_output_only"))
    }

    async fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
        Err(pending("delete"))
    }

    async fn undelete(&self, _: &str, _: Option<&str>) -> Result<M> {
        Err(pending("undelete"))
    }

    async fn purge(&self, _: &str) -> Result<()> {
        Err(pending("purge"))
    }
}
