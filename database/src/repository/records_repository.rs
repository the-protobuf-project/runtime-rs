//! [`Repository`] over any [`records`](crate::records) provider: the trait's methods, each
//! delegating to the module that implements it (`read`, `write`, `soft`).

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use super::names::Names;
use super::shape::Shape;
use super::{Hooks, ListInput, Page, Repository, Rules};
use crate::records::{DB, Resource};
use crate::{Error, Result};

/// A [`Repository`] of `M` over a records [`DB`], whichever provider is behind it.
pub struct RecordsRepository<M> {
    pub(super) db: DB,
    pub(super) inner: Arc<Inner<M>>,
    _m: PhantomData<fn() -> M>,
}

/// What a write's transaction needs, shared with it.
pub(super) struct Inner<M> {
    pub(super) res: Arc<Resource>,
    pub(super) rules: Rules,
    pub(super) shape: Shape,
    pub(super) names: Names,
    pub(super) hooks: Hooks<M>,
}

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// `M`'s repository over `db`, stored as `resource` describes and behaving as `rules`
    /// says. Fails when `resource` does not describe `M`, when `rules` names a field `M`
    /// lacks or the resource does not store, or when the etag is not a string or a
    /// soft-delete time not a `google.protobuf.Timestamp`.
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
        for field in &rules.output_only {
            if want.get_field_by_name(field).is_none() {
                return Err(Error::Invalid(format!(
                    "{} has no field {field:?}",
                    want.full_name()
                )));
            }
        }
        let shape = Shape::new(&resource, &rules)?;
        let names = Names::new(&resource, &rules)?;
        Ok(Self {
            db: db.clone(),
            inner: Arc::new(Inner {
                res: resource,
                rules,
                shape,
                names,
                hooks: Hooks::default(),
            }),
            _m: PhantomData,
        })
    }

    /// This repository running `hooks` around its calls, as Go's `With<Model>Hooks`.
    pub fn with_hooks(self, hooks: Hooks<M>) -> Self {
        let inner = &self.inner;
        Self {
            inner: Arc::new(Inner {
                res: inner.res.clone(),
                rules: inner.rules.clone(),
                shape: inner.shape.clone(),
                names: inner.names.clone(),
                hooks,
            }),
            ..self
        }
    }

    /// The database, descriptor and rules, for what the trait does not cover.
    pub fn parts(&self) -> (&DB, &Resource, &Rules) {
        (&self.db, &self.inner.res, &self.inner.rules)
    }
}

impl<M> Inner<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// `msg` as `M`.
    pub(super) fn decode(&self, msg: &DynamicMessage) -> Result<M> {
        msg.transcode_to::<M>()
            .map_err(|e| Error::Invalid(format!("decoding a {}: {e}", self.res.name)))
    }

    /// A stored record as the API reads it: references back under their collections.
    pub(super) fn as_read(&self, msg: DynamicMessage) -> DynamicMessage {
        let mut msg = msg;
        self.names.as_read(&mut msg);
        msg
    }

    /// `msg` as `M`, through the `after_read` hook: what every read returns.
    pub(super) async fn read(&self, msg: &DynamicMessage) -> Result<M> {
        let m = self.decode(&self.as_read(msg.clone()))?;
        match &self.hooks.after_read {
            Some(h) => h(m).await,
            None => Ok(m),
        }
    }

    /// The `before_delete` hook, when there is one.
    pub(super) async fn before_delete(&self, name: &str) -> Result<()> {
        match &self.hooks.before_delete {
            Some(h) => h(name.to_owned()).await,
            None => Ok(()),
        }
    }
}

impl<M> Inner<M> {
    /// Refuses an empty name, or one the pattern does not fit.
    pub(super) fn named<'a>(&self, name: &'a str) -> Result<&'a str> {
        self.names.split(name).map(|_| name)
    }
}

#[async_trait::async_trait]
impl<M> Repository<M> for RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    async fn create(&self, m: &M) -> Result<M> {
        self.create_record(m).await
    }

    async fn get(&self, name: &str) -> Result<M> {
        let msg = self
            .db
            .get(&self.inner.res, self.inner.named(name)?)
            .await?;
        self.inner.read(&msg).await
    }

    async fn list(&self, input: &ListInput) -> Result<Page<M>> {
        self.list_records(input).await
    }

    async fn count(&self, input: &ListInput) -> Result<i64> {
        self.count_records(input).await
    }

    async fn update(&self, m: &M, paths: &[String]) -> Result<M> {
        self.update_record(m, paths, false).await
    }

    async fn update_output_only(&self, m: &M, paths: &[String]) -> Result<M> {
        if paths.is_empty() {
            return Err(Error::Invalid(
                "update_output_only needs the fields to write".into(),
            ));
        }
        self.update_record(m, paths, true).await
    }

    async fn delete(&self, name: &str, if_match: Option<&str>) -> Result<()> {
        self.delete_record(self.inner.named(name)?, if_match.unwrap_or_default())
            .await
    }

    async fn undelete(&self, name: &str, if_match: Option<&str>) -> Result<M> {
        self.undelete_record(self.inner.named(name)?, if_match.unwrap_or_default())
            .await
    }

    async fn purge(&self, name: &str) -> Result<()> {
        let name = self.inner.named(name)?;
        self.inner.before_delete(name).await?;
        self.db.delete(&self.inner.res, name).await
    }
}
