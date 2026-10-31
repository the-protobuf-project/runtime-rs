//! Typed views bound to one resource and one model, as runtime-go's `For[T]`.

use super::{DB, Error, ListOptions, RangeOptions, Record, Resource, Result, WriteResult};
use std::collections::BTreeSet;

/// A model with a generated descriptor and conversions, emitted per message:
///
/// ```text
/// impl Model for Reading {
///     fn resource() -> &'static Resource { &READING }
///     fn to_record(&self) -> Result<Record> { … }
///     fn from_record(record: Record) -> Result<Self> { … }
/// }
/// ```
pub trait Model: Sized + Send + Sync + 'static {
    /// The descriptor generated for this message.
    fn resource() -> &'static Resource;
    /// Column values, keyed by column name.
    fn to_record(&self) -> Result<Record>;
    /// Rebuilds the model from stored columns.
    fn from_record(record: Record) -> Result<Self>;
}

/// A view over a [`DB`] bound to one resource, reading and writing `T`.
/// Cheap to make: the DB underneath is shared.
pub struct For<T> {
    pub(super) db: DB,
    pub(super) res: Resource,
    encode: fn(&T) -> Result<Record>,
    pub(super) decode: fn(Record) -> Result<T>,
}

impl<T> Clone for For<T> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            res: self.res.clone(),
            encode: self.encode,
            decode: self.decode,
        }
    }
}

impl<T: Model> For<T> {
    /// Binds `T` and its generated descriptor to `db`.
    pub fn new(db: &DB) -> Self {
        Self::with(db, T::resource().clone(), T::to_record, T::from_record)
    }
}

impl<T> For<T> {
    /// Binds a resource and explicit conversions; [`For::new`] and
    /// [`collection`](crate::collection) are the usual constructors.
    pub fn with(
        db: &DB,
        res: Resource,
        encode: fn(&T) -> Result<Record>,
        decode: fn(Record) -> Result<T>,
    ) -> Self {
        Self {
            db: db.clone(),
            res,
            encode,
            decode,
        }
    }

    /// The database and descriptor, for operations the view does not cover.
    pub fn unwrap(&self) -> (&DB, &Resource) {
        (&self.db, &self.res)
    }

    /// The bound descriptor.
    pub fn resource(&self) -> &Resource {
        &self.res
    }

    /// The bound database.
    pub fn db(&self) -> &DB {
        &self.db
    }

    pub(crate) fn encode(&self, value: &T) -> Result<Record> {
        let record = (self.encode)(value)?;
        self.res.validate(&record)?;
        Ok(record)
    }

    fn decode_all(&self, records: Vec<Record>) -> Result<Vec<T>> {
        records.into_iter().map(self.decode).collect()
    }

    /// Stores `value` as a new record.
    pub async fn create(&self, value: &T) -> Result<WriteResult> {
        self.db.create(&self.res, &self.encode(value)?).await
    }

    /// Stores every value in one round trip.
    pub async fn create_many(&self, values: &[T]) -> Result<Vec<WriteResult>> {
        let records = values
            .iter()
            .map(|v| self.encode(v))
            .collect::<Result<Vec<_>>>()?;
        self.db.batch.create_many(&self.res, &records).await
    }

    /// The record under `key`, already a `T`.
    pub async fn get(&self, key: &str) -> Result<T> {
        (self.decode)(self.db.get(&self.res, key).await?)
    }

    /// Replaces the record with the same key.
    pub async fn update(&self, value: &T) -> Result<WriteResult> {
        self.db.update(&self.res, &self.encode(value)?).await
    }

    /// Removes the record under `key`.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.db.delete(&self.res, key).await
    }

    /// Whether a record is stored under `key`.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        self.db.exists(&self.res, key).await
    }

    /// How many records match `opts.filter`.
    pub async fn count(&self, opts: &ListOptions) -> Result<i64> {
        self.db.count(&self.res, opts).await
    }

    /// One page as `T`, with the continuation token and total.
    pub async fn list(&self, opts: &ListOptions) -> Result<(Vec<T>, String, i64)> {
        let page = self.db.list(&self.res, opts).await?;
        Ok((
            self.decode_all(page.items)?,
            page.next_page_token,
            page.total,
        ))
    }

    /// Every matching record, following page tokens. Refuses a repeated token
    /// rather than looping. Costs memory for every record.
    pub async fn all(&self, opts: &ListOptions) -> Result<Vec<T>> {
        let (mut opts, mut out, mut seen) = (opts.clone(), Vec::new(), BTreeSet::new());
        loop {
            let (page, next, _) = self.list(&opts).await?;
            out.extend(page);
            if next.is_empty() {
                return Ok(out);
            }
            if !seen.insert(next.clone()) {
                return Err(Error::Invalid(format!(
                    "{} repeated page token; refusing to loop",
                    self.res.name()
                )));
            }
            opts.page_token = next;
        }
    }

    /// One page of a time window as `T`, with the continuation token.
    pub async fn range(&self, opts: &RangeOptions) -> Result<(Vec<T>, String)> {
        let page = self.db.series.range(&self.res, opts).await?;
        Ok((self.decode_all(page.items)?, page.next_page_token))
    }
}
