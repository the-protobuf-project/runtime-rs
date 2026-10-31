//! The backend-agnostic contract, shaped after runtime-go's `database/store`.
//!
//! A [`Resource`] describes columns once; generated code adds a [`Model`] per
//! message. A [`Provider`] selects a [`DB`], which is the CRUD [`Driver`] plus
//! [`Transactional`], [`Migrator`], [`Batcher`], [`TimeSeries`] and
//! [`SeriesReader`], each always present and refusing by name where the
//! backend lacks it. [`For`] binds a model to a DB.

mod capabilities;
mod driver;
mod errors;
pub(crate) mod filter;
mod provider;
mod record;
mod reduce;
mod refuses;
mod registry;
mod resource;
mod stream;
mod timeseries;
mod typed;
mod typed_stream;
pub(crate) mod window;

pub use crate::observe::instrument;
pub use capabilities::{Batcher, Migrator, Release, Transactional, TxWork};
pub(crate) use driver::page_size;
pub use driver::{DEFAULT_PAGE_SIZE, Driver, ListOptions, ListResult, WriteResult};
pub use errors::{Error, Result};
pub use provider::{Capabilities, DB, Provider};
pub(crate) use record::validate_value;
pub use record::{Record, Value};
pub use registry::Registry;
pub(crate) use resource::check_name;
pub use resource::{Column, Kind, Resource, Role};
pub use stream::{RecordStream, Scan, SeriesReader};
pub use timeseries::{
    AggregateOptions, Bucket, DEFAULT_BUCKET_LIMIT, HypertableOptions, RangeOptions, Reducer,
    Reduction, TimeSeries,
};
pub use typed::{For, Model};
pub use typed_stream::TypedStream;
