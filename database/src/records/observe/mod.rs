//! Observation of a records [`DB`]: every call one operation, as `store::instrument` does
//! for time series and Go's `store.Instrument` does for records.
//!
//! Each call is an operation of component `database` on the backend, a client span named
//! `database.<operation>` (create, get, get_for_update, update, delete, list, count,
//! exists, create_many, get_many, transaction, ensure_schema, drop_schema, has_schema,
//! watch) with the shared `runtime.operation.*` metrics and `runtime.records.*` counts. A
//! missing record is `not_found` and a taken key or a lost race `conflict`, neither of
//! which fails the span. Keys, filters, messages and raw errors are never recorded.
mod calls;

use std::sync::Arc;

use telemetry::{Observer, Telemetry};

use super::{Capabilities, DB};

/// One observed capability: the backend's own, and the operations reported over it.
pub(crate) struct Observed<T: ?Sized> {
    pub(crate) next: Arc<T>,
    pub(crate) observer: Observer,
    pub(crate) backend: &'static str,
}

/// `db` with every capability observed through `telemetry`, or as the backend built it
/// when `telemetry` has no signals. Observing an observed DB replaces its signals rather
/// than adding a second layer: it starts again from the backend's own capabilities.
pub fn instrument(db: DB, telemetry: Telemetry) -> DB {
    let raw = db.raw.clone().unwrap_or_else(|| Arc::new(db.clone()));
    if !telemetry.is_enabled() {
        return (*raw).clone();
    }
    let observer = telemetry.observer("database");
    let backend = raw.backend;
    macro_rules! wrap {
        ($next:expr) => {
            Arc::new(Observed {
                next: $next,
                observer: observer.clone(),
                backend,
            })
        };
    }
    let caps = Capabilities {
        driver: wrap!(raw.driver().clone()),
        tx: Some(wrap!(raw.tx.clone())),
        schema: Some(wrap!(raw.schema.clone())),
        batch: raw.has_batch().then(|| wrap!(raw.batch.clone()) as _),
        watch: Some(wrap!(raw.watch.clone())),
    };
    let mut observed = DB::build(backend, raw.name.clone(), caps, raw.release.clone());
    observed.raw = Some(raw);
    observed
}

/// [`instrument`] with the process's default signals ([`Telemetry::process`]): what every
/// records provider's `set_database` returns, so a database selected after the application
/// set its signals is observed without being handed any.
pub fn instrument_default(db: DB) -> DB {
    instrument(db, Telemetry::process())
}
