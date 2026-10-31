//! Unified database observations preserve capabilities and errors.
mod calls;
mod classify;
mod driver;
mod reader;
mod series;
mod stream;

use crate::store::{Capabilities, DB};
pub(crate) use calls::run;
pub(crate) use classify::outcome;
use std::sync::Arc;
use telemetry::{Observer, Telemetry};

pub(super) struct Observed<T: ?Sized> {
    next: Arc<T>,
    observer: Observer,
    backend: &'static str,
}

/// Instruments every capability the backend has, as runtime-go's
/// `store.Instrument`. Refusals stay unwrapped. Rewiring starts again from the
/// backend's own capabilities, so instrumenting twice never nests wrappers.
/// No names, keys, SQL, filters or raw error strings are recorded.
pub fn instrument(db: DB, telemetry: Telemetry) -> DB {
    let raw = db.raw.clone();
    if !telemetry.is_enabled() {
        let mut plain = DB::build(db.backend, db.name, raw);
        plain.telemetry = telemetry;
        return plain;
    }
    let observer = telemetry.observer("database");
    let backend = db.backend;
    macro_rules! wrap {
        ($field:ident) => {
            raw.$field.as_ref().map(|next| {
                Arc::new(Observed {
                    next: next.clone(),
                    observer: observer.clone(),
                    backend,
                }) as _
            })
        };
    }
    let wrapped = Capabilities {
        driver: wrap!(driver),
        batch: wrap!(batch),
        series: wrap!(series),
        schema: wrap!(schema),
        streams: wrap!(streams),
        tx: raw.tx.clone(),
        release: raw.release.clone(),
    };
    let mut observed = DB::build(backend, db.name, wrapped);
    observed.raw = raw;
    observed.telemetry = telemetry;
    observed
}
