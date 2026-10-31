//! Unified database observations preserve optional capabilities and errors.
mod calls;
mod classify;
mod reader;
mod records;
mod series;
mod stream;

use crate::{Capabilities, DB};
pub(crate) use calls::run;
pub(crate) use classify::outcome;
use std::sync::Arc;
use telemetry::{Observer, Telemetry};

pub(super) struct Observed<T: ?Sized> {
    next: Arc<T>,
    observer: Observer,
    backend: &'static str,
}

/// Instruments every capability while preserving its availability and ownership.
/// Rewiring replaces earlier instrumentation rather than nesting wrappers.
/// No names, keys, SQL, predicates or raw error strings are recorded.
pub fn instrument(mut db: DB, telemetry: Telemetry) -> DB {
    let raw = db
        .original
        .take()
        .unwrap_or_else(|| db.capabilities.clone());
    db.telemetry = telemetry.clone();
    if !telemetry.is_enabled() {
        db.capabilities = raw;
        return db;
    }
    let observer = telemetry.observer("database");
    let backend = db.backend();
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
    db.capabilities = Capabilities {
        records: wrap!(records),
        series: wrap!(series),
        reader: wrap!(reader),
        maintenance: wrap!(maintenance),
        row_deletion: wrap!(row_deletion),
    };
    db.original = Some(raw);
    db
}
