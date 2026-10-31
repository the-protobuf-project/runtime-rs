//! Queue ownership balances depth even when a failed worker drops pending work.
use crate::Record;
use telemetry::{Context, Observer};

#[derive(Clone)]
pub(super) struct Wiring {
    pub observer: Observer,
    pub backend: &'static str,
}

struct Lease(Wiring);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.observer.queue_delta(self.0.backend, -1);
    }
}

pub(super) struct Entry {
    record: Record,
    context: Context,
    _lease: Lease,
}
impl Entry {
    pub fn new(record: Record, context: Context, wiring: Wiring) -> Self {
        wiring.observer.queue_delta(wiring.backend, 1);
        Self {
            record,
            context,
            _lease: Lease(wiring),
        }
    }
    pub fn dequeue(self) -> (Record, Context) {
        let Self {
            record, context, ..
        } = self;
        (record, context)
    }
}
