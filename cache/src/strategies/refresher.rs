// Refresher is introduced before Provider lifecycle wiring consumes it.
#![cfg_attr(not(test), allow(dead_code))]

//! Bounded, drainable execution for stale background refreshes.
//!
//! Capacity is deliberately not a queue. When every slot is occupied, new work
//! is declined because the caller already has a still-servable stale value and a
//! later reader can try again. Shutdown stops admission and waits for admitted
//! work before backend resources are released.

use std::{future::Future, sync::Arc, time::Duration};

use tokio::sync::{Mutex, Notify, Semaphore};

use crate::{CacheError, Result};

struct State {
    closed: bool,
    running: usize,
}

/// Runs background work under a fixed non-queuing budget.
pub(crate) struct Refresher {
    state: Arc<Mutex<State>>,
    idle: Arc<Notify>,
    slots: Arc<Semaphore>,
    timeout: Duration,
}

impl Refresher {
    pub(crate) fn new(limit: usize, timeout: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                closed: false,
                running: 0,
            })),
            idle: Arc::new(Notify::new()),
            slots: Arc::new(Semaphore::new(limit.max(1))),
            timeout,
        }
    }

    /// Attempts to admit one background task without waiting for capacity.
    ///
    /// Returning `false` is expected when closed, full, or briefly contended;
    /// stale data remains valid and a later reader may retry. The state lock is
    /// acquired with `try_lock`, so serving a stale reader never waits here.
    pub(crate) fn go<W, F>(&self, work: W) -> bool
    where
        W: FnOnce() -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        if state.closed {
            return false;
        }

        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return false,
        };
        state.running += 1;
        drop(state);

        let state = self.state.clone();
        let idle = self.idle.clone();
        let timeout = self.timeout;
        tokio::spawn(async move {
            // Background refresh is best-effort. Timeout cancels this waiter;
            // work detached again by Flight retains Flight's independent bound.
            let _ = tokio::time::timeout(timeout, work()).await;

            let mut state = state.lock().await;
            state.running = state.running.saturating_sub(1);
            let is_idle = state.running == 0;
            drop(state);
            drop(permit);
            if is_idle {
                idle.notify_waiters();
            }
        });
        true
    }

    /// Stops admission and waits for admitted tasks up to `limit`.
    pub(crate) async fn drain(&self, limit: Duration) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            state.closed = true;
            if state.running == 0 {
                return Ok(());
            }
        }

        tokio::time::timeout(limit, async {
            loop {
                // Register before checking state so completion between the check
                // and await cannot become a missed notification.
                let notified = self.idle.notified();
                if self.state.lock().await.running == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            CacheError::Internal("cache background refreshes did not finish in time".to_owned())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[tokio::test]
    async fn test_refresher_full_budget_declines_without_queueing() {
        let refresher = Refresher::new(1, Duration::from_secs(1));
        let release = Arc::new(Notify::new());
        let first_release = release.clone();

        assert!(refresher.go(move || async move {
            first_release.notified().await;
        }));
        assert!(!refresher.go(|| async {}));

        release.notify_one();
        refresher.drain(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn test_refresher_drain_waits_for_running_work() {
        let refresher = Refresher::new(1, Duration::from_secs(1));
        let finished = Arc::new(AtomicBool::new(false));
        let work_finished = finished.clone();

        assert!(refresher.go(move || async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            work_finished.store(true, Ordering::SeqCst);
        }));

        refresher.drain(Duration::from_secs(1)).await.unwrap();
        assert!(finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_refresher_drain_stops_future_admission() {
        let refresher = Refresher::new(1, Duration::from_secs(1));

        refresher.drain(Duration::from_secs(1)).await.unwrap();

        assert!(!refresher.go(|| async {}));
    }

    #[tokio::test]
    async fn test_refresher_drain_timeout_returns_error() {
        let refresher = Refresher::new(1, Duration::from_secs(1));
        assert!(refresher.go(|| async {
            std::future::pending::<()>().await;
        }));

        let result = refresher.drain(Duration::from_millis(10)).await;

        assert!(matches!(result, Err(CacheError::Internal(_))));
    }
}
