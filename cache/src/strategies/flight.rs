// Flight is introduced one reviewed milestone before AsideImpl consumes it.
// Remove this temporary allowance when the strategy implementation lands.
#![cfg_attr(not(test), allow(dead_code))]

//! In-process single-flight coordination for read-through loads.
//!
//! A cold or expired hot key can attract many readers simultaneously. Without
//! coordination, every reader invokes the same slow source and the cache turns
//! one miss into a load spike. Flight maps an ID to one pending result so all
//! readers in this process share one execution.
//!
//! The map contains only transient coordination state, never cached application
//! data. Values still live exclusively behind [`crate::core::Driver`], keeping
//! the Driver/Strategy boundary intact.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use tokio::sync::{watch, Mutex, Semaphore};

use crate::{CacheError, Result};

type SharedResult = Option<Result<Vec<u8>>>;
type Calls = HashMap<String, watch::Receiver<SharedResult>>;

/// Collapses concurrent work by key and bounds work across distinct keys.
///
/// Joining an existing key never consumes capacity and is never rejected. Only
/// the first caller for a new key acquires a permit. Work runs in a detached
/// Tokio task, so cancellation of the caller that happened to arrive first does
/// not cancel a result awaited by other callers.
pub(crate) struct Flight {
    calls: Arc<Mutex<Calls>>,
    budget: Arc<Semaphore>,
    timeout: Duration,
}

impl Flight {
    /// Creates a flight group with a distinct-load limit and per-load timeout.
    ///
    /// A zero concurrency setting still permits one load. Leaving it at zero
    /// would reject all cold keys and make the cache permanently unusable.
    pub(crate) fn new(concurrency: usize, timeout: Duration) -> Self {
        Self {
            calls: Arc::new(Mutex::new(HashMap::new())),
            budget: Arc::new(Semaphore::new(concurrency.max(1))),
            timeout,
        }
    }

    /// Runs `work` for a new key or joins the result already being produced.
    ///
    /// The closure, rather than a pre-created future, is accepted so a joining
    /// caller does not even construct work that will never run.
    pub(crate) async fn run<W, F>(&self, key: &str, work: W) -> Result<Vec<u8>>
    where
        W: FnOnce() -> F + Send + 'static,
        F: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        let mut receiver = {
            let mut calls = self.calls.lock().await;

            if let Some(receiver) = calls.get(key) {
                receiver.clone()
            } else {
                // Claim capacity before publishing the call. If admission is
                // refused, no orphan map entry exists for another caller to
                // join and wait on forever.
                let permit = self
                    .budget
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| CacheError::Overloaded)?;
                let (sender, receiver) = watch::channel(None);
                calls.insert(key.to_owned(), receiver.clone());

                let calls = self.calls.clone();
                let key = key.to_owned();
                let timeout = self.timeout;
                tokio::spawn(async move {
                    let result = match tokio::time::timeout(timeout, work()).await {
                        Ok(result) => result,
                        Err(_) => Err(CacheError::Internal("cache load timed out".to_owned())),
                    };

                    // Remove before publication, matching the Go implementation:
                    // a caller arriving after work is complete starts a fresh
                    // execution instead of joining a result that already ended.
                    calls.lock().await.remove(&key);
                    let _ = sender.send(Some(result));

                    // The owned permit is returned only after the key can no
                    // longer be joined and all waiters have been notified.
                    drop(permit);
                });

                receiver
            }
        };

        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }

            receiver.changed().await.map_err(|_| {
                CacheError::Internal("cache load ended without publishing a result".to_owned())
            })?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    #[tokio::test]
    async fn test_flight_same_key_collapses_work() {
        let flight = Arc::new(Flight::new(8, Duration::from_secs(1)));
        let executions = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::new();

        for _ in 0..32 {
            let flight = flight.clone();
            let executions = executions.clone();
            callers.push(tokio::spawn(async move {
                flight
                    .run("hot", move || async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(b"shared".to_vec())
                    })
                    .await
                    .unwrap()
            }));
        }

        for caller in callers {
            assert_eq!(caller.await.unwrap(), b"shared");
        }
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_flight_distinct_key_over_budget_is_rejected() {
        let flight = Arc::new(Flight::new(1, Duration::from_secs(1)));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = {
            let flight = flight.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                flight
                    .run("first", move || async move {
                        started.notify_one();
                        release.notified().await;
                        Ok(b"first".to_vec())
                    })
                    .await
            })
        };

        started.notified().await;
        let second = flight
            .run("second", || async { Ok(b"second".to_vec()) })
            .await;
        assert!(matches!(second, Err(CacheError::Overloaded)));

        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap(), b"first");
    }

    #[tokio::test]
    async fn test_flight_existing_key_joins_when_budget_is_full() {
        let flight = Arc::new(Flight::new(1, Duration::from_secs(1)));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = {
            let flight = flight.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                flight
                    .run("hot", move || async move {
                        started.notify_one();
                        release.notified().await;
                        Ok(b"shared".to_vec())
                    })
                    .await
            })
        };

        started.notified().await;
        let joined = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run("hot", || async { Ok(b"must not run".to_vec()) })
                    .await
            })
        };

        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap(), b"shared");
        assert_eq!(joined.await.unwrap().unwrap(), b"shared");
    }

    #[tokio::test]
    async fn test_flight_dropped_first_caller_does_not_cancel_work() {
        let flight = Arc::new(Flight::new(1, Duration::from_secs(1)));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = {
            let flight = flight.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                flight
                    .run("hot", move || async move {
                        started.notify_one();
                        release.notified().await;
                        Ok(b"survived".to_vec())
                    })
                    .await
            })
        };

        started.notified().await;
        first.abort();

        let joined = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run("hot", || async { Ok(b"must not run".to_vec()) })
                    .await
            })
        };

        release.notify_one();
        assert_eq!(joined.await.unwrap().unwrap(), b"survived");
    }

    #[tokio::test]
    async fn test_flight_timeout_publishes_error_and_releases_capacity() {
        let flight = Flight::new(1, Duration::from_millis(10));

        let timed_out = flight
            .run("slow", || async {
                std::future::pending::<()>().await;
                Ok(Vec::new())
            })
            .await;
        assert!(matches!(timed_out, Err(CacheError::Internal(_))));

        let recovered = flight
            .run("next", || async { Ok(b"recovered".to_vec()) })
            .await
            .unwrap();
        assert_eq!(recovered, b"recovered");
    }
}
