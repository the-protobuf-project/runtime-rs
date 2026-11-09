//! Retry around publishing.

use std::{sync::Arc, time::Duration};

use crate::{Middleware, Options, Payload, Publisher, Result};

/// Publisher that repeats a failed publish with doubling backoff.
struct RetryPublisher {
    next: Arc<dyn Publisher>,
    attempts: usize,
    backoff: Duration,
}

/// Retries a failed publish up to `attempts` times in total, doubling `backoff`
/// between attempts.
///
/// A settled failure, an undeclared subject or an unsupported request, is
/// returned at once: no number of attempts makes the stream declare the subject.
///
/// Retrying a publish can deliver it twice when the first attempt landed and
/// only its acknowledgement was lost. Pin [`Options::id`] so consumers can
/// deduplicate; the id is the same on every attempt.
pub fn with_retry(
    next: Arc<dyn Publisher>,
    attempts: usize,
    backoff: Duration,
) -> Arc<dyn Publisher> {
    if attempts <= 1 {
        return next;
    }
    Arc::new(RetryPublisher {
        next,
        attempts,
        backoff,
    })
}

/// [`with_retry`] as [`Middleware`].
pub fn with_retry_middleware(attempts: usize, backoff: Duration) -> Middleware {
    Arc::new(move |next| with_retry(next, attempts, backoff))
}

#[async_trait::async_trait]
impl Publisher for RetryPublisher {
    /// Pins an id before the first attempt so every retry reuses it.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        // Every attempt goes out under one id, so a duplicate from a lost
        // acknowledgement is recognisable as one.
        let opts = if opts.id.is_some() {
            opts.clone()
        } else {
            opts.clone().with_id(crate::provider::new_id())
        };
        let mut wait = self.backoff;
        let mut attempt = 1;
        loop {
            match self.next.publish(subject, payload.clone(), &opts).await {
                Ok(id) => return Ok(id),
                Err(error) if error.is_settled() || attempt == self.attempts => return Err(error),
                Err(_) => {
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2);
                    attempt += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::StreamError;

    struct Flaky {
        failures: Mutex<Vec<StreamError>>,
        ids: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Publisher for Flaky {
        /// Records the id it was given, then pops the next scripted failure.
        async fn publish(&self, _: &str, _: Payload, opts: &Options) -> Result<String> {
            self.ids.lock().unwrap().push(opts.id.clone().unwrap());
            match self.failures.lock().unwrap().pop() {
                Some(error) => Err(error),
                None => Ok(opts.id.clone().unwrap()),
            }
        }
    }

    /// A publisher that fails with `failures`, last element first, then succeeds.
    fn flaky(failures: Vec<StreamError>) -> Arc<Flaky> {
        Arc::new(Flaky {
            failures: Mutex::new(failures),
            ids: Mutex::new(Vec::new()),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn retries_transient_failures_under_one_id() {
        let inner = flaky(vec![StreamError::Internal("down".into()); 2]);
        let publisher = with_retry(inner.clone(), 3, Duration::from_millis(10));
        let id = publisher
            .publish("s", Payload::json(&1).unwrap(), &Options::default())
            .await
            .unwrap();
        let ids = inner.ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|seen| *seen == id));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_once_on_a_settled_failure() {
        let inner = flaky(vec![StreamError::Unsupported("no".into())]);
        let publisher = with_retry(inner.clone(), 5, Duration::from_millis(10));
        let error = publisher
            .publish("s", Payload::json(&1).unwrap(), &Options::default())
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)));
        assert_eq!(inner.ids.lock().unwrap().len(), 1);
    }
}
