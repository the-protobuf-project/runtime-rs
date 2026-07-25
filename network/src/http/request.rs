use std::collections::HashMap;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::HttpMethod;
use crate::error::{Error, Result};
use crate::options::UrlOptions;
use crate::url::build_full_url;

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(2);

impl super::HttpClient {
    /// Performs an HTTP request with optional retries. Builds the URL from `url_options` and
    /// `path_index`, sends the request, and retries up to `max_retries` times with
    /// [`ConnectionOptions::retry_delay`](crate::ConnectionOptions::retry_delay) between attempts.
    /// Each attempt gets its own fresh timeout window (mirroring a per-attempt deadline rather than
    /// one deadline for the whole retry sequence). `cancel`, when provided, aborts the request and
    /// any pending retries.
    #[allow(clippy::too_many_arguments)] // mirrors runtime-go/network's HTTPClient.Request signature
    pub async fn request(
        &self,
        method: HttpMethod,
        url_options: &UrlOptions,
        body: Vec<u8>,
        headers: &HashMap<String, String>,
        path_index: i64,
        max_retries: usize,
        cancel: Option<&CancellationToken>,
    ) -> Result<Vec<u8>> {
        let retry_delay = if self.options.retry_delay.is_zero() {
            DEFAULT_RETRY_DELAY
        } else {
            self.options.retry_delay
        };

        let mut last_err: Option<Error> = None;
        for attempt in 0..=max_retries {
            if attempt > 0 {
                if let Some(cancel) = cancel {
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {}
                        _ = cancel.cancelled() => return Err(Error::Cancelled),
                    }
                } else {
                    tokio::time::sleep(retry_delay).await;
                }
            }

            let full_url = build_full_url(url_options, path_index)
                .map_err(|e| Error::BuildUrl(Box::new(e)))?;
            let attempt_fut = self.send_once(method, &full_url, body.clone(), headers);
            let attempt_result = match cancel {
                Some(cancel) => {
                    tokio::select! {
                        res = tokio::time::timeout(self.options.timeout, attempt_fut) => {
                            res.map_err(|_| Error::Timeout)
                        }
                        _ = cancel.cancelled() => return Err(Error::Cancelled),
                    }
                }
                None => tokio::time::timeout(self.options.timeout, attempt_fut)
                    .await
                    .map_err(|_| Error::Timeout),
            };

            match attempt_result.and_then(|inner| inner) {
                Ok(data) => return Ok(data),
                Err(err) => last_err = Some(err),
            }
        }

        Err(Error::RetryExhausted {
            retries: max_retries,
            source: Box::new(
                last_err.expect("at least one attempt runs when max_retries loop executes"),
            ),
        })
    }

    async fn send_once(
        &self,
        method: HttpMethod,
        full_url: &str,
        body: Vec<u8>,
        headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let client = self.client.as_ref().ok_or(Error::HttpNotConnected)?;
        let mut builder = client.request(method.into(), full_url).body(body);
        for (k, v) in headers {
            builder = builder.header(k, v);
        }
        if let Some(propagator) = &self.options.trace_propagator {
            let mut carrier = crate::transport::HeaderCarrier::default();
            let cx = opentelemetry::Context::current();
            propagator.inject_context(&cx, &mut carrier);
            for (k, v) in carrier.0 {
                builder = builder.header(k, v);
            }
        }

        let resp = builder.send().await.map_err(Error::SendRequest)?;
        let status = resp.status();
        let data = resp.bytes().await.map_err(Error::ReadBody)?.to_vec();
        validate_status_code(status.as_u16())?;
        Ok(data)
    }
}

/// Maps HTTP status codes to errors. 2xx returns `Ok`; 4xx/5xx return descriptive errors.
fn validate_status_code(status: u16) -> Result<()> {
    match status {
        200..=299 => Ok(()),
        400 => Err(Error::ClientStatus(400, "Bad Request")),
        401 => Err(Error::ClientStatus(401, "Unauthorized")),
        403 => Err(Error::ClientStatus(403, "Forbidden")),
        404 => Err(Error::ClientStatus(404, "Not Found")),
        429 => Err(Error::ClientStatus(429, "Too Many Requests")),
        500..=599 => Err(Error::ServerStatus(status)),
        other => Err(Error::UnexpectedStatus(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_2xx() {
        assert!(validate_status_code(200).is_ok());
        assert!(validate_status_code(204).is_ok());
    }

    #[test]
    fn validates_known_4xx() {
        assert!(matches!(
            validate_status_code(404),
            Err(Error::ClientStatus(404, "Not Found"))
        ));
        assert!(matches!(
            validate_status_code(429),
            Err(Error::ClientStatus(429, "Too Many Requests"))
        ));
    }

    #[test]
    fn validates_5xx() {
        assert!(matches!(
            validate_status_code(503),
            Err(Error::ServerStatus(503))
        ));
    }

    #[test]
    fn validates_unexpected() {
        assert!(matches!(
            validate_status_code(300),
            Err(Error::UnexpectedStatus(300))
        ));
    }
}
