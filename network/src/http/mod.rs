mod request;

use std::fmt;

use crate::error::{Error, Result};
use crate::options::{ConnectionOptions, UrlScheme, DEFAULT_TIMEOUT};
use crate::transport::new_pooled_client;
use crate::url::build_full_url;

/// Caps the bytes drained from the connectivity-check response body (after a GET fallback),
/// limiting exposure to oversized responses.
const MAX_CONNECTIVITY_RESPONSE_BODY_BYTES: usize = 1 << 20; // 1 MiB

/// The HTTP verb for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    /// `GET`
    Get,
    /// `POST`
    Post,
    /// `PUT`
    Put,
    /// `PATCH`
    Patch,
    /// `DELETE`
    Delete,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        })
    }
}

impl From<HttpMethod> for reqwest::Method {
    fn from(m: HttpMethod) -> Self {
        match m {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Patch => reqwest::Method::PATCH,
            HttpMethod::Delete => reqwest::Method::DELETE,
        }
    }
}

/// An HTTP REST client. Create via [`crate::Network::new_connection`] and
/// [`crate::Network::as_http`]. It embeds [`ConnectionOptions`] (URL, timeout, headers, retries,
/// retry delay, skip-connectivity-check).
#[derive(Default)]
pub struct HttpClient {
    pub(crate) client: Option<reqwest::Client>,
    /// The options this client was last connected with.
    pub options: ConnectionOptions,
}

impl HttpClient {
    /// Configures the HTTP client and optionally verifies the server is reachable. If
    /// `opts.timeout` is zero, [`DEFAULT_TIMEOUT`] is used. If `skip_connectivity_check` is true,
    /// no request is sent. Otherwise a HEAD request is sent; if the server returns 405, a GET is
    /// sent instead and the body is drained (up to 1 MiB). Returns an error if the reachability
    /// check fails.
    pub async fn connect(&mut self, mut opts: ConnectionOptions) -> Result<()> {
        if opts.timeout.is_zero() {
            opts.timeout = DEFAULT_TIMEOUT;
        }

        let full_url = build_full_url(&opts.url, 0).map_err(|e| Error::BuildUrl(Box::new(e)))?;
        if !matches!(opts.url.scheme, UrlScheme::Http | UrlScheme::Https) {
            return Err(Error::InvalidScheme(opts.url.scheme.to_string()));
        }

        let client = new_pooled_client(opts.timeout)?;
        self.client = Some(client);
        self.options = opts;

        if self.options.skip_connectivity_check {
            return Ok(());
        }
        self.check_connectivity(&full_url).await
    }

    /// Sends a HEAD request (falling back to GET on 405) and drains the response body so the
    /// connection can be reused. Returns an error if the host is unreachable.
    async fn check_connectivity(&self, full_url: &str) -> Result<()> {
        let client = self.client.as_ref().expect("connect sets client before check_connectivity");
        let host = self.options.url.host.clone();

        let resp = client
            .head(full_url)
            .send()
            .await
            .map_err(|source| Error::HttpConnect { host: host.clone(), source })?;

        let resp = if resp.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            client
                .get(full_url)
                .send()
                .await
                .map_err(|source| Error::HttpConnect { host: host.clone(), source })?
        } else {
            resp
        };

        let mut stream = resp.bytes_stream();
        let mut drained = 0usize;
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| Error::HttpConnect { host: host.clone(), source })?;
            drained += chunk.len();
            if drained >= MAX_CONNECTIVITY_RESPONSE_BODY_BYTES {
                break;
            }
        }
        Ok(())
    }

    /// Clears the HTTP client. It is not usable until `connect` is called again.
    pub async fn close(&mut self) -> Result<()> {
        self.client = None;
        Ok(())
    }

    /// Re-applies the current [`ConnectionOptions`] (calls `connect` again).
    pub async fn reconnect(&mut self) -> Result<()> {
        self.connect(self.options.clone()).await
    }
}
