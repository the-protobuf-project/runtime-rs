//! The D1 REST API over the workspace's network client: one statement, or one batch, per
//! request.

use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};
use serde::Deserialize;

use super::BACKEND;
use super::wire::{Envelope, classify, reasons};
use crate::{Error, Result};

/// Cloudflare's API root.
const API: &str = "https://api.cloudflare.com/client/v4";

/// Which account a [`D1Client`] talks to, as whom, and which database it drives by default.
#[derive(Clone, Debug, Default)]
pub struct D1Config {
    /// The account that owns the databases.
    pub account_id: String,
    /// An API token with D1 read and write access.
    pub api_token: String,
    /// The database an empty name selects. May be empty on a client that only ever selects
    /// by name.
    pub database_id: String,
    /// The API root; empty is Cloudflare's. Tests point it at a stand-in.
    pub base_url: String,
    /// Bounds each request; zero takes the network client's default. D1 bounds a query to
    /// thirty seconds, so a timeout below that can give up on a write that then lands.
    pub timeout: Duration,
}

/// A D1 account reached over HTTP. The network client inside is this type's own; it holds
/// nothing that needs closing.
pub struct D1Client {
    http: HttpClient,
    url: UrlOptions,
    root: String,
    headers: HashMap<String, String>,
    pub(crate) config: D1Config,
}

impl D1Client {
    /// A client over the account in `config`. Nothing is sent: a database is reached when
    /// [`D1Provider::set_database`](super::D1Provider) selects it.
    pub async fn connect(config: D1Config) -> Result<Self> {
        if config.account_id.is_empty() || config.api_token.is_empty() {
            return Err(Error::Invalid(
                "d1 needs an account id and an API token".to_owned(),
            ));
        }
        let base = match config.base_url.is_empty() {
            true => API,
            false => config.base_url.trim_end_matches('/'),
        };
        let (scheme, rest) = base
            .split_once("://")
            .ok_or_else(|| Error::Invalid(format!("d1: {base:?} is not a URL")))?;
        let (host, root) = rest.split_once('/').unwrap_or((rest, ""));
        let url = UrlOptions {
            scheme: scheme
                .parse::<UrlScheme>()
                .map_err(|e| Error::Invalid(format!("d1: {e}")))?,
            host: host.to_owned(),
            paths: vec![format!("/{root}")],
            params: HashMap::new(),
        };
        let mut http = HttpClient::default();
        http.connect(ConnectionOptions {
            url: url.clone(),
            timeout: config.timeout,
            skip_connectivity_check: true,
            ..ConnectionOptions::default()
        })
        .await
        .map_err(backend)?;
        let headers = HashMap::from([
            (
                "Authorization".to_owned(),
                format!("Bearer {}", config.api_token),
            ),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ]);
        Ok(Self {
            http,
            url,
            root: root.trim_matches('/').to_owned(),
            headers,
            config,
        })
    }

    /// The URL for `path` under the account.
    pub(crate) fn at(&self, path: &str, params: HashMap<String, String>) -> UrlOptions {
        let account = format!("accounts/{}/d1/database{path}", self.config.account_id);
        let path = match self.root.is_empty() {
            true => format!("/{account}"),
            false => format!("/{}/{account}", self.root),
        };
        UrlOptions {
            paths: vec![path],
            params,
            ..self.url.clone()
        }
    }

    /// Sends one request without retrying it, and reads the envelope's result.
    pub(crate) async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: HttpMethod,
        url: UrlOptions,
        body: Vec<u8>,
    ) -> Result<T> {
        let resp = self
            .http
            .request(method, &url, body, &self.headers, 0, 0, None)
            .await;
        let envelope: Option<Envelope<T>> = serde_json::from_slice(&resp.data).ok();
        if let Some(error) = resp.error {
            // The status says a request failed; the body says why, and only the body tells a
            // duplicate key from any other refusal.
            let why = envelope
                .map(|e| reasons(&e.errors))
                .filter(|why| !why.is_empty());
            return Err(classify(why.unwrap_or_else(|| error.to_string())));
        }
        envelope
            .and_then(|e| e.result)
            .ok_or_else(|| Error::Backend {
                backend: BACKEND,
                source: "the API answered without a result".into(),
            })
    }
}

fn backend(e: network::Error) -> Error {
    Error::Backend {
        backend: BACKEND,
        source: Box::new(e),
    }
}
