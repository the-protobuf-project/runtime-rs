//! Small management transport for endpoints absent from the Rust SDK.
//! Point writes and all queries continue to use the SDK. This client owns a
//! separate pool and follows the SDK's headers, auth, proxy and CA settings.
use super::errors::sdk;
use crate::store::{Error, Result};
use influxdb3_client::ClientConfig;
use reqwest::{Client, Method, header::AUTHORIZATION};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub(super) struct Http {
    client: Client,
    host: String,
}

fn transport(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else {
        Error::Backend {
            backend: "influxdb3",
            source: Box::new(error.without_url()),
        }
    }
}

impl Http {
    pub fn new(config: &ClientConfig, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("timeout must be positive".into()));
        }
        let mut headers = config.headers.clone();
        if let Some(mut auth) = config.authorization_header().map_err(sdk)? {
            auth.set_sensitive(true);
            headers.insert(AUTHORIZATION, auth);
        }
        let mut builder = Client::builder()
            .timeout(timeout)
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(proxy) = &config.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy).map_err(transport)?);
        }
        if let Some(path) = &config.ssl_roots_path {
            let pem = std::fs::read(path).map_err(|source| Error::Backend {
                backend: "influxdb3",
                source: Box::new(source),
            })?;
            builder = builder
                .add_root_certificate(reqwest::Certificate::from_pem(&pem).map_err(transport)?);
        }
        Ok(Self {
            client: builder.build().map_err(transport)?,
            host: config.host_url().trim_end_matches('/').into(),
        })
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
    ) -> Result<()> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.host))
            .query(query);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(transport)?;
        match response.status().as_u16() {
            200..=299 => Ok(()),
            404 => Err(Error::NotFound),
            409 => Err(Error::AlreadyExists),
            status => Err(Error::HttpStatus { status }),
        }
    }
}
