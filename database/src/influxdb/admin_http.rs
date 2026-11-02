//! Small management transport for endpoints absent from the Rust SDK.
//! Point writes and all queries continue to use the SDK. This client owns a
//! separate pool and follows the SDK's headers, auth, proxy and CA settings.
//!
//! TLS is a rustls config built here on the ring provider. reqwest is compiled with no
//! provider of its own, so that this crate never turns on aws-lc, and a client it built
//! itself would panic unless the process had installed a default. Handing it a finished
//! config touches no process-wide state. The roots are the SDK's too: the bundled Mozilla
//! set, plus `ssl_roots_path`. reqwest's `add_root_certificate` does not apply to a finished
//! config, which is why that file is read here.
use super::errors::sdk;
use crate::store::{Error, Result};
use influxdb3_client::ClientConfig;
use reqwest::{Client, Method, header::AUTHORIZATION};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::Value;
use std::sync::Arc;
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

fn backend(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend {
        backend: "influxdb3",
        source: Box::new(source),
    }
}

/// The client's TLS: ring, the bundled roots, and every certificate in `extra`.
fn tls(extra: Option<&[u8]>) -> Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(pem) = extra {
        for certificate in CertificateDer::pem_slice_iter(pem) {
            roots.add(certificate.map_err(backend)?).map_err(backend)?;
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(backend)?
        .with_root_certificates(roots)
        .with_no_client_auth())
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
        let extra = match &config.ssl_roots_path {
            Some(path) => Some(std::fs::read(path).map_err(backend)?),
            None => None,
        };
        let tls = tls(extra.as_deref())?;
        Ok(Self {
            client: builder
                .tls_backend_preconfigured(tls)
                .build()
                .map_err(transport)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ring_config_needs_no_process_wide_provider() {
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
        tls(None).expect("a config");
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    }

    #[test]
    fn a_malformed_extra_root_is_an_error_rather_than_ignored() {
        let pem = b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n";
        assert!(tls(Some(pem)).is_err());
    }

    /// Past the build, on to a real handshake: the server hangs up after the ClientHello, so
    /// the call fails as an error, and reaching it means TLS ran on the ring config.
    #[tokio::test]
    async fn a_client_built_here_speaks_tls_without_a_default_provider() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hello = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first = [0u8; 1];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut first)
                .await
                .unwrap();
            first[0]
        });
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls(None).unwrap())
            .build()
            .expect("no provider is needed to build");
        let answer = client.get(format!("https://{addr}/")).send().await;
        assert!(answer.is_err(), "the peer never finished a handshake");
        assert_eq!(hello.await.unwrap(), 0x16, "a TLS handshake record");
    }
}
