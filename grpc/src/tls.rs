//! The TLS material every transport on this server shares.

use std::io::BufReader;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{Error, Result};

/// A TLS certificate and its private key, as the PEM bytes both consumers want.
///
/// tonic takes PEM directly and rustls takes parsed DER, so the PEM is kept and parsed on
/// demand rather than stored twice in two shapes that could drift.
#[derive(Clone)]
pub struct Certificate {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    cert_path: String,
    key_path: String,
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certificate")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish_non_exhaustive()
    }
}

impl Certificate {
    /// Loads a certificate chain and private key from PEM files.
    ///
    /// Go's `mustLoadCertificate` ends the process on failure. Returning the error instead
    /// lets it surface from [`crate::HybridServer::start`] like every other configuration
    /// problem — the server still does not come up, but the caller decides what that means.
    pub fn from_pem_files(
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
    ) -> Result<Self> {
        let (cert_path, key_path) = (cert_path.into(), key_path.into());
        tracing::debug!(cert = %cert_path, key = %key_path, "loading TLS certificate");

        let cert_pem = std::fs::read(&cert_path).map_err(|source| Error::ReadCertificate {
            path: cert_path.clone(),
            source,
        })?;
        let key_pem = std::fs::read(&key_path).map_err(|source| Error::ReadCertificate {
            path: key_path.clone(),
            source,
        })?;

        let loaded = Self {
            cert_pem,
            key_pem,
            cert_path,
            key_path,
        };
        // Parse once here so a malformed file fails at load rather than at bind, when the
        // banner has already claimed the port.
        loaded.chain()?;
        loaded.private_key()?;
        Ok(loaded)
    }

    /// The certificate file this was loaded from.
    pub fn cert_path(&self) -> &str {
        &self.cert_path
    }

    /// The key file this was loaded from.
    pub fn key_path(&self) -> &str {
        &self.key_path
    }

    /// The certificate chain, parsed.
    pub fn chain(&self) -> Result<Vec<CertificateDer<'static>>> {
        let mut reader = BufReader::new(self.cert_pem.as_slice());
        let chain: Vec<_> =
            rustls_pemfile::certs(&mut reader).collect::<std::result::Result<_, _>>()?;
        if chain.is_empty() {
            return Err(Error::EmptyCertificate {
                kind: "certificate",
                path: self.cert_path.clone(),
            });
        }
        Ok(chain)
    }

    /// The private key, parsed.
    pub fn private_key(&self) -> Result<PrivateKeyDer<'static>> {
        let mut reader = BufReader::new(self.key_pem.as_slice());
        rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| Error::EmptyCertificate {
            kind: "private key",
            path: self.key_path.clone(),
        })
    }

    /// The identity a tonic server takes.
    pub fn identity(&self) -> tonic::transport::Identity {
        tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem)
    }

    /// A rustls server configuration advertising `alpn`, for the transports that build
    /// their own acceptor rather than letting tonic do it.
    pub fn rustls_config(&self, alpn: &[&str]) -> Result<rustls::ServerConfig> {
        install_crypto_provider();
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(self.chain()?, self.private_key()?)?;
        config.alpn_protocols = alpn.iter().map(|p| p.as_bytes().to_vec()).collect();
        Ok(config)
    }
}

/// Installs the ring provider once, so a caller that never touches rustls directly still
/// gets a working default.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
