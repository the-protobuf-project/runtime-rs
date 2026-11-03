//! The TLS material every transport shares — in a build without the `tls` feature, where
//! there is none.
//!
//! [`Certificate`] still exists so the transports keep one shape across builds: they take an
//! `Option<&Certificate>`, and here that option can only ever be `None`, because the type
//! has no values. The compiler, not a runtime check, is what proves no plaintext-only build
//! reaches a TLS code path. [`crate::HybridServer::with_certificates`] is absent, so nothing
//! can ask for one.

/// A TLS certificate and key — uninhabited in this build, which has no TLS. Enable the
/// `tls` feature for the real one.
#[derive(Debug, Clone)]
pub enum Certificate {}

impl Certificate {
    /// The certificate file this was loaded from. Unreachable: no value exists.
    pub fn cert_path(&self) -> &str {
        match *self {}
    }

    /// The key file this was loaded from. Unreachable: no value exists.
    pub fn key_path(&self) -> &str {
        match *self {}
    }
}

/// Unreachable: no certificate exists to turn TLS on with.
pub(crate) fn configure_grpc(
    _builder: tonic::transport::Server,
    cert: &Certificate,
) -> crate::Result<tonic::transport::Server> {
    match *cert {}
}
