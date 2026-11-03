//! HTTP/1.1, plaintext or over TLS.
//!
//! Despite the name this is not gRPC over HTTP/1.1 — gRPC requires HTTP/2 and always has. It
//! is the JSON/REST surface, which is what the Go runtime's `h1.go` serves too. For gRPC from
//! a browser, layer `tonic-web` onto the [`Surface`] instead; that is a different protocol
//! reaching the same services.
//!
//! [`Surface`]: super::Surface

use std::net::SocketAddr;

use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::tls::Certificate;

/// Serves `router` over HTTP/1.1 until `cancel` fires, then drains.
///
/// TLS is applied when a certificate is given. The listener is bound before this returns its
/// task, so an address already in use surfaces here rather than from a background task nobody
/// is watching.
pub async fn serve(
    router: Router,
    addr: &str,
    certificate: Option<&Certificate>,
    cancel: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    serve_bound(router, addr, certificate, cancel)
        .await
        .map(|(task, _)| task)
}

/// [`serve`], also returning the address bound: the port the operating system chose when
/// `addr` asked for port 0.
pub async fn serve_bound(
    router: Router,
    addr: &str,
    certificate: Option<&Certificate>,
    cancel: CancellationToken,
) -> Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|source| Error::Listen {
            addr: addr.to_string(),
            source,
        })?;

    let bound: SocketAddr = listener.local_addr().map_err(|source| Error::Listen {
        addr: addr.to_string(),
        source,
    })?;

    match certificate {
        None => {
            tracing::debug!(%bound, "HTTP/1.1: serving plaintext");
            Ok((
                tokio::spawn(async move {
                    let served = axum::serve(listener, router)
                        .with_graceful_shutdown(async move { cancel.cancelled().await })
                        .await;
                    if let Err(err) = served {
                        tracing::error!(%bound, error = %err, "HTTP/1.1 server stopped");
                    }
                }),
                bound,
            ))
        }
        #[cfg(feature = "tls")]
        Some(certificate) => {
            super::h1_tls::spawn(listener, bound, certificate, router, cancel).map(|t| (t, bound))
        }
        // Uninhabited without the `tls` feature: no certificate can reach here.
        #[cfg(not(feature = "tls"))]
        Some(certificate) => match *certificate {},
    }
}
