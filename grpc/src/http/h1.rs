//! HTTP/1.1, plaintext or over TLS.
//!
//! Despite the name this is not gRPC over HTTP/1.1 — gRPC requires HTTP/2 and always has. It
//! is the JSON/REST surface, which is what the Go runtime's `h1.go` serves too. For gRPC from
//! a browser, layer `tonic-web` onto the [`Surface`] instead; that is a different protocol
//! reaching the same services.
//!
//! [`Surface`]: super::Surface

use std::net::SocketAddr;
use std::sync::Arc;

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
            Ok(tokio::spawn(async move {
                let served = axum::serve(listener, router)
                    .with_graceful_shutdown(async move { cancel.cancelled().await })
                    .await;
                if let Err(err) = served {
                    tracing::error!(%bound, error = %err, "HTTP/1.1 server stopped");
                }
            }))
        }
        Some(certificate) => {
            // The gateway advertises HTTP/1.1 and HTTP/2 over ALPN. HTTP/3 is advertised by
            // Alt-Svc instead, because a client cannot negotiate QUIC over a TCP handshake.
            let config = certificate.rustls_config(&["h2", "http/1.1"])?;
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            tracing::debug!(%bound, "HTTP/1.1: serving over TLS 1.3");
            Ok(tokio::spawn(serve_tls(listener, acceptor, router, cancel)))
        }
    }
}

/// Accepts TLS connections until cancelled.
async fn serve_tls(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    router: Router,
    cancel: CancellationToken,
) {
    let service = router.into_make_service();

    loop {
        let accepted = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            accepted = listener.accept() => accepted,
        };

        let Ok((stream, peer)) = accepted else {
            continue;
        };
        let acceptor = acceptor.clone();
        let mut service = service.clone();

        tokio::spawn(async move {
            let Ok(tls) = acceptor.accept(stream).await else {
                // A failed handshake is one client's problem — a bad SNI, an old cipher suite,
                // a port scanner — and not a reason to stop serving everyone else.
                tracing::debug!(%peer, "TLS handshake failed");
                return;
            };

            let io = hyper_util::rt::TokioIo::new(tls);
            // MakeService is infallible here, but it still has to be driven to readiness
            // before it will hand over the per-connection service.
            if tower::ServiceExt::<SocketAddr>::ready(&mut service)
                .await
                .is_err()
            {
                return;
            }
            let Ok(tower_service) = tower::Service::call(&mut service, peer).await;

            let hyper_service = hyper::service::service_fn(move |request| {
                let mut tower_service = tower_service.clone();
                async move { tower::Service::call(&mut tower_service, request).await }
            });

            if let Err(err) =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection_with_upgrades(io, hyper_service)
                    .await
            {
                tracing::debug!(%peer, ?err, "connection closed");
            }
        });
    }
}
