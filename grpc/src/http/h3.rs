//! HTTP/3 over QUIC.
//!
//! Always encrypted: QUIC carries TLS 1.3 in its transport handshake, so there is no plaintext
//! mode to offer and no option to add one.
//!
//! What is worth noticing is how little there is here beyond the transport. The request
//! handling is the same router the HTTP/1.1 listener serves, because a router never knew which
//! transport it was behind — which is why adding HTTP/3 costs a listener rather than a second
//! implementation of the API.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use bytes::{Buf, Bytes};
use http_body_util::BodyExt;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::error::{Error, Result};
use crate::tls::Certificate;

/// The ALPN token HTTP/3 negotiates under.
const ALPN_H3: &str = "h3";

/// Serves `router` over HTTP/3 until `cancel` fires.
///
/// # Errors
///
/// Fails if the socket cannot be bound or the TLS configuration is rejected by the QUIC layer.
/// Per-connection failures are logged and the listener continues.
pub async fn serve(
    router: Router,
    addr: &str,
    certificate: &Certificate,
    cancel: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    let bound: SocketAddr = addr
        .parse()
        .map_err(|_| Error::InvalidPort { component: "http3" })?;

    let tls = certificate.rustls_config(&[ALPN_H3])?;
    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|err| Error::Http3(err.to_string()))?;
    let endpoint = quinn::Endpoint::server(quinn::ServerConfig::with_crypto(Arc::new(quic)), bound)
        .map_err(|source| Error::Listen {
            addr: addr.to_string(),
            source,
        })?;

    tracing::debug!(%bound, "HTTP/3: listening (QUIC, TLS 1.3)");

    Ok(tokio::spawn(async move {
        loop {
            let incoming = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                incoming = endpoint.accept() => incoming,
            };
            let Some(incoming) = incoming else { break };

            let router = router.clone();
            tokio::spawn(async move {
                match incoming.await {
                    Ok(connection) => {
                        if let Err(err) = serve_connection(router, connection).await {
                            tracing::debug!(?err, "HTTP/3 connection ended");
                        }
                    }
                    Err(err) => tracing::debug!(?err, "QUIC handshake failed"),
                }
            });
        }
        // Close every connection before waiting for them to go idle: without it, wait_idle
        // waits on clients that have no reason to hang up, and the stop runs out its timeout.
        endpoint.close(0u32.into(), b"server shutting down");
        endpoint.wait_idle().await;
    }))
}

/// Serves every request on one QUIC connection.
async fn serve_connection(
    router: Router,
    connection: quinn::Connection,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut h3 = h3::server::Connection::new(h3_quinn::Connection::new(connection)).await?;

    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => {
                // Resolved inside the task, so a client slow to send its headers holds up only
                // its own request, and one malformed request does not end the connection.
                let router = router.clone();
                tokio::spawn(async move {
                    match resolver.resolve_request().await {
                        Ok((request, stream)) => {
                            if let Err(err) = respond(router, request, stream).await {
                                tracing::debug!(?err, "HTTP/3 request failed");
                            }
                        }
                        Err(err) => tracing::debug!(?err, "HTTP/3 request could not be resolved"),
                    }
                });
            }
            // The peer closed the connection cleanly.
            Ok(None) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }
}

/// Reads one request body, runs the router, and writes the response back.
async fn respond<S>(
    router: Router,
    request: http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::BidiStream<Bytes> + Send + 'static,
{
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        // Rejected as soon as it would pass the limit, before the chunk is copied, so an
        // oversized body costs at most the limit in memory.
        if body.len() + chunk.remaining() > super::MAX_REQUEST_BODY {
            stream
                .send_response(
                    http::Response::builder()
                        .status(http::StatusCode::PAYLOAD_TOO_LARGE)
                        .body(())?,
                )
                .await?;
            stream.finish().await?;
            return Ok(());
        }
        body.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
    }

    let (parts, ()) = request.into_parts();
    let rebuilt = http::Request::from_parts(parts, Body::from(body));

    let response = router.oneshot(rebuilt).await?;
    let (parts, response_body) = response.into_parts();

    stream
        .send_response(http::Response::from_parts(parts, ()))
        .await?;

    // The body is collected before the status is written, so a handler that fails partway
    // through cannot have already committed a 200 — the same rule the HTTP/1.1 path follows.
    let bytes = response_body.collect().await?.to_bytes();
    if !bytes.is_empty() {
        stream.send_data(bytes).await?;
    }
    stream.finish().await?;
    Ok(())
}
