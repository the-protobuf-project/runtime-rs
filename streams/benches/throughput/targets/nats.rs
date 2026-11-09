//! NATS core and JetStream.
//!
//! - `plain` is docker/compose.nats.yaml (`NATS_URL`, default
//!   `nats://127.0.0.1:14222`).
//! - `tls` is the `nats` service of docker/compose.nats.secure.yaml
//!   (`NATS_E2E_URL`, default `tls://127.0.0.1:14223`), reached exactly as
//!   tests/nats_e2e.rs reaches it: mutual TLS with docker/tls/out's client
//!   certificate, then the test-only password user, whose permissions include
//!   the bench's `bench.>` subjects. Start the broker with
//!   `STREAMS_DURABILITY=prod` for the server's default fsync interval, or
//!   leave it unset for an fsync on every write.

use std::path::PathBuf;

use async_nats::ConnectOptions;
use streams::{
    Registry,
    drivers::nats::{JetStreamProvider, NatsConfig, NatsProvider},
};

use super::{Conn, Error, Flush, env, unknown_profile};

/// Core NATS or JetStream, with separate publishing and subscribing
/// connections; `flush` drains the publisher for publish-only runs.
pub async fn connect(jetstream: bool, profile: &str, registry: Registry) -> Result<Conn, Error> {
    // Publishers and subscribers get a connection each, as `nats bench` runs
    // them and as a service that both produces and consumes heavily would:
    // one async-nats connection is one reader and one writer task, and
    // sharing it makes PubAcks queue behind deliveries.
    let config = NatsConfig {
        registry,
        ..NatsConfig::default()
    };
    let (publisher, subscriber) = (dial(profile).await?, dial(profile).await?);
    let flushing = publisher.clone();
    let flush: Flush = Box::new(move || {
        let client = flushing.clone();
        Box::pin(async move { client.flush().await.map_err(Error::from) })
    });
    Ok(if jetstream {
        Conn {
            publisher: Box::new(JetStreamProvider::new(publisher, config.clone()).await?),
            subscriber: Some(Box::new(JetStreamProvider::new(subscriber, config).await?)),
            flush: Some(flush),
        }
    } else {
        Conn {
            publisher: Box::new(NatsProvider::new(publisher, config.clone())),
            subscriber: Some(Box::new(NatsProvider::new(subscriber, config))),
            flush: Some(flush),
        }
    })
}

/// Opens one connection for `profile`: plain, or mutual TLS plus the test password user.
async fn dial(profile: &str) -> Result<async_nats::Client, Error> {
    Ok(match profile {
        "plain" => async_nats::connect(env("NATS_URL", "nats://127.0.0.1:14222")).await?,
        "tls" => {
            let certs = std::env::var("NATS_E2E_CERTS").map_or_else(
                |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"),
                PathBuf::from,
            );
            ConnectOptions::new()
                .add_root_certificates(certs.join("ca.crt"))
                .add_client_certificate(certs.join("client.crt"), certs.join("client.key"))
                .require_tls(true)
                .user_and_password("e2e-password".into(), "e2e-test-only-password".into())
                .connect(env("NATS_E2E_URL", "tls://127.0.0.1:14223"))
                .await?
        }
        other => return Err(unknown_profile("nats", other)),
    })
}
