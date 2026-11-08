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

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(jetstream: bool, profile: &str, registry: Registry) -> Result<Conn, Error> {
    let client = match profile {
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
    };
    let config = NatsConfig {
        registry,
        ..NatsConfig::default()
    };
    Ok(if jetstream {
        Conn::one(JetStreamProvider::new(client, config).await?)
    } else {
        Conn::one(NatsProvider::new(client, config))
    })
}
