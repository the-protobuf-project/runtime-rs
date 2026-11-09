//! RabbitMQ. `plain` is docker/compose.rabbitmq.yaml; `tls` is
//! docker/compose.rabbitmq.secure.yaml, reached as tests/rabbitmq_e2e.rs
//! reaches it: AMQPS with mutual TLS, the client certificate logging in
//! through SASL EXTERNAL.
//!
//! `AMQP_URL` overrides the plain URL; `RABBITMQ_E2E_ADDR` (default
//! `127.0.0.1:15671`) and `STREAMS_E2E_TLS_DIR` (default
//! `streams/docker/tls/out`) the secure broker and its certificates.
//! `RABBITMQ_PUBLISH_CHANNELS` sets `RabbitConfig::publish_channels` (default:
//! the driver's).

use std::{path::PathBuf, sync::Arc};

use lapin::{
    Connection, ConnectionProperties, DefaultConnectionBuilder,
    tcp::{OwnedIdentity, OwnedTLSConfig},
};
use streams::{
    Registry,
    drivers::rabbitmq::{RabbitConfig, RabbitProvider},
};

use super::{Conn, Error, env, unknown_profile};

/// Connects over AMQP, or AMQPS with SASL EXTERNAL for `tls`;
/// `RABBITMQ_PUBLISH_CHANNELS` sets the publishing channel pool.
pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let conn = match profile {
        "plain" => {
            let url = env("AMQP_URL", "amqp://guest:guest@127.0.0.1:15682/%2f");
            Connection::connect(&url, ConnectionProperties::default()).await?
        }
        "tls" => {
            let addr = env("RABBITMQ_E2E_ADDR", "127.0.0.1:15671");
            DefaultConnectionBuilder::new()?
                .with_uri_str(format!("amqps://{addr}/%2f?auth_mechanism=external"))
                .with_properties(ConnectionProperties::default())
                .with_tls_config(mutual_tls()?)
                .connect()
                .await?
        }
        other => return Err(unknown_profile("rabbitmq", other)),
    };
    let mut config = RabbitConfig {
        prefix: "bench".into(),
        registry,
        ..RabbitConfig::default()
    };
    if let Ok(n) = std::env::var("RABBITMQ_PUBLISH_CHANNELS") {
        config.publish_channels = n
            .parse()
            .map_err(|_| format!("RABBITMQ_PUBLISH_CHANNELS: {n:?}"))?;
    }
    Ok(Conn::one(
        RabbitProvider::new(Arc::new(conn), config).await?,
    ))
}

/// Trusts the test CA and presents the client certificate, whose CN is the
/// broker's one user.
fn mutual_tls() -> Result<OwnedTLSConfig, Error> {
    let dir = std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"));
    let read = |name: &str| {
        std::fs::read(dir.join(name))
            .map_err(|e| format!("cannot read {}: {e}", dir.join(name).display()))
    };
    Ok(OwnedTLSConfig {
        identity: Some(OwnedIdentity::PKCS8 {
            pem: read("client.crt")?,
            key: read("client.key")?,
        }),
        cert_chain: Some(String::from_utf8(read("ca.crt")?)?),
    })
}
