//! MQTT. `plain` is docker/compose.mqtt.yaml; `tls` is
//! docker/compose.mqtt.secure.yaml's password listener, reached exactly as
//! tests/mqtt_e2e.rs reaches it: mutual TLS (the test client certificate,
//! trusting the test CA, rustls on ring), then the `e2e-app` user's password,
//! under the broker's ACLs.
//!
//! Those ACLs give each user only `<user>/#`, so the `tls` profile publishes
//! under the prefix `e2e-app/bench` rather than `bench`: the topic tree is the
//! one the user owns, and the broker's authorization stays exactly what the e2e
//! suite proves it is. Widening the ACL for the bench would measure a broker
//! the e2e suite does not test.
//!
//! `MQTT_HOST`/`MQTT_PORT` (plain), `MQTT_TLS_ADDR` (default `127.0.0.1:18884`),
//! and `STREAMS_E2E_TLS_DIR` (default `docker/tls/out`) point it elsewhere.
//! `MQTT_PUBLISH_CONNECTIONS` overrides the provider's publishing connections.

use std::{path::PathBuf, sync::Arc};

use rumqttc::{TlsConfiguration, Transport};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use streams::{
    Registry,
    drivers::mqtt::{MqttConfig, MqttOptions, MqttProvider, QoS},
};

use super::{Conn, Error, env, unknown_profile};

/// The e2e suite's password-listener user, from docker/compose.mqtt.secure.yaml.
const USER: (&str, &str) = ("e2e-app", "e2e-app-secret");

/// Connects at QoS 1. `tls` publishes under `e2e-app/bench`, the only tree the
/// broker's ACLs give that user.
pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let (options, prefix) = match profile {
        "plain" => {
            let port = env("MQTT_PORT", "11883").parse()?;
            (
                MqttOptions::new("bench", env("MQTT_HOST", "127.0.0.1"), port),
                "bench".to_owned(),
            )
        }
        "tls" => {
            let addr = env("MQTT_TLS_ADDR", "127.0.0.1:18884");
            let (host, port) = addr.rsplit_once(':').ok_or("MQTT_TLS_ADDR is host:port")?;
            let mut options = MqttOptions::new("bench", host, port.parse()?);
            options.set_connection_timeout(5);
            options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(tls()?)));
            options.set_credentials(USER.0, USER.1);
            (options, format!("{}/bench", USER.0))
        }
        other => return Err(unknown_profile("mqtt", other)),
    };
    let mut config = MqttConfig {
        prefix,
        qos: QoS::AtLeastOnce,
        registry,
        ..MqttConfig::default()
    };
    if let Ok(n) = std::env::var("MQTT_PUBLISH_CONNECTIONS") {
        config.publish_connections = n.parse()?;
    }
    Ok(Conn::one(MqttProvider::connect(options, config).await?))
}

/// Mutual TLS on ring with the test CA and client certificate.
fn tls() -> Result<Arc<ClientConfig>, Error> {
    let dir = std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")));
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(dir.join("ca.crt"))? {
        roots.add(cert?)?;
    }
    let chain =
        CertificateDer::pem_file_iter(dir.join("client.crt"))?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(dir.join("client.key"))?;
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)?;
    Ok(Arc::new(config))
}
