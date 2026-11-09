//! OPC UA PubSub over MQTT (`opcua-pubsub`), on the same brokers as the
//! `mqtt` backend so the two compare like with like: `plain` is
//! docker/compose.mqtt.yaml, `tls` is docker/compose.mqtt.secure.yaml's
//! password listener (mutual TLS, the `e2e-app` user, its ACLs, so the Part
//! 14 prefix is `e2e-app/bench/opcua`).
//!
//! Run it with `--message opcua --subject-prefix pubsub.`: the payload is a
//! typed OPC UA DataValue, as on the client/server backend, and every bench
//! subject is a DataSetWriter of its own with one field, `v`. The subjects
//! are mapped here, `pubsub.bench.s00` to `pubsub.bench.s00/v`, so each writer
//! has its own topic and sequence, as each subject has its own topic on
//! `mqtt`. A batch is one NetworkMessage per batch, up to the provider's
//! `max_dataset_messages`.
//!
//! `MQTT_HOST`/`MQTT_PORT`, `MQTT_TLS_ADDR`, `STREAMS_E2E_TLS_DIR`, and
//! `MQTT_PUBLISH_CONNECTIONS` are read as the `mqtt` backend reads them.

use std::{path::PathBuf, sync::Arc};

use rumqttc::{TlsConfiguration, Transport};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use streams::{
    Batch, Manager, Options, Payload, Publisher, Registry, Result, Stream, Streams, Subscriber,
    Subscription, as_batch,
    drivers::opcua::pubsub::{MqttOptions, PubSubConfig, PubSubProvider},
};

use super::{Conn, Error, env, unknown_profile};

const USER: (&str, &str) = ("e2e-app", "e2e-app-secret");

pub async fn connect(profile: &str, _registry: Registry) -> std::result::Result<Conn, Error> {
    let (mut options, prefix) = match profile {
        "plain" => (
            MqttOptions::new(
                "bench",
                env("MQTT_HOST", "127.0.0.1"),
                env("MQTT_PORT", "11883").parse()?,
            ),
            "bench/opcua".to_owned(),
        ),
        "tls" => {
            let addr = env("MQTT_TLS_ADDR", "127.0.0.1:18884");
            let (host, port) = addr.rsplit_once(':').ok_or("MQTT_TLS_ADDR is host:port")?;
            let mut options = MqttOptions::new("bench", host, port.parse()?);
            options.set_connection_timeout(5);
            options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(tls()?)));
            options.set_credentials(USER.0, USER.1);
            (options, format!("{}/bench/opcua", USER.0))
        }
        other => return Err(unknown_profile("opcua-pubsub", other)),
    };
    // A batch's NetworkMessage is larger than rumqttc's 10 KiB default.
    options.set_max_packet_size(Some(1 << 20));
    let mut config = PubSubConfig {
        publisher_id: "bench".into(),
        prefix,
        ..PubSubConfig::default()
    };
    if let Ok(n) = std::env::var("MQTT_PUBLISH_CONNECTIONS") {
        config.publish_connections = n.parse()?;
    }
    Ok(Conn::one(OneFieldWriters(
        PubSubProvider::connect(options, config).await?,
    )))
}

/// Each declared subject is a DataSetWriter, published and read as its one
/// field `v`.
struct OneFieldWriters(PubSubProvider);

struct Bound(Arc<dyn Manager>);

fn field(subject: &str) -> String {
    format!("{subject}/v")
}

#[async_trait::async_trait]
impl Streams for OneFieldWriters {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        self.0.create(stream).await
    }
    async fn get(&self, id: &str) -> Result<Stream> {
        self.0.get(id).await
    }
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        Ok(Arc::new(Bound(self.0.bind(id).await?)))
    }
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        self.0.update(id, stream).await
    }
    async fn delete(&self, id: &str) -> Result<()> {
        self.0.delete(id).await
    }
    async fn list(&self) -> Result<Vec<Stream>> {
        self.0.list().await
    }
    async fn close(&self) -> Result<()> {
        self.0.close().await
    }
}

#[async_trait::async_trait]
impl Publisher for Bound {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.0.publish(&field(subject), payload, opts).await
    }
}

#[async_trait::async_trait]
impl Subscriber for Bound {
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.0.subscribe(&field(subject), opts).await
    }
}

#[async_trait::async_trait]
impl Batch for Bound {
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        as_batch(self.0.as_ref())?
            .publish_batch(&field(subject), payloads, opts)
            .await
    }
}

impl Manager for Bound {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// Mutual TLS on ring with the test CA and client certificate, as the `mqtt`
/// backend's `tls` profile.
fn tls() -> std::result::Result<Arc<ClientConfig>, Error> {
    let dir = std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")));
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(dir.join("ca.crt"))? {
        roots.add(cert?)?;
    }
    let chain = CertificateDer::pem_file_iter(dir.join("client.crt"))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(dir.join("client.key"))?;
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)?;
    Ok(Arc::new(config))
}
