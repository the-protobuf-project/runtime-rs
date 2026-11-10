//! Kafka. `plain` is docker/compose.kafka.yaml; `tls` is the mutual-TLS
//! listener of docker/compose.kafka.secure.yaml, connecting exactly as
//! tests/kafka_e2e.rs does with its primary (`Mtls`) principal: TLS verified
//! against docker/tls/out/ca.crt, client certificate presented.

use std::path::PathBuf;

use streams::{
    Registry,
    drivers::kafka::{KafkaConfig, KafkaProvider, rdkafka::ClientConfig},
};

use super::{Conn, Error, env, unknown_profile};

/// Path of a test certificate, from `STREAMS_E2E_TLS_DIR` or docker/tls/out.
fn cert(name: &str) -> String {
    let dir = std::env::var("STREAMS_E2E_TLS_DIR").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"),
        PathBuf::from,
    );
    dir.join(name).to_string_lossy().into_owned()
}

/// Connects `plain` or mutual-TLS `tls` with ten partitions per topic, so
/// subjects spread across partitions; `KAFKA_PRODUCERS` sets the producer pool,
/// and `KAFKA_CONFIG` (`key=value,key=value`) sets librdkafka properties on top.
pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let mut client = ClientConfig::new();
    match profile {
        "plain" => {
            client.set("bootstrap.servers", env("KAFKA_ADDR", "127.0.0.1:19092"));
        }
        "tls" => {
            client
                .set(
                    "bootstrap.servers",
                    env("KAFKA_E2E_ADDR", "127.0.0.1:19093"),
                )
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", cert("ca.crt"))
                .set("ssl.endpoint.identification.algorithm", "https")
                .set("ssl.certificate.location", cert("client.crt"))
                .set("ssl.key.location", cert("client.key"));
        }
        other => return Err(unknown_profile("kafka", other)),
    }
    for pair in env("KAFKA_CONFIG", "").split(',').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').expect("KAFKA_CONFIG is key=value,...");
        client.set(key, value);
    }
    let config = KafkaConfig {
        prefix: "bench".into(),
        partitions: 10,
        registry,
        producers: env("KAFKA_PRODUCERS", "8")
            .parse()
            .expect("KAFKA_PRODUCERS"),
        ..KafkaConfig::default()
    };
    Ok(Conn::one(KafkaProvider::connect(client, config).await?))
}
