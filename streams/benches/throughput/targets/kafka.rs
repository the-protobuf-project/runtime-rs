//! Kafka. `plain` is docker/compose.kafka.yaml.

use streams::{
    Registry,
    drivers::kafka::{KafkaConfig, KafkaProvider, rdkafka::ClientConfig},
};

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let mut client = ClientConfig::new();
    match profile {
        "plain" => {
            client.set("bootstrap.servers", env("KAFKA_ADDR", "127.0.0.1:19092"));
        }
        other => return Err(unknown_profile("kafka", other)),
    }
    let config = KafkaConfig { prefix: "bench".into(), partitions: 10, registry, ..KafkaConfig::default() };
    Ok(Conn::one(KafkaProvider::connect(client, config).await?))
}
