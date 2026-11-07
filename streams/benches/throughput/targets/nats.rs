//! NATS core and JetStream. `plain` is docker/compose.nats.yaml.

use streams::{
    Registry,
    drivers::nats::{JetStreamProvider, NatsConfig, NatsProvider},
};

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(jetstream: bool, profile: &str, registry: Registry) -> Result<Conn, Error> {
    let client = match profile {
        "plain" => async_nats::connect(env("NATS_URL", "nats://127.0.0.1:14222")).await?,
        other => return Err(unknown_profile("nats", other)),
    };
    let config = NatsConfig { registry, ..NatsConfig::default() };
    Ok(if jetstream {
        Conn::one(JetStreamProvider::new(client, config).await?)
    } else {
        Conn::one(NatsProvider::new(client, config))
    })
}
