//! RabbitMQ. `plain` is docker/compose.rabbitmq.yaml.

use std::sync::Arc;

use streams::{
    Registry,
    drivers::rabbitmq::{RabbitConfig, RabbitProvider},
};

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let conn = match profile {
        "plain" => {
            let url = env("AMQP_URL", "amqp://guest:guest@127.0.0.1:15682/%2f");
            lapin::Connection::connect(&url, Default::default()).await?
        }
        other => return Err(unknown_profile("rabbitmq", other)),
    };
    let config = RabbitConfig { prefix: "bench".into(), registry, ..RabbitConfig::default() };
    Ok(Conn::one(RabbitProvider::new(Arc::new(conn), config).await?))
}
