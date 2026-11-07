//! Redis pub/sub and Streams. `plain` is docker/compose.redis.yaml.

use streams::{
    Registry,
    drivers::redis::{RedisConfig, RedisProvider, redis},
};

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(durable: bool, profile: &str, registry: Registry) -> Result<Conn, Error> {
    let client = match profile {
        "plain" => redis::Client::open(format!("redis://{}", env("REDIS_ADDR", "127.0.0.1:16380")))?,
        other => return Err(unknown_profile("redis", other)),
    };
    let config = RedisConfig { prefix: "bench".into(), registry, ..RedisConfig::default() };
    Ok(Conn::one(if durable {
        RedisProvider::durable(client, config)
    } else {
        RedisProvider::new(client, config)
    }))
}
