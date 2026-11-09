//! Redis pub/sub and Streams.
//!
//! - `plain` is docker/compose.redis.yaml (`REDIS_ADDR`, default
//!   127.0.0.1:16380).
//! - `tls` is docker/compose.redis.secure.yaml, reached as tests/redis_e2e.rs
//!   reaches it: mutual TLS on `REDIS_TLS_ADDR` (default 127.0.0.1:16381) with
//!   the certificates in `REDIS_TLS_CERTS` (default docker/tls/out), logged in
//!   as the ACL user `streams`, database 3. That user may touch only keys and
//!   channels under `e2e*` prefixes, so this profile names its keys
//!   `e2ebench:...` rather than widening the ACL for the bench.
//!
//! Durable consumers run with `confirm_acks` off (an ack is queued and sent as
//! part of a multi-id XACK instead of waiting a round trip; at-least-once is
//! unchanged, see `RedisConfig::confirm_acks`). `REDIS_CONFIRM_ACKS=1` measures
//! the driver's default, where every ack waits for its XACK reply.

use streams::{
    Registry,
    drivers::redis::{RedisConfig, RedisProvider, redis},
};

use super::{Conn, Error, env, unknown_profile};

/// Pub/sub, or Streams when `durable`. `tls` (feature `tls`) prefixes keys
/// `e2ebench` to stay inside the ACL user's key space.
pub async fn connect(durable: bool, profile: &str, registry: Registry) -> Result<Conn, Error> {
    let (client, prefix) = match profile {
        "plain" => (
            redis::Client::open(format!("redis://{}", env("REDIS_ADDR", "127.0.0.1:16380")))?,
            "bench",
        ),
        #[cfg(feature = "tls")]
        "tls" => (tls_client()?, "e2ebench"),
        other => return Err(unknown_profile("redis", other)),
    };
    let config = RedisConfig {
        prefix: prefix.into(),
        registry,
        confirm_acks: env("REDIS_CONFIRM_ACKS", "0") == "1",
        ..RedisConfig::default()
    };
    Ok(Conn::one(if durable {
        RedisProvider::durable(client, config)
    } else {
        RedisProvider::new(client, config)
    }))
}

/// The mTLS client the secure e2e suite uses, as the `streams` ACL user.
#[cfg(feature = "tls")]
fn tls_client() -> Result<redis::Client, Error> {
    use redis::{ClientTlsConfig, TlsCertificates};

    let dir = std::path::PathBuf::from(env(
        "REDIS_TLS_CERTS",
        concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out"),
    ));
    let read = |name: &str| {
        std::fs::read(dir.join(name)).map_err(|e| format!("{}: {e}", dir.join(name).display()))
    };
    Ok(redis::Client::build_with_tls(
        format!(
            "rediss://streams:streams-e2e-not-a-secret@{}/3",
            env("REDIS_TLS_ADDR", "127.0.0.1:16381")
        ),
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert: read("client.crt")?,
                client_key: read("client.key")?,
            }),
            root_cert: Some(read("ca.crt")?),
        },
    )?)
}
