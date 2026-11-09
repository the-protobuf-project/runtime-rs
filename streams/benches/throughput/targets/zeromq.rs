//! ZeroMQ: brokerless, so the bench binds a publisher and connects a
//! subscriber to it over loopback TCP. There is no TLS profile: the zeromq
//! crate implements neither TLS nor CURVE.

use streams::{
    Registry,
    drivers::zeromq::{ZmqConfig, ZmqProvider},
};

use super::{Conn, Error, unknown_profile};

/// Binds a publisher on an ephemeral loopback port and connects a subscriber
/// to it; only `plain` exists.
pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    if profile != "plain" {
        return Err(unknown_profile("zeromq", profile));
    }
    let publisher = ZmqProvider::publisher(ZmqConfig {
        registry: registry.clone(),
        ..ZmqConfig::new("tcp://127.0.0.1:0")
    })
    .await?;
    let endpoint = publisher.endpoint().to_owned();
    let subscriber = ZmqProvider::subscriber(ZmqConfig {
        registry,
        ..ZmqConfig::new(endpoint)
    })?;
    Ok(Conn {
        publisher: Box::new(publisher),
        subscriber: Some(Box::new(subscriber)),
        flush: None,
    })
}
