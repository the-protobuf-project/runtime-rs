//! Writing and monitoring PLC tags over OPC UA, end to end.
//!
//! Start Microsoft's OPC PLC simulator with `docker compose -f
//! docker/compose.opcua.yaml up -d --wait`, then `cargo run --features opcua
//! --example opcua`. Point it elsewhere with `OPCUA_URL` (default
//! `opc.tcp://127.0.0.1:14840`) and `OPCUA_NAMESPACE`, the namespace URI the
//! nodes live in.

use std::time::Duration;

use futures::StreamExt;
use opcua::{
    client::{ClientBuilder, IdentityToken},
    crypto::SecurityPolicy,
    types::{DataValue, MessageSecurityMode, StatusCode, UserTokenPolicy, Variant},
};
use streams::{
    Options, Stream, Streams, as_durable,
    drivers::opcua::{OpcUaConfig, OpcUaProvider, OpcUaValue},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("OPCUA_URL").unwrap_or_else(|_| "opc.tcp://127.0.0.1:14840".into());
    let namespace = std::env::var("OPCUA_NAMESPACE")
        .unwrap_or_else(|_| "http://microsoft.com/Opc/OpcPlc/".into());

    // The session is yours: endpoint, security, certificates, identity. This
    // one is anonymous over SecurityPolicy None, which only a test server
    // should accept; tests/opcua_e2e.rs shows a signed and encrypted one.
    let mut client = ClientBuilder::new()
        .application_name("tpp-streams example")
        .application_uri("urn:tpp-streams:example")
        .pki_dir(std::env::temp_dir().join("tpp-streams-opcua-example"))
        .trust_server_certs(true)
        .session_retry_limit(3)
        .client()
        .map_err(|e| e.join("; "))?;
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                url.as_str(),
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await?;
    let events = event_loop.spawn();
    session.wait_for_connection().await;
    println!("connected to {url}");

    let provider = OpcUaProvider::new(session.clone(), OpcUaConfig::default());

    // Subjects are node ids. A namespace URI is stable where an index is not.
    let temperature = format!("nsu={namespace};s=Temperature");
    let mode = format!("nsu={namespace};s=Mode");
    let stream = provider
        .create(Stream {
            name: "line-1".into(),
            subjects: vec![temperature.clone(), mode.clone()],
            ..Stream::default()
        })
        .await?;
    let line = provider.bind(&stream.id).await?;

    let mut readings = line.subscribe(&temperature, &Options::default()).await?;
    for value in [21.5f64, 22.0, 22.75] {
        line.publish(
            &temperature,
            OpcUaValue::payload(value),
            &Options::default(),
        )
        .await?;
    }
    for _ in 0..3 {
        let message = tokio::time::timeout(Duration::from_secs(10), readings.next())
            .await?
            .ok_or("the stream ended")?;
        // The whole DataValue arrives: value, quality, and both timestamps.
        let value: DataValue = OpcUaValue::data_value(&message)?;
        println!(
            "{} = {:?} ({}) at {:?}",
            message.subject(),
            value.value.unwrap_or(Variant::Empty),
            value.status.unwrap_or(StatusCode::Good),
            value.source_timestamp,
        );
    }

    // A value of the wrong type is the server's refusal, named.
    if let Err(error) = line
        .publish(&mode, OpcUaValue::payload(42i32), &Options::default())
        .await
    {
        println!("refused: {error}");
    }
    // And what OPC UA does not have is refused by the contract.
    if let Err(error) = as_durable(line.as_ref()) {
        println!("refused: {error}");
    }

    drop(readings);
    provider.close().await?;
    session.disconnect().await?;
    let _ = events.await;
    Ok(())
}
