//! OPC UA client/server. `plain` is docker/compose.opcua.yaml (SecurityPolicy
//! None, anonymous); `tls` is docker/compose.opcua.secure.yaml, reached as
//! tests/opcua_e2e.rs reaches it: Basic256Sha256 with SignAndEncrypt, the
//! client application certificate the server trusts, the server's own
//! certificate pinned, and the `e2e-operator` user's password.
//!
//! Run it with `--message opcua`: the nodes are compose.opcua.nodes.json's
//! `bench.s00`..`bench.s09`, Int64 tags in opc-plc's namespace, so at most ten
//! subjects. A notification is a monitored item's data change, delivered in
//! the subscription's publishing cycle, so the publishing interval bounds
//! latency from below; `OPCUA_PUBLISHING_MS` (default 20) sets it.
//! `OPCUA_URL` (plain, default `opc.tcp://127.0.0.1:14840`), `OPCUA_E2E_URL`
//! (tls, default `opc.tcp://127.0.0.1:14841`) and `STREAMS_E2E_TLS_DIR`
//! (default `docker/tls/out`) point it elsewhere.

use std::{path::PathBuf, time::Duration};

use opcua::{
    client::{ClientBuilder, IdentityToken},
    crypto::{CertificateStore, SecurityPolicy, X509},
    types::{MessageSecurityMode, UserTokenPolicy},
};
use streams::{
    Registry,
    drivers::opcua::{OpcUaConfig, OpcUaProvider},
};

use super::{Conn, Error, env, unknown_profile};

/// The e2e suite's user, from docker/compose.opcua.secure.yaml.
const USER: (&str, &str) = ("e2e-operator", "e2e-operator-secret");

pub async fn connect(profile: &str, _registry: Registry) -> Result<Conn, Error> {
    let certs = std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")))
        .join("opcua");
    let pki = std::env::temp_dir().join(format!("tpp-streams-opcua-bench-{}", std::process::id()));
    std::fs::create_dir_all(pki.join("trusted"))?;
    let builder = ClientBuilder::new()
        .application_name("tpp-streams bench")
        .application_uri("urn:tpp-streams:e2e-client")
        .pki_dir(&pki)
        .create_sample_keypair(false)
        .session_retry_limit(3)
        .request_timeout(Duration::from_secs(10));
    let (url, builder, policy, mode, identity) = match profile {
        "plain" => (
            env("OPCUA_URL", "opc.tcp://127.0.0.1:14840"),
            builder.trust_server_certs(true),
            SecurityPolicy::None,
            MessageSecurityMode::None,
            IdentityToken::Anonymous,
        ),
        "tls" => {
            // Pin the server's certificate, as the e2e suite does.
            let der = std::fs::read(certs.join("server.der"))?;
            let cert = X509::from_der(&der)?;
            std::fs::write(
                pki.join("trusted")
                    .join(CertificateStore::cert_file_name(&cert)),
                der,
            )?;
            (
                env("OPCUA_E2E_URL", "opc.tcp://127.0.0.1:14841"),
                builder
                    .certificate_path(certs.join("client.der"))
                    .private_key_path(certs.join("client.pem"))
                    .trust_server_certs(false)
                    .verify_server_certs(true),
                SecurityPolicy::Basic256Sha256,
                MessageSecurityMode::SignAndEncrypt,
                IdentityToken::UserName(USER.0.into(), USER.1.into()),
            )
        }
        other => return Err(unknown_profile("opcua", other)),
    };
    let mut client = builder.client().map_err(|e| e.join("; "))?;
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                url.as_str(),
                policy.to_str(),
                mode,
                UserTokenPolicy::anonymous(),
            ),
            identity,
        )
        .await?;
    event_loop.spawn();
    if !tokio::time::timeout(Duration::from_secs(15), session.wait_for_connection()).await? {
        return Err(format!("opcua: cannot connect to {url}").into());
    }
    let publishing: u64 = env("OPCUA_PUBLISHING_MS", "20").parse()?;
    let config = OpcUaConfig {
        publishing_interval: Duration::from_millis(publishing),
        ..OpcUaConfig::default()
    };
    Ok(Conn::one(OpcUaProvider::new(session, config)))
}
