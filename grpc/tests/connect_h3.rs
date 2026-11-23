//! Connect over HTTP/3: bidi is full duplex there too, a call the server ends early does
//! not wait for the client, and a plain request is still served as before. Drives a real
//! QUIC client, since system curl on macOS has no HTTP/3.

#![cfg(feature = "http3")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::pb::{GetShelfRequest, Shelf};
use common::{SHOP_DESCRIPTOR_SET, Shop, free_port, shop_server};
use grpc::options::{Endpoint, Options};
use prost::Message;

/// Trusts the test's self-signed certificate, and nothing else.
#[derive(Debug)]
struct TrustOne(rustls::pki_types::CertificateDer<'static>);

impl rustls::client::danger::ServerCertVerifier for TrustOne {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.0.as_ref() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("unexpected certificate".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("tls 1.2 is not offered".into()))
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

type Send = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// A shop server with HTTP/3 and Connect, and an h3 client connected to it.
async fn shop_over_h3() -> (grpc::HybridServer, Send, tempdir::Dir) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certified =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let dir = tempdir::Dir::new();
    let (cert, key) = (dir.path("cert.pem"), dir.path("key.pem"));
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();

    let http_port = free_port();
    let mut server = grpc::HybridServer::new(Options {
        service_name: "connect-h3-test".into(),
        grpc: Endpoint::new("127.0.0.1", free_port()),
        http: Endpoint::new("127.0.0.1", http_port),
        enable_http: true,
        experimental_http3: true,
        ..Default::default()
    })
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(shop_server(Shop::default()))
    .with_certificates(cert.to_string_lossy(), key.to_string_lossy())
    .with_connect();
    server.start().await.expect("the server starts");

    let mut tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TrustOne(certified.cert.der().clone())))
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic)));
    let at: SocketAddr = format!("127.0.0.1:{}", http_port + 1).parse().unwrap();
    let connection = endpoint
        .connect(at, "localhost")
        .unwrap()
        .await
        .expect("QUIC handshake");
    let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(connection))
        .await
        .unwrap();
    tokio::spawn(async move { std::future::poll_fn(|cx| driver.poll_close(cx)).await });
    (server, send, dir)
}

fn envelope(name: &str) -> Bytes {
    let message = GetShelfRequest { name: name.into() }.encode_to_vec();
    let mut out = BytesMut::new();
    out.put_u8(0);
    out.put_u32(message.len() as u32);
    out.put_slice(&message);
    out.freeze()
}

/// Reads the next envelope from an h3 response stream.
async fn next_envelope<S: h3::quic::RecvStream>(
    stream: &mut h3::client::RequestStream<S, Bytes>,
    buf: &mut BytesMut,
) -> (u8, Bytes) {
    loop {
        if buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if buf.len() >= 5 + len {
                let flags = buf[0];
                buf.advance(5);
                return (flags, buf.split_to(len).freeze());
            }
        }
        let mut chunk = tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv_data())
            .await
            .expect("an envelope within 5s")
            .unwrap()
            .expect("more data");
        buf.put(chunk.copy_to_bytes(chunk.remaining()));
    }
}

fn connect_request(path: &str) -> http::Request<()> {
    http::Request::post(format!("https://localhost/shop.v1.ShopService/{path}"))
        .header("content-type", "application/connect+proto")
        .body(())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidi_over_http3_answers_each_request_before_the_next_is_sent() {
    let (server, mut send, _dir) = shop_over_h3().await;
    let stream = send.send_request(connect_request("Chat")).await.unwrap();
    let (mut tx, mut rx) = stream.split();
    tx.send_data(envelope("shelves/a")).await.unwrap();
    let response = rx.recv_response().await.unwrap();
    assert_eq!(response.status(), 200);
    let mut buf = BytesMut::new();
    let (_, first) = next_envelope(&mut rx, &mut buf).await;
    assert_eq!(Shelf::decode(first).unwrap().name, "shelves/a");
    tx.send_data(envelope("shelves/b")).await.unwrap();
    let (_, second) = next_envelope(&mut rx, &mut buf).await;
    assert_eq!(Shelf::decode(second).unwrap().name, "shelves/b");
    tx.finish().await.unwrap();
    let (flags, end) = next_envelope(&mut rx, &mut buf).await;
    assert_eq!(flags & 0x02, 0x02);
    let end: serde_json::Value = serde_json::from_slice(&end).unwrap();
    assert!(end.get("error").is_none(), "{end}");
    server.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_the_server_ends_does_not_wait_for_the_client() {
    let (server, mut send, _dir) = shop_over_h3().await;
    let stream = send
        .send_request(connect_request("NoSuchMethod"))
        .await
        .unwrap();
    let (mut tx, mut rx) = stream.split();
    tx.send_data(envelope("shelves/a")).await.unwrap(); // and the request side stays open
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv_response())
        .await
        .expect("the response within 5s")
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut buf = BytesMut::new();
    let (flags, end) = next_envelope(&mut rx, &mut buf).await;
    assert_eq!(flags & 0x02, 0x02);
    let end: serde_json::Value = serde_json::from_slice(&end).unwrap();
    assert_eq!(end["error"]["code"], "unimplemented", "{end}");
    server.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_requests_over_http3_are_served_as_before() {
    let (server, mut send, _dir) = shop_over_h3().await;
    let request = http::Request::post("https://localhost/shop.v1.ShopService/GetShelf")
        .header("content-type", "application/json")
        .body(())
        .unwrap();
    let mut stream = send.send_request(request).await.unwrap();
    stream
        .send_data(Bytes::from_static(br#"{"name":"shelves/1"}"#))
        .await
        .unwrap();
    stream.finish().await.unwrap();
    assert_eq!(stream.recv_response().await.unwrap().status(), 200);
    let mut body = BytesMut::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        body.put(chunk.copy_to_bytes(chunk.remaining()));
    }
    let shelf: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(shelf["name"], "shelves/1");
    server.stop().await.unwrap();
}

/// A directory removed when the test ends.
mod tempdir {
    pub struct Dir(std::path::PathBuf);

    impl Dir {
        pub fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "connect-h3-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        pub fn path(&self, name: &str) -> std::path::PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
