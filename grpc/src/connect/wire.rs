//! A Connect unary body to one gRPC frame, and the single reply frame back.

use bytes::{BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body_util::BodyExt;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor};
use tonic::Status;

use crate::loopback::Loopback;

/// The two Connect unary encodings.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Format {
    Binary,
    Json,
}

impl Format {
    /// Picks the encoding from a `Content-Type`, ignoring parameters such as `charset`.
    pub(super) fn from_content_type(value: &str) -> Option<Self> {
        let media = value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match media.as_str() {
            "application/proto" | "application/protobuf" => Some(Self::Binary),
            "application/json" => Some(Self::Json),
            _ => None,
        }
    }

    pub(super) fn content_type(self) -> &'static str {
        match self {
            Self::Binary => "application/proto",
            Self::Json => "application/json",
        }
    }

    /// The protobuf bytes of the method's input.
    pub(super) fn request(
        self,
        pool: &DescriptorPool,
        path: &str,
        body: Bytes,
    ) -> Result<Bytes, Status> {
        if self == Self::Binary {
            return Ok(body);
        }
        let method = method(pool, path)?;
        let json: &[u8] = if body.is_empty() { b"{}" } else { &body };
        let mut deserializer = serde_json::Deserializer::from_slice(json);
        let message = DynamicMessage::deserialize(method.input(), &mut deserializer)
            .map_err(|err| Status::invalid_argument(format!("decoding the request: {err}")))?;
        Ok(message.encode_to_vec().into())
    }

    /// The response body for the method's protobuf output.
    pub(super) fn response(
        self,
        pool: &DescriptorPool,
        path: &str,
        message: Bytes,
    ) -> Result<Bytes, Status> {
        if self == Self::Binary {
            return Ok(message);
        }
        let method = method(pool, path)?;
        let message = DynamicMessage::decode(method.output(), message)
            .map_err(|err| Status::internal(format!("decoding the response: {err}")))?;
        serde_json::to_vec(&message)
            .map(Bytes::from)
            .map_err(|err| Status::internal(format!("encoding the response: {err}")))
    }
}

/// Resolves `/pkg.Service/Method` in the registered descriptor sets.
fn method(pool: &DescriptorPool, path: &str) -> Result<MethodDescriptor, Status> {
    let (service, name) = path
        .trim_start_matches('/')
        .split_once('/')
        .unwrap_or_default();
    pool.get_service_by_name(service)
        .and_then(|s| s.methods().find(|m| m.name() == name))
        .ok_or_else(|| {
            Status::unimplemented(format!(
                "{service}.{name} is not in a registered descriptor set: register it with \
                 with_reflection, or call it as application/proto"
            ))
        })
}

/// Headers that belong to HTTP or to the protocols themselves, not to the call's metadata.
pub(super) fn is_protocol_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    matches!(
        name,
        "host"
            | "content-type"
            | "content-length"
            | "content-encoding"
            | "accept-encoding"
            | "connection"
            | "keep-alive"
            | "te"
            | "transfer-encoding"
            | "upgrade"
            | "user-agent"
            | "accept"
            | "date"
            | "server"
    ) || name.starts_with("connect-")
        || name.starts_with("grpc-")
}

/// One gRPC frame: an uncompressed flag, the big-endian length, the message.
pub(super) fn frame(message: &[u8]) -> Result<Bytes, Status> {
    let len = u32::try_from(message.len())
        .map_err(|_| Status::resource_exhausted("message too large"))?;
    let mut frame = BytesMut::with_capacity(5 + message.len());
    frame.put_u8(0);
    frame.put_u32(len);
    frame.put_slice(message);
    Ok(frame.freeze())
}

/// The body of a unary gRPC request: one frame.
pub(super) fn unary_body(message: &[u8]) -> Result<tonic::body::Body, Status> {
    Ok(tonic::body::Body::new(http_body_util::Full::new(frame(
        message,
    )?)))
}

/// A gRPC request for `path` with `body`, carrying the caller's headers as metadata and
/// `Connect-Timeout-Ms` as `grpc-timeout`. `-bin` values are base64 on both wires, so they
/// pass through unchanged.
pub(super) fn grpc_request(
    path: &str,
    headers: &HeaderMap,
    body: tonic::body::Body,
) -> Result<http::Request<tonic::body::Body>, Status> {
    let mut request = http::Request::builder()
        .method(http::Method::POST)
        .uri(path)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(body)
        .map_err(|err| Status::invalid_argument(format!("bad request path: {err}")))?;
    for (name, value) in headers {
        if !is_protocol_header(name) {
            request.headers_mut().append(name.clone(), value.clone());
        }
    }
    if let Some(ms) = timeout_ms(headers)? {
        // gRPC allows eight digits per unit, so a long deadline is sent in seconds.
        let value = if ms < 100_000_000 {
            format!("{ms}m")
        } else {
            format!("{}S", ms / 1000)
        };
        if let Ok(value) = HeaderValue::from_str(&value) {
            request.headers_mut().insert("grpc-timeout", value);
        }
    }
    Ok(request)
}

/// `Connect-Timeout-Ms`, if the caller sent one.
fn timeout_ms(headers: &HeaderMap) -> Result<Option<u64>, Status> {
    let Some(timeout) = headers.get("connect-timeout-ms") else {
        return Ok(None);
    };
    timeout
        .to_str()
        .ok()
        .filter(|v| !v.is_empty() && v.len() <= 10)
        .and_then(|v| v.parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            Status::invalid_argument(
                "Connect-Timeout-Ms must be a positive integer of at most 10 digits",
            )
        })
}

/// When the call must end. The Loopback has no deadline layer of its own (tonic enforces
/// `grpc-timeout` in its listener, not in the routes), so the bridge enforces it.
pub(super) fn deadline(headers: &HeaderMap) -> Result<Option<tokio::time::Instant>, Status> {
    Ok(timeout_ms(headers)?
        .map(|ms| tokio::time::Instant::now() + std::time::Duration::from_millis(ms)))
}

/// Runs `work` until `deadline`, ending it with DEADLINE_EXCEEDED after. Dropping the
/// future is what cancels the handler.
pub(super) async fn until<T>(
    deadline: Option<tokio::time::Instant>,
    work: impl std::future::Future<Output = T>,
) -> Result<T, Status> {
    match deadline {
        None => Ok(work.await),
        Some(at) => tokio::time::timeout_at(at, work)
            .await
            .map_err(|_| Status::deadline_exceeded("the Connect-Timeout-Ms deadline passed")),
    }
}

/// What a unary gRPC call returned: its message, its status (absent means OK), and the
/// response headers and trailers the caller should see, trailers prefixed `trailer-`.
pub(super) struct Reply {
    pub(super) message: Bytes,
    pub(super) status: Option<Status>,
    pub(super) metadata: HeaderMap,
}

impl Reply {
    fn failed(status: Status, metadata: HeaderMap) -> Self {
        Self {
            message: Bytes::new(),
            status: Some(status),
            metadata,
        }
    }
}

/// Calls the loopback and reads the one response message and the trailers.
pub(super) async fn call(
    mut loopback: Loopback,
    request: http::Request<tonic::body::Body>,
) -> Reply {
    let Ok(response) = tower::Service::call(&mut loopback, request).await;
    let (parts, body) = response.into_parts();
    let mut metadata = HeaderMap::new();
    copy_metadata(&parts.headers, &mut metadata, "");
    // A trailers-only response carries its status in the headers.
    if let Some(status) = Status::from_header_map(&parts.headers) {
        return Reply::failed(status, metadata);
    }
    let collected = match body.collect().await {
        Ok(collected) => collected,
        Err(err) => return Reply::failed(Status::internal(err.to_string()), metadata),
    };
    let trailers = collected.trailers().cloned().unwrap_or_default();
    copy_metadata(&trailers, &mut metadata, "trailer-");
    let status = Status::from_header_map(&trailers);
    match unframe(&collected.to_bytes()) {
        Ok(message) => Reply {
            message,
            status,
            metadata,
        },
        Err(status) => Reply::failed(status, metadata),
    }
}

/// The single message of a unary gRPC response body.
fn unframe(framed: &Bytes) -> Result<Bytes, Status> {
    if framed.is_empty() {
        return Ok(Bytes::new());
    }
    let header = framed
        .get(..5)
        .ok_or_else(|| Status::internal("truncated gRPC frame"))?;
    if header[0] != 0 {
        return Err(Status::internal(
            "compressed gRPC responses are not supported",
        ));
    }
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    framed
        .get(5..5 + len)
        .map(|m| framed.slice_ref(m))
        .ok_or_else(|| Status::internal("truncated gRPC message"))
}

/// Copies call metadata, leaving out the protocols' own headers, each name prefixed.
pub(super) fn copy_metadata(from: &HeaderMap, into: &mut HeaderMap, prefix: &str) {
    for (name, value) in from {
        if is_protocol_header(name) {
            continue;
        }
        if let Ok(name) = HeaderName::from_bytes(format!("{prefix}{name}").as_bytes()) {
            into.append(name, value.clone());
        }
    }
}

/// A successful Connect unary response.
pub(super) fn connect_response(
    format: Format,
    body: Bytes,
    metadata: &HeaderMap,
) -> axum::response::Response {
    let mut response = axum::response::Response::new(axum::body::Body::from(body));
    response.headers_mut().extend(metadata.clone());
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(format.content_type()),
    );
    response
}
