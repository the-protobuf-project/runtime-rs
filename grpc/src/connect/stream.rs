//! Connect's streaming calls: server streaming, client streaming and bidi.
//!
//! A streaming call is `application/connect+proto` or `application/connect+json`, each
//! message in an envelope (a flags byte, a big-endian length, the message) and ends with an
//! end-stream envelope carrying the status and trailers as JSON. The bridge does not need to
//! know a method's shape: it opens one gRPC call on the [`Loopback`], streams the request
//! envelopes into it as gRPC frames while relaying the reply frames as envelopes, and turns
//! the trailers into the end-stream envelope. Both directions run at once, so bidi is full
//! duplex over HTTP/2 and HTTP/3.

use std::sync::{Arc, Mutex};

use axum::response::Response;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use prost_reflect::DescriptorPool;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status};

use super::error::error_json;
use super::wire::{self, Format};
use crate::loopback::Loopback;

/// Envelope flags.
const COMPRESSED: u8 = 0x01;
const END_STREAM: u8 = 0x02;

/// Picks a streaming encoding from a `Content-Type`.
pub(super) fn stream_format(value: &str) -> Option<Format> {
    let media = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match media.as_str() {
        "application/connect+proto" => Some(Format::Binary),
        "application/connect+json" => Some(Format::Json),
        _ => None,
    }
}

fn stream_content_type(format: Format) -> &'static str {
    match format {
        Format::Binary => "application/connect+proto",
        Format::Json => "application/connect+json",
    }
}

/// Splits a byte stream into envelopes, however it was chunked.
#[derive(Default)]
struct Envelopes {
    buf: BytesMut,
}

impl Envelopes {
    fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// The next whole envelope, if one has arrived.
    fn next(&mut self) -> Result<Option<(u8, Bytes)>, Status> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
        if len > crate::http::MAX_REQUEST_BODY {
            return Err(Status::resource_exhausted(format!(
                "message of {len} bytes exceeds {}",
                crate::http::MAX_REQUEST_BODY
            )));
        }
        if self.buf.len() < 5 + len {
            return Ok(None);
        }
        let flags = self.buf[0];
        self.buf.advance(5);
        Ok(Some((flags, self.buf.split_to(len).freeze())))
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

fn envelope(flags: u8, data: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(5 + data.len());
    out.put_u8(flags);
    out.put_u32(u32::try_from(data.len()).unwrap_or(u32::MAX));
    out.put_slice(data);
    out.freeze()
}

/// The end-stream envelope: the error, when the call failed, and the trailers.
fn end_stream(status: Option<&Status>, trailers: &HeaderMap) -> Bytes {
    let mut end = serde_json::json!({});
    if let Some(status) = status.filter(|s| s.code() != Code::Ok) {
        end["error"] = error_json(status.code(), status.message(), status.details());
    }
    let mut metadata = serde_json::Map::new();
    for (name, value) in trailers {
        if wire::is_protocol_header(name) {
            continue;
        }
        if let Ok(value) = value.to_str() {
            let values = metadata
                .entry(name.as_str())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()));
            if let serde_json::Value::Array(values) = values {
                values.push(value.into());
            }
        }
    }
    if !metadata.is_empty() {
        end["metadata"] = metadata.into();
    }
    envelope(END_STREAM, end.to_string().as_bytes())
}

/// What a request-side failure leaves for the end-stream envelope to report.
type Failure = Arc<Mutex<Option<Status>>>;

/// Serves one streaming call.
pub(super) async fn serve(
    loopback: Loopback,
    pool: Arc<DescriptorPool>,
    format: Format,
    path: String,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    if headers
        .get("connect-content-encoding")
        .is_some_and(|e| e.as_bytes() != b"identity")
    {
        let status = Status::unimplemented("content encoding is not supported");
        return response(
            format,
            HeaderMap::new(),
            end_stream(Some(&status), &HeaderMap::new()).into(),
        );
    }

    let failure: Failure = Arc::default();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Status>>(16);
    tokio::spawn(pump(
        body,
        tx,
        format,
        pool.clone(),
        path.clone(),
        failure.clone(),
    ));
    let body = tonic::body::Body::new(StreamBody::new(ReceiverStream::new(rx)));
    let request = match wire::grpc_request(&path, &headers, body) {
        Ok(request) => request,
        Err(status) => {
            return response(
                format,
                HeaderMap::new(),
                end_stream(Some(&status), &HeaderMap::new()).into(),
            );
        }
    };

    let mut loopback = loopback;
    let Ok(reply) = tower::Service::call(&mut loopback, request).await;
    let (parts, body) = reply.into_parts();
    let mut metadata = HeaderMap::new();
    wire::copy_metadata(&parts.headers, &mut metadata, "");
    // A trailers-only reply carries its status in the headers.
    if let Some(status) = Status::from_header_map(&parts.headers) {
        return response(
            format,
            metadata,
            end_stream(Some(&status), &parts.headers).into(),
        );
    }
    let replies = relay(body, format, pool, path, failure);
    response(format, metadata, axum::body::Body::from_stream(replies))
}

fn response(format: Format, metadata: HeaderMap, body: axum::body::Body) -> Response {
    let mut response = Response::new(body);
    response.headers_mut().extend(metadata);
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(stream_content_type(format)),
    );
    response
}

/// Feeds the request envelopes into the gRPC call as frames. A bad envelope is recorded for
/// the end-stream envelope and aborts the request body, which ends the call.
async fn pump(
    mut body: axum::body::Body,
    tx: tokio::sync::mpsc::Sender<Result<Frame<Bytes>, Status>>,
    format: Format,
    pool: Arc<DescriptorPool>,
    path: String,
    failure: Failure,
) {
    let fail = |status: Status| {
        *failure.lock().expect("connect: failure poisoned") = Some(status.clone());
        status
    };
    let mut envelopes = Envelopes::default();
    loop {
        let chunk = match body.frame().await {
            None => break,
            Some(Err(err)) => {
                let _ = tx.send(Err(fail(Status::cancelled(err.to_string())))).await;
                return;
            }
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => data,
                Err(_) => continue,
            },
        };
        envelopes.push(&chunk);
        loop {
            let message = match envelopes.next() {
                Ok(None) => break,
                Ok(Some((flags, _))) if flags & COMPRESSED != 0 => Err(Status::unimplemented(
                    "compressed messages are not supported",
                )),
                Ok(Some((_, data))) => format
                    .request(&pool, &path, data)
                    .and_then(|message| wire::frame(&message)),
                Err(status) => Err(status),
            };
            match message {
                Ok(frame) => {
                    if tx.send(Ok(Frame::data(frame))).await.is_err() {
                        return; // the call ended; its status comes from the reply side
                    }
                }
                Err(status) => {
                    let _ = tx.send(Err(fail(status))).await;
                    return;
                }
            }
        }
    }
    if !envelopes.is_empty() {
        let _ = tx
            .send(Err(fail(Status::invalid_argument("truncated envelope"))))
            .await;
    }
}

/// The reply frames as envelopes, then the end-stream envelope from the trailers.
fn relay(
    body: tonic::body::Body,
    format: Format,
    pool: Arc<DescriptorPool>,
    path: String,
    failure: Failure,
) -> impl futures_util::Stream<Item = Result<Bytes, std::convert::Infallible>> + Send {
    struct State {
        body: tonic::body::Body,
        frames: Envelopes,
        done: bool,
    }
    let state = State {
        body,
        frames: Envelopes::default(),
        done: false,
    };
    futures_util::stream::unfold(state, move |mut state| {
        let pool = pool.clone();
        let path = path.clone();
        let failure = failure.clone();
        async move {
            if state.done {
                return None;
            }
            let finish = |state: &mut State, status: Option<Status>, trailers: &HeaderMap| {
                state.done = true;
                // A request-side failure is what the caller did wrong; report it first.
                let reported = failure
                    .lock()
                    .expect("connect: failure poisoned")
                    .clone()
                    .or(status);
                end_stream(reported.as_ref(), trailers)
            };
            loop {
                let out = match state.body.frame().await {
                    None => {
                        let status = Status::internal("the call ended without a status");
                        finish(&mut state, Some(status), &HeaderMap::new())
                    }
                    Some(Err(err)) => {
                        let status = Status::from_error(err.into());
                        finish(&mut state, Some(status), &HeaderMap::new())
                    }
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => {
                            state.frames.push(&data);
                            match messages(&mut state.frames, format, &pool, &path) {
                                Ok(out) if out.is_empty() => continue,
                                Ok(out) => out,
                                Err(status) => finish(&mut state, Some(status), &HeaderMap::new()),
                            }
                        }
                        Err(frame) => {
                            let trailers = frame.into_trailers().unwrap_or_default();
                            let status = Status::from_header_map(&trailers);
                            finish(&mut state, status, &trailers)
                        }
                    },
                };
                return Some((Ok(out), state));
            }
        }
    })
}

/// Every whole reply frame so far, converted and enveloped.
fn messages(
    frames: &mut Envelopes,
    format: Format,
    pool: &DescriptorPool,
    path: &str,
) -> Result<Bytes, Status> {
    let mut out = BytesMut::new();
    while let Some((flags, data)) = frames.next()? {
        if flags & COMPRESSED != 0 {
            return Err(Status::internal(
                "compressed gRPC replies are not supported",
            ));
        }
        let message = format.response(pool, path, data)?;
        out.extend_from_slice(&envelope(0, &message));
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::Envelopes;

    #[test]
    fn envelopes_split_across_chunks_reassemble() {
        let mut envelopes = Envelopes::default();
        envelopes.push(&[0, 0, 0]);
        assert!(envelopes.next().unwrap().is_none());
        envelopes.push(&[0, 2, b'h', b'i', 2, 0, 0]);
        let (flags, data) = envelopes.next().unwrap().unwrap();
        assert_eq!((flags, &data[..]), (0, &b"hi"[..]));
        assert!(envelopes.next().unwrap().is_none());
        assert!(!envelopes.is_empty());
    }
}
