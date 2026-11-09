//! The wire frame every provider writes and reads.
//!
//! The layout is byte-for-byte runtime-go's `streams/core` frame, so a Go
//! publisher and a Rust subscriber on one broker understand each other:
//!
//! ```text
//! 0xB7 | 0x01 | uvarint len | codec name | uvarint len | message id | data
//! ```
//!
//! The marker byte rejects a payload some other producer wrote to the same
//! topic, and the version byte lets the layout change without a future frame
//! being misread as this one.
//!
//! Version 2 adds headers, for the transports that have no header mechanism
//! of their own (Redis pub/sub, ZeroMQ); everywhere else headers travel with
//! the broker's own. A version-2 frame is written only when there are headers
//! to carry, so a message without them is still version 1, which runtime-go
//! reads:
//!
//! ```text
//! 0xB7 | 0x02 | codec name | message id | uvarint count | (key, value) * count | data
//! ```

use std::sync::Arc;

use bytes::Bytes;

use crate::{Headers, Message, Payload, Registry, Result, StreamError};

/// First byte of every frame.
const FRAME_MARKER: u8 = 0xB7;
/// The layout version this program writes and reads.
const FRAME_VERSION: u8 = 0x01;
/// The layout that also carries headers.
const FRAME_VERSION_HEADERS: u8 = 0x02;
/// The most headers one frame may declare, so a corrupt count cannot make a
/// reader loop for long.
const MAX_HEADERS: u64 = 1024;
/// Upper bound on the codec name and id, so a corrupt length cannot make a
/// reader allocate or scan gigabytes.
const MAX_FIELD: u64 = 1 << 20;

/// Frames a payload under `id`.
pub fn pack(id: &str, payload: &Payload) -> Vec<u8> {
    let name = payload.codec().as_bytes();
    let data = payload.data();
    let mut out = Vec::with_capacity(2 + 20 + name.len() + id.len() + data.len());
    out.push(FRAME_MARKER);
    out.push(FRAME_VERSION);
    put_uvarint(&mut out, name.len() as u64);
    out.extend_from_slice(name);
    put_uvarint(&mut out, id.len() as u64);
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(data);
    out
}

/// Frames a payload under `id` with `headers`, for a transport with no header
/// mechanism of its own: version 1 when there are none, so runtime-go still
/// reads it, and version 2 otherwise.
pub fn pack_with(id: &str, payload: &Payload, headers: &Headers) -> Vec<u8> {
    if headers.is_empty() {
        return pack(id, payload);
    }
    let name = payload.codec().as_bytes();
    let data = payload.data();
    let extra: usize = headers.iter().map(|(k, v)| k.len() + v.len() + 4).sum();
    let mut out = Vec::with_capacity(2 + 30 + name.len() + id.len() + extra + data.len());
    out.push(FRAME_MARKER);
    out.push(FRAME_VERSION_HEADERS);
    put_uvarint(&mut out, name.len() as u64);
    out.extend_from_slice(name);
    put_uvarint(&mut out, id.len() as u64);
    out.extend_from_slice(id.as_bytes());
    put_uvarint(&mut out, headers.len() as u64);
    for (key, value) in headers.iter() {
        put_uvarint(&mut out, key.len() as u64);
        out.extend_from_slice(key.as_bytes());
        put_uvarint(&mut out, value.len() as u64);
        out.extend_from_slice(value.as_bytes());
    }
    out.extend_from_slice(data);
    out
}

/// Reads a frame that arrived on `subject`.
///
/// It refuses a payload this contract did not write, a frame from a future
/// version, one without an id, and one naming a codec `registry` lacks. The
/// data is a slice of `frame`, not a copy.
pub fn unpack(registry: &Registry, subject: &str, frame: impl Into<Bytes>) -> Result<Message> {
    unpack_shared(registry, &Arc::from(subject), frame)
}

/// [`unpack`] for a subject the caller already holds shared, as a subscription
/// does for every message it receives: the message points at that one copy, so
/// reading a frame allocates only its id.
pub fn unpack_shared(
    registry: &Registry,
    subject: &Arc<str>,
    frame: impl Into<Bytes>,
) -> Result<Message> {
    let frame: Bytes = frame.into();
    let fail = |reason: String| StreamError::Frame {
        subject: subject.to_string(),
        reason,
    };
    if frame.len() < 2 || frame[0] != FRAME_MARKER {
        return Err(fail("was not published through this contract".into()));
    }
    let version = frame[1];
    if version != FRAME_VERSION && version != FRAME_VERSION_HEADERS {
        return Err(fail(format!(
            "uses frame version {version}, and this program understands {FRAME_VERSION} and {FRAME_VERSION_HEADERS}"
        )));
    }
    let mut at = 2;
    let name = field(&frame, &mut at, "codec name").map_err(fail)?;
    let id = field(&frame, &mut at, "message id").map_err(fail)?;
    if id.is_empty() {
        return Err(fail("carries no id".into()));
    }
    let name = std::str::from_utf8(name).map_err(|_| fail("has a non-UTF-8 codec name".into()))?;
    let id = std::str::from_utf8(id).map_err(|_| fail("has a non-UTF-8 id".into()))?;
    let mut headers = Headers::new();
    if version == FRAME_VERSION_HEADERS {
        let (count, read) =
            uvarint(&frame[at..]).ok_or_else(|| fail("has a truncated header count".into()))?;
        if count > MAX_HEADERS {
            return Err(fail(format!(
                "declares {count} headers, beyond the {MAX_HEADERS} limit"
            )));
        }
        at += read;
        for _ in 0..count {
            let key = field(&frame, &mut at, "header key").map_err(fail)?;
            let value = field(&frame, &mut at, "header value").map_err(fail)?;
            let key =
                std::str::from_utf8(key).map_err(|_| fail("has a non-UTF-8 header key".into()))?;
            let value = std::str::from_utf8(value)
                .map_err(|_| fail("has a non-UTF-8 header value".into()))?;
            headers.insert(key, value);
        }
    }
    let codec = registry.resolve(name)?;
    Ok(Message::shared(id, subject.clone(), codec, frame.slice(at..)).with_headers(headers))
}

/// Reads one length-prefixed field at `*at`, advancing past it.
fn field<'a>(frame: &'a [u8], at: &mut usize, what: &str) -> std::result::Result<&'a [u8], String> {
    let truncated = || format!("has a truncated {what}");
    let (len, read) = uvarint(&frame[*at..]).ok_or_else(truncated)?;
    if len > MAX_FIELD {
        return Err(format!(
            "declares a {what} of {len} bytes, beyond the {MAX_FIELD} limit"
        ));
    }
    let start = *at + read;
    let end = start + len as usize;
    if end > frame.len() {
        return Err(truncated());
    }
    *at = end;
    Ok(&frame[start..end])
}

/// Appends `value` as an unsigned LEB128 varint, Go's `binary.AppendUvarint`.
fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Reads an unsigned LEB128 varint, Go's `binary.Uvarint`, returning the value
/// and the bytes consumed, or `None` when truncated or overflowing.
fn uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, &byte) in buf.iter().enumerate().take(10) {
        if i == 9 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte < 0x80 {
            return Some((value, i + 1));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Order {
        user: String,
        total: u32,
    }

    /// A fixed order the round-trip tests encode and compare against.
    fn order() -> Order {
        Order {
            user: "ada".into(),
            total: 42,
        }
    }

    #[test]
    fn round_trips() {
        let frame = pack("msg-1", &Payload::json(&order()).unwrap());
        let message = unpack(&Registry::new(), "orders.placed", frame).unwrap();
        assert_eq!(message.id(), "msg-1");
        assert_eq!(message.subject(), "orders.placed");
        assert_eq!(message.codec(), "json");
        assert_eq!(message.json::<Order>().unwrap(), order());
    }

    #[test]
    fn matches_the_go_layout() {
        // Pack(streams.JSON, "id", "x") in runtime-go.
        let frame = pack("id", &Payload::json(&"x".to_owned()).unwrap());
        assert_eq!(frame, b"\xB7\x01\x04json\x02id\"x\"");
    }

    #[test]
    fn rejects_a_foreign_payload() {
        let registry = Registry::new();
        for payload in [
            &b"this is not a frame"[..],
            br#"{"id":"1","data":{}}"#,
            b"",
            &[FRAME_MARKER],
        ] {
            assert!(
                unpack(&registry, "s", payload.to_vec()).is_err(),
                "{payload:?}"
            );
        }
    }

    #[test]
    fn rejects_an_unknown_version() {
        let mut frame = pack("msg-1", &Payload::json(&order()).unwrap());
        frame[1] = 0x99;
        let error = unpack(&Registry::new(), "s", frame).unwrap_err();
        assert!(error.to_string().contains("version"), "{error}");
    }

    #[test]
    fn refuses_an_unknown_codec_unless_registered() {
        let frame = pack("msg-1", &Payload::from_parts("madeup", "opaque"));
        let error = unpack(&Registry::new(), "s", frame.clone()).unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)));
        assert!(error.to_string().contains("madeup"));
        assert!(unpack(&Registry::new().with("madeup"), "s", frame).is_ok());
    }

    #[test]
    fn survives_every_truncation() {
        let frame = pack("msg-1", &Payload::json(&order()).unwrap());
        let header = 2 + 1 + 4 + 1 + 5;
        for len in 0..header {
            assert!(
                unpack(&Registry::new(), "s", frame[..len].to_vec()).is_err(),
                "{len}"
            );
        }
    }

    #[test]
    fn headers_ride_in_version_2() {
        let headers: Headers = [("lang", "rust"), ("site", "plant-3")]
            .into_iter()
            .collect();
        let frame = pack_with("msg-1", &Payload::json(&order()).unwrap(), &headers);
        assert_eq!(frame[1], FRAME_VERSION_HEADERS);
        let message = unpack(&Registry::new(), "s", frame).unwrap();
        assert_eq!(message.headers(), &headers);
        assert_eq!(message.json::<Order>().unwrap(), order());
    }

    #[test]
    fn no_headers_stays_version_1_for_go() {
        let payload = Payload::json(&"x".to_owned()).unwrap();
        assert_eq!(
            pack_with("id", &payload, &Headers::new()),
            pack("id", &payload)
        );
        assert!(
            unpack(&Registry::new(), "s", pack("id", &payload))
                .unwrap()
                .headers()
                .is_empty()
        );
    }

    #[test]
    fn a_header_frame_survives_every_truncation() {
        let headers: Headers = [("lang", "rust")].into_iter().collect();
        let frame = pack_with("msg-1", &Payload::json(&order()).unwrap(), &headers);
        let header_end = frame.len() - Payload::json(&order()).unwrap().data().len();
        for len in 0..header_end {
            assert!(
                unpack(&Registry::new(), "s", frame[..len].to_vec()).is_err(),
                "{len}"
            );
        }
    }

    #[test]
    fn varints_match_go() {
        for value in [0, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut out = Vec::new();
            put_uvarint(&mut out, value);
            assert_eq!(uvarint(&out), Some((value, out.len())));
        }
    }
}
