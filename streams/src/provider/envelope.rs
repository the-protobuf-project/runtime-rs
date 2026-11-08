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

use bytes::Bytes;

use crate::{Message, Payload, Registry, Result, StreamError};

/// First byte of every frame.
const FRAME_MARKER: u8 = 0xB7;
/// The layout version this program writes and reads.
const FRAME_VERSION: u8 = 0x01;
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

/// Reads a frame that arrived on `subject`.
///
/// It refuses a payload this contract did not write, a frame from a future
/// version, one without an id, and one naming a codec `registry` lacks. The
/// data is a slice of `frame`, not a copy.
pub fn unpack(registry: &Registry, subject: &str, frame: impl Into<Bytes>) -> Result<Message> {
    let frame: Bytes = frame.into();
    let fail = |reason: String| StreamError::Frame {
        subject: subject.to_owned(),
        reason,
    };
    if frame.len() < 2 || frame[0] != FRAME_MARKER {
        return Err(fail("was not published through this contract".into()));
    }
    if frame[1] != FRAME_VERSION {
        return Err(fail(format!(
            "uses frame version {}, and this program understands {FRAME_VERSION}",
            frame[1]
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
    registry.lookup(name)?;
    Ok(Message::new(id, subject, name, frame.slice(at..)))
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
    fn varints_match_go() {
        for value in [0, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut out = Vec::new();
            put_uvarint(&mut out, value);
            assert_eq!(uvarint(&out), Some((value, out.len())));
        }
    }
}
