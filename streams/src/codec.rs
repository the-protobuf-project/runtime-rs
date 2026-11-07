//! Payload codecs.
//!
//! There is no message type on the publish path. A value is encoded by a
//! [`Codec`] into a [`Payload`] that carries the codec's name, and the name
//! travels in the frame so a message decodes the way it was written rather than
//! the way the receiving program happens to be configured.

use std::{borrow::Cow, collections::BTreeSet};

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

use crate::{Result, StreamError};

/// Encodes values of `T` for the wire and decodes them back.
///
/// The name is what goes in the frame. Two codecs with one name are one codec
/// as far as a receiver is concerned, so names must be stable and distinct.
pub trait Codec<T>: Send + Sync {
    /// The name written into every frame this codec produces.
    fn name(&self) -> &'static str;

    /// Encodes a value.
    fn encode(&self, value: &T) -> Result<Vec<u8>>;

    /// Decodes a value from bytes this codec, or a peer with its name, wrote.
    fn decode(&self, data: &[u8]) -> Result<T>;
}

/// JSON through serde. The default codec, and the one a Go peer uses unless
/// configured otherwise.
#[derive(Clone, Copy, Debug, Default)]
pub struct Json;

impl<T: Serialize + DeserializeOwned> Codec<T> for Json {
    fn name(&self) -> &'static str {
        "json"
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value)
            .map_err(|error| StreamError::Codec(format!("cannot encode value as JSON: {error}")))
    }

    fn decode(&self, data: &[u8]) -> Result<T> {
        serde_json::from_slice(data)
            .map_err(|error| StreamError::Codec(format!("cannot decode message as JSON: {error}")))
    }
}

/// Protocol Buffers through prost, wire-compatible with Go's `codec/protobuf`.
#[cfg(feature = "protobuf")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Protobuf;

#[cfg(feature = "protobuf")]
impl<T: prost::Message + Default> Codec<T> for Protobuf {
    fn name(&self) -> &'static str {
        "protobuf"
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        Ok(value.encode_to_vec())
    }

    fn decode(&self, data: &[u8]) -> Result<T> {
        T::decode(data).map_err(|error| {
            StreamError::Codec(format!("cannot decode message as protobuf: {error}"))
        })
    }
}

/// An encoded value and the name of the codec that encoded it.
///
/// Publishers take a Payload rather than a value so the contract stays
/// object-safe: encoding happens at the application boundary, through
/// [`Payload::encode`] or a [`Typed`](crate::Typed) view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    codec: Cow<'static, str>,
    data: Bytes,
}

impl Payload {
    /// Encodes `value` with `codec`.
    pub fn encode<T, C: Codec<T> + ?Sized>(codec: &C, value: &T) -> Result<Self> {
        Ok(Self {
            codec: Cow::Borrowed(codec.name()),
            data: Bytes::from(codec.encode(value)?),
        })
    }

    /// Encodes `value` as JSON.
    pub fn json<T: Serialize + DeserializeOwned>(value: &T) -> Result<Self> {
        Self::encode(&Json, value)
    }

    /// Wraps bytes some other codec already produced.
    pub fn from_parts(codec: impl Into<Cow<'static, str>>, data: impl Into<Bytes>) -> Self {
        Self {
            codec: codec.into(),
            data: data.into(),
        }
    }

    /// The codec name that goes in the frame.
    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// The encoded bytes.
    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

/// The codec names a receiver accepts.
///
/// A frame naming a codec this program does not have is refused with
/// [`StreamError::Unsupported`] when it arrives, rather than handed over as
/// bytes nobody can read. JSON is always present.
#[derive(Clone, Debug)]
pub struct Registry {
    names: BTreeSet<Cow<'static, str>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// A registry accepting JSON only.
    pub fn new() -> Self {
        Self {
            names: BTreeSet::from([Cow::Borrowed("json")]),
        }
    }

    /// Accepts frames written by a codec with this name as well.
    pub fn with(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.names.insert(name.into());
        self
    }

    /// Checks that a frame's codec is one this program can decode.
    pub fn lookup(&self, name: &str) -> Result<()> {
        if self.names.contains(name) {
            return Ok(());
        }
        Err(StreamError::Unsupported(format!(
            "this message was encoded with the {name:?} codec, which this program does not have; \
             register it with Registry::with"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Order {
        user: String,
        total: u32,
    }

    #[test]
    fn json_round_trips() {
        let order = Order {
            user: "ada".into(),
            total: 42,
        };
        let payload = Payload::json(&order).unwrap();
        assert_eq!(payload.codec(), "json");
        let back: Order = Json.decode(payload.data()).unwrap();
        assert_eq!(back, order);
    }

    #[test]
    fn registry_refuses_an_unknown_codec_by_name() {
        let error = Registry::new().lookup("madeup").unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)));
        assert!(error.to_string().contains("madeup"));
        assert!(Registry::new().with("madeup").lookup("madeup").is_ok());
    }
}
