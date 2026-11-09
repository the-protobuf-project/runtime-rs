//! [`OpcUaValue`]: OPC UA values as payloads, in OPC UA's own binary encoding.

use std::{io::Cursor, sync::LazyLock};

use opcua::types::{
    BinaryDecodable, BinaryEncodable, ContextOwned, DataValue, DecodingOptions, NamespaceMap,
    Variant,
};

use crate::{Codec, Message, Payload, Result, StreamError};

/// The codec name every OPC UA payload carries: a [`DataValue`] in OPC UA
/// binary encoding.
pub const CODEC_NAME: &str = "opcua.datavalue";

/// OPC UA values as payloads: a [`DataValue`] in OPC UA binary encoding
/// (Part 6, 5.2.2.17), named [`CODEC_NAME`].
///
/// The DataValue, not just its [`Variant`], is what travels, so a value's
/// status code and its source and server timestamps reach the subscriber
/// beside the value. These bytes are exactly a DataValue's encoding on an
/// OPC UA wire, so any OPC UA stack decodes them; nothing of this crate's
/// frame is added.
///
/// `Codec<Variant>` is the shorthand for writing a bare value: it encodes a
/// DataValue holding only the value, and decoding gives back the value of
/// whatever DataValue arrived ([`Variant::Empty`] when it carried none).
///
/// The encoding context is OPC UA's default: built-in types and the core
/// namespace. A structure a server defines travels as an ExtensionObject the
/// receiver decodes with its own type loaders.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpcUaValue;

/// The default encoding context, built once: it is the same for every value.
static CONTEXT: LazyLock<ContextOwned> =
    LazyLock::new(|| ContextOwned::new_default(NamespaceMap::new(), DecodingOptions::default()));

impl OpcUaValue {
    /// A payload holding `value` with no status or timestamps; the server
    /// stamps a write with its own time.
    pub fn payload(value: impl Into<Variant>) -> Payload {
        Self::payload_of(&DataValue::value_only(value.into()))
    }

    /// A payload holding a whole DataValue: value, status, and timestamps.
    pub fn payload_of(value: &DataValue) -> Payload {
        Payload::from_parts(CODEC_NAME, encode(value))
    }

    /// Decodes a delivered message's DataValue, refusing one written by
    /// another codec.
    pub fn data_value(message: &Message) -> Result<DataValue> {
        message.decode(&OpcUaValue)
    }

    /// Decodes a payload's DataValue, refusing one written by another codec.
    pub fn decode_payload(payload: &Payload) -> Result<DataValue> {
        if payload.codec() != CODEC_NAME {
            return Err(StreamError::Unsupported(format!(
                "opcua: a node's value is a typed OPC UA value, and this payload was encoded \
                 with the {:?} codec; encode it with OpcUaValue",
                payload.codec()
            )));
        }
        decode(payload.data())
    }
}

/// A DataValue in OPC UA binary encoding.
pub(crate) fn encode(value: &DataValue) -> Vec<u8> {
    value.encode_to_vec(&CONTEXT.context())
}

/// The DataValue `data` encodes, refusing trailing bytes: a payload is one
/// value, and anything after it means the bytes are not what they claim.
pub(crate) fn decode(data: &[u8]) -> Result<DataValue> {
    let mut cursor = Cursor::new(data);
    let value = DataValue::decode(&mut cursor, &CONTEXT.context()).map_err(|error| {
        StreamError::Codec(format!("opcua: cannot decode an OPC UA DataValue: {error}"))
    })?;
    if cursor.position() as usize != data.len() {
        return Err(StreamError::Codec(format!(
            "opcua: {} bytes follow the DataValue",
            data.len() - cursor.position() as usize
        )));
    }
    Ok(value)
}

impl Codec<DataValue> for OpcUaValue {
    /// The same name for both value types: a frame does not say which one it holds.
    fn name(&self) -> &'static str {
        CODEC_NAME
    }

    /// The whole DataValue in OPC UA binary, timestamps and status included.
    fn encode(&self, value: &DataValue) -> Result<Vec<u8>> {
        Ok(encode(value))
    }

    /// Refuses trailing bytes as well as a malformed DataValue.
    fn decode(&self, data: &[u8]) -> Result<DataValue> {
        decode(data)
    }
}

impl Codec<Variant> for OpcUaValue {
    /// Shares the DataValue codec's name; a Variant travels as a value-only DataValue.
    fn name(&self) -> &'static str {
        CODEC_NAME
    }

    /// Wraps the Variant in a DataValue with no status or timestamps.
    fn encode(&self, value: &Variant) -> Result<Vec<u8>> {
        Ok(encode(&DataValue::value_only(value.clone())))
    }

    /// Keeps only the value; a DataValue without one reads as [`Variant::Empty`].
    fn decode(&self, data: &[u8]) -> Result<Variant> {
        Ok(decode(data)?.value.unwrap_or(Variant::Empty))
    }
}

#[cfg(test)]
mod tests {
    use opcua::types::{DateTime, StatusCode};

    use super::*;

    /// Asserts `value` survives an encode and decode unchanged.
    fn round_trip(value: DataValue) {
        let bytes = Codec::<DataValue>::encode(&OpcUaValue, &value).unwrap();
        let back: DataValue = OpcUaValue.decode(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn round_trips_typed_values() {
        let at = DateTime::now();
        for value in [
            Variant::from(21.5f64),
            Variant::from(-7i32),
            Variant::from(true),
            Variant::from("on"),
            Variant::from(i64::MAX),
            Variant::from(vec![1.0f32, 2.0, 3.0]),
        ] {
            round_trip(DataValue::value_only(value.clone()));
            round_trip(DataValue {
                value: Some(value),
                status: Some(StatusCode::UncertainLastUsableValue),
                source_timestamp: Some(at),
                source_picoseconds: Some(7),
                server_timestamp: Some(at),
                server_picoseconds: None,
            });
        }
    }

    #[test]
    fn is_the_opc_ua_binary_encoding() {
        // Part 6, 5.2.2.17: the encoding mask (0x01, value present), then the
        // Variant: type id 6 (Int32) and the value, little-endian.
        let bytes = Codec::<Variant>::encode(&OpcUaValue, &Variant::from(1i32)).unwrap();
        assert_eq!(bytes, [0x01, 0x06, 0x01, 0x00, 0x00, 0x00]);
        // A Bad status alone: mask 0x02 and the code.
        let bytes = encode(&DataValue {
            status: Some(StatusCode::BadSensorFailure),
            ..DataValue::default()
        });
        assert_eq!(bytes[0], 0x02);
        assert_eq!(
            u32::from_le_bytes(bytes[1..5].try_into().unwrap()),
            StatusCode::BadSensorFailure.bits()
        );
    }

    #[test]
    fn variant_shorthand_reads_any_data_value() {
        let bytes = encode(&DataValue::new_now(3.25f64));
        let value: Variant = OpcUaValue.decode(&bytes).unwrap();
        assert_eq!(value, Variant::from(3.25f64));
        let empty: Variant = OpcUaValue.decode(&encode(&DataValue::null())).unwrap();
        assert_eq!(empty, Variant::Empty);
    }

    #[test]
    fn refuses_garbage_and_trailing_bytes() {
        assert!(decode(&[0x01, 0x06, 0x01]).is_err());
        let mut bytes = encode(&DataValue::value_only(1i32));
        bytes.push(0);
        let error = decode(&bytes).unwrap_err();
        assert!(error.to_string().contains("follow"), "{error}");
        assert!(matches!(error, StreamError::Codec(_)));
    }

    #[test]
    fn a_payload_names_the_codec() {
        let payload = OpcUaValue::payload(42i32);
        assert_eq!(payload.codec(), CODEC_NAME);
        assert_eq!(
            OpcUaValue::decode_payload(&payload).unwrap().value,
            Some(Variant::from(42i32))
        );
        let json = Payload::json(&42).unwrap();
        assert!(matches!(
            OpcUaValue::decode_payload(&json),
            Err(StreamError::Unsupported(_))
        ));
    }

    #[test]
    fn a_message_decodes_only_as_opc_ua() {
        let message = Message::new(
            "id",
            "ns=2;s=x",
            CODEC_NAME,
            encode(&DataValue::value_only(1i32)),
        );
        assert_eq!(
            OpcUaValue::data_value(&message).unwrap().value,
            Some(Variant::from(1i32))
        );
        assert!(message.json::<i32>().is_err());
    }
}
