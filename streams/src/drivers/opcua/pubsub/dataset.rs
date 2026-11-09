//! [`DataSet`]: a whole DataSet as one payload, for publishing every field of
//! a DataSetWriter in one DataSetMessage.

use std::io::Cursor;

use opcua::types::{
    BinaryDecodable, BinaryEncodable, DataSetMetaDataType, DataValue, FieldMetaData, NodeId,
    UAString, VariantTypeId,
};

use crate::{Payload, Result, StreamError};

use super::message::CONTEXT;

/// The codec name of a [`DataSet`] payload.
pub const DATASET_CODEC_NAME: &str = "opcua.dataset";

/// The most fields a DataSet payload may declare, so a corrupt count cannot
/// make a reader allocate without bound.
const MAX_FIELDS: usize = 1 << 16;

/// A DataSet's fields by name, each a [`DataValue`], published as one
/// DataSetMessage.
///
/// As a payload it is OPC UA binary: an Int32 count, then each field's name
/// as a String and its DataValue, the same DataValue bytes
/// [`OpcUaValue`](crate::drivers::opcua::OpcUaValue) writes. It is only the
/// publish side's container; a subscriber receives each field as a message of
/// its own.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DataSet {
    /// The fields, by name, in order.
    pub fields: Vec<(String, DataValue)>,
}

impl DataSet {
    /// An empty DataSet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a field.
    pub fn with(mut self, name: impl Into<String>, value: impl Into<DataValue>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }

    /// The DataSet as a payload, named [`DATASET_CODEC_NAME`].
    pub fn payload(&self) -> Payload {
        let context = CONTEXT.context();
        let mut out = Vec::with_capacity(4 + self.fields.len() * 24);
        out.extend_from_slice(&(self.fields.len() as i32).to_le_bytes());
        for (name, value) in &self.fields {
            out.extend(UAString::from(name.as_str()).encode_to_vec(&context));
            out.extend(value.encode_to_vec(&context));
        }
        Payload::from_parts(DATASET_CODEC_NAME, out)
    }

    /// Decodes a DataSet payload, refusing one written by another codec.
    pub fn decode_payload(payload: &Payload) -> Result<Self> {
        if payload.codec() != DATASET_CODEC_NAME {
            return Err(StreamError::Unsupported(format!(
                "opcua pubsub: a DataSetWriter's subject takes a whole DataSet, and this payload \
                 was encoded with the {:?} codec; build it with DataSet, or publish one field to \
                 \"<writer>/<field>\" with OpcUaValue",
                payload.codec()
            )));
        }
        let data = payload.data();
        let bad = |why: &dyn std::fmt::Display| {
            StreamError::Codec(format!("opcua pubsub: cannot decode a DataSet: {why}"))
        };
        let count = data
            .get(..4)
            .map(|b| i32::from_le_bytes(b.try_into().expect("four bytes")))
            .ok_or_else(|| bad(&"it is truncated"))?;
        let count = usize::try_from(count).map_err(|_| bad(&"it declares a negative count"))?;
        if count > MAX_FIELDS {
            return Err(bad(&format_args!("it declares {count} fields")));
        }
        let context = CONTEXT.context();
        let mut cursor = Cursor::new(&data[4..]);
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let name = UAString::decode(&mut cursor, &context).map_err(|e| bad(&e))?;
            let value = DataValue::decode(&mut cursor, &context).map_err(|e| bad(&e))?;
            fields.push((name.to_string(), value));
        }
        if cursor.position() as usize != data.len() - 4 {
            return Err(bad(&"bytes follow the last field"));
        }
        Ok(Self { fields })
    }

    /// DataSetMetaData describing these fields: each one's name, built-in
    /// type, and whether it is a scalar or an array, read off its value. An
    /// empty value is described as BaseDataType. The ConfigurationVersion is
    /// left zero, for [`publish_metadata`](super::PubSubProvider::publish_metadata)
    /// to stamp.
    pub fn metadata(&self, name: &str) -> DataSetMetaDataType {
        let fields = self
            .fields
            .iter()
            .map(|(field, value)| {
                let (built_in, rank, dims) = match value.value.as_ref().map(|v| v.type_id()) {
                    Some(VariantTypeId::Scalar(t)) => (t as u32, -1, None),
                    Some(VariantTypeId::Array(t, dims)) => (
                        t as u32,
                        dims.map_or(1, |d| d.len() as i32),
                        dims.map(<[u32]>::to_vec),
                    ),
                    _ => (0, -2, None),
                };
                FieldMetaData {
                    name: field.as_str().into(),
                    built_in_type: built_in as u8,
                    // A built-in type's DataType node is its own number in
                    // namespace 0; BaseDataType is 24.
                    data_type: NodeId::new(0, if built_in == 0 { 24 } else { built_in }),
                    value_rank: rank,
                    array_dimensions: dims,
                    ..FieldMetaData::default()
                }
            })
            .collect();
        DataSetMetaDataType {
            name: name.into(),
            fields: Some(fields),
            ..DataSetMetaDataType::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use opcua::types::Variant;

    use super::*;

    #[test]
    fn round_trips() {
        let set = DataSet::new()
            .with("Temperature", DataValue::value_only(21.5f64))
            .with("Mode", DataValue::new_now(Variant::from("auto")));
        let payload = set.payload();
        assert_eq!(payload.codec(), DATASET_CODEC_NAME);
        assert_eq!(DataSet::decode_payload(&payload).unwrap(), set);
    }

    #[test]
    fn describes_its_fields() {
        let meta = DataSet::new()
            .with("Temperature", DataValue::value_only(21.5f64))
            .with("Samples", DataValue::value_only(vec![1i32, 2]))
            .with("Nothing", DataValue::null())
            .metadata("boiler");
        assert_eq!(meta.name.as_ref(), "boiler");
        let fields = meta.fields.unwrap();
        assert_eq!((fields[0].built_in_type, fields[0].value_rank), (11, -1));
        assert_eq!(fields[0].data_type, NodeId::new(0, 11u32));
        assert_eq!((fields[1].built_in_type, fields[1].value_rank), (6, 1));
        assert_eq!(fields[2].data_type, NodeId::new(0, 24u32));
    }

    #[test]
    fn refuses_other_codecs_and_garbage() {
        assert!(matches!(
            DataSet::decode_payload(&Payload::json(&1).unwrap()),
            Err(StreamError::Unsupported(_))
        ));
        for bytes in [&[][..], &[1, 0, 0, 0], &[0xff, 0xff, 0xff, 0xff], &[0, 0, 0, 0, 9]] {
            let payload = Payload::from_parts(DATASET_CODEC_NAME, bytes.to_vec());
            assert!(
                matches!(DataSet::decode_payload(&payload), Err(StreamError::Codec(_))),
                "{bytes:?}"
            );
        }
    }
}
