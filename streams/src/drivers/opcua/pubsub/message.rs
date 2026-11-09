//! OPC UA PubSub JSON messages (Part 14, 7.2.5): the NetworkMessage, its
//! DataSetMessages, and the DataSetMetaData announcement.
//!
//! The envelope is written field by field, absent fields omitted as Part 14
//! asks. Every DataValue in a Payload is Part 6's JSON encoding (5.4.2.18),
//! its Variant through async-opcua: the reversible form, `{"Value":{"Type":11,"Body":21.5},
//! "SourceTimestamp":"..."}`, which keeps a value's built-in type across the
//! wire.
//!
//! Reading is more forgiving than writing, because other publishers choose
//! other forms: a Payload field may be a DataValue, a bare Variant, the
//! 1.05 forms that carry the type as `UaType`, or a plain JSON value from a
//! non-reversible encoder, whose type is then inferred (an integer is Int64,
//! a fraction Double, text String). A NetworkMessage may also arrive without
//! its header, as one DataSetMessage or an array of them.

use std::{
    io::{Cursor, Read, Write},
    sync::LazyLock,
};

use opcua::types::{
    ConfigurationVersionDataType, ContextOwned, DataSetMetaDataType, DataValue, DateTime,
    DecodingOptions, NamespaceMap, StatusCode,
    json::{
        JsonDecodable, JsonEncodable, JsonReader, JsonStreamReader, JsonStreamWriter, JsonWriter,
        ValueType, consume_raw_value, from_bytes,
    },
};
use serde_json::{Map, Value, json};

use crate::{Result, StreamError};

/// The MessageType of a NetworkMessage carrying data.
pub const MESSAGE_TYPE_DATA: &str = "ua-data";
/// The MessageType of a DataSetMetaData announcement.
pub const MESSAGE_TYPE_METADATA: &str = "ua-metadata";
/// The DataSetMessage type of a keep-alive, which carries no Payload.
pub const MESSAGE_TYPE_KEEPALIVE: &str = "ua-keepalive";

/// The encoding context JSON values are written and read in: built-in types
/// and the core namespace, as [`OpcUaValue`](super::super::OpcUaValue) uses.
pub(crate) static CONTEXT: LazyLock<ContextOwned> =
    LazyLock::new(|| ContextOwned::new_default(NamespaceMap::new(), DecodingOptions::default()));

type Failure = Box<dyn std::error::Error + Send + Sync>;
type Outcome<T> = std::result::Result<T, Failure>;
type Writer<'a> = JsonStreamWriter<&'a mut dyn Write>;
type Reader<'a> = JsonStreamReader<&'a mut dyn Read>;

/// A Part 14 JSON NetworkMessage with MessageType `ua-data`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NetworkMessage {
    /// Identifies the message; the id a publish returns.
    pub message_id: Option<String>,
    /// The publisher that sent it.
    pub publisher_id: Option<String>,
    /// The WriterGroup it was sent for.
    pub writer_group_name: Option<String>,
    /// The DataSetClass every DataSetMessage in it belongs to, if one.
    pub data_set_class_id: Option<String>,
    /// The DataSetMessages, in the order they were written.
    pub messages: Vec<DataSetMessage>,
}

/// One DataSetMessage: a DataSetWriter's values at one moment.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DataSetMessage {
    /// The writer's numeric id within its publisher.
    pub data_set_writer_id: Option<u16>,
    /// The writer's name.
    pub data_set_writer_name: Option<String>,
    /// The publisher, when the NetworkMessage header does not carry it.
    pub publisher_id: Option<String>,
    /// The WriterGroup, when the NetworkMessage header does not carry it.
    pub writer_group_name: Option<String>,
    /// Counts the writer's messages, so a reader can tell when it missed one.
    pub sequence_number: Option<u32>,
    /// The version of the DataSetMetaData the Payload follows.
    pub meta_data_version: Option<ConfigurationVersionDataType>,
    /// When the message was written.
    pub timestamp: Option<DateTime>,
    /// The status of the DataSet as a whole.
    pub status: Option<StatusCode>,
    /// `ua-keyframe`, `ua-deltaframe`, `ua-event` or `ua-keepalive`; absent
    /// means a key frame.
    pub message_type: Option<String>,
    /// The DataSet's fields, by name, in order.
    pub payload: Vec<(String, DataValue)>,
}

/// A Part 14 JSON DataSetMetaData message (MessageType `ua-metadata`): the
/// names, types and version of a writer's fields.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetaDataMessage {
    /// Identifies the message.
    pub message_id: Option<String>,
    /// The publisher the metadata describes.
    pub publisher_id: Option<String>,
    /// The writer's numeric id.
    pub data_set_writer_id: Option<u16>,
    /// The writer's WriterGroup.
    pub writer_group_name: Option<String>,
    /// The writer's name.
    pub data_set_writer_name: Option<String>,
    /// When the metadata was sent.
    pub timestamp: Option<DateTime>,
    /// The metadata itself.
    pub meta_data: DataSetMetaDataType,
}

/// Every encode and decode failure in this module is a [`StreamError::Codec`].
fn codec_error(what: &str, error: impl std::fmt::Display) -> StreamError {
    StreamError::Codec(format!("opcua pubsub: cannot {what}: {error}"))
}

/// Writes a document with `body` and returns its bytes.
fn document(body: impl FnOnce(&mut Writer<'_>) -> Outcome<()>) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(256);
    {
        let mut writer = JsonStreamWriter::new(&mut out as &mut dyn Write);
        body(&mut writer).map_err(|e| codec_error("encode a JSON message", e))?;
        writer
            .finish_document()
            .map_err(|e| codec_error("encode a JSON message", e))?;
    }
    Ok(out)
}

/// Reads a document with `body`, refusing anything after it.
fn read_document<T>(data: &[u8], body: impl FnOnce(&mut Reader<'_>) -> Outcome<T>) -> Result<T> {
    let mut cursor = Cursor::new(data);
    let mut reader = JsonStreamReader::new(&mut cursor as &mut dyn Read);
    let value = body(&mut reader).map_err(|e| codec_error("decode a JSON message", e))?;
    reader
        .consume_trailing_whitespace()
        .map_err(|e| codec_error("decode a JSON message", e))?;
    Ok(value)
}

/// Writes `name: value` only when set; Part 14 omits absent fields rather than nulling them.
fn put_str(writer: &mut Writer<'_>, name: &str, value: &Option<String>) -> Outcome<()> {
    if let Some(value) = value {
        writer.name(name)?;
        writer.string_value(value)?;
    }
    Ok(())
}

/// Writes `name` with async-opcua's JSON encoding, omitting it when unset.
fn put<T: JsonEncodable>(writer: &mut Writer<'_>, name: &str, value: &Option<T>) -> Outcome<()> {
    if let Some(value) = value {
        writer.name(name)?;
        value.encode(writer, &CONTEXT.context())?;
    }
    Ok(())
}

/// A DateTime in Part 6's JSON form, to the 100 ns an OPC UA DateTime
/// holds. async-opcua's own JSON writes whole milliseconds, which would make
/// a timestamp read back differ from the one written.
fn put_time(writer: &mut Writer<'_>, name: &str, value: &Option<DateTime>) -> Outcome<()> {
    if let Some(value) = value {
        writer.name(name)?;
        writer.string_value(
            &value
                .as_chrono()
                .format("%Y-%m-%dT%H:%M:%S%.fZ")
                .to_string(),
        )?;
    }
    Ok(())
}

/// A DataValue in Part 6's reversible JSON form (5.4.2.18), its Variant
/// encoded by async-opcua and its timestamps by [`put_time`].
fn put_data_value(writer: &mut Writer<'_>, value: &DataValue) -> Outcome<()> {
    writer.begin_object()?;
    if let Some(variant) = &value.value
        && !matches!(variant, opcua::types::Variant::Empty)
    {
        writer.name("Value")?;
        variant.encode(writer, &CONTEXT.context())?;
    }
    if let Some(status) = value.status {
        writer.name("Status")?;
        writer.number_value(status.bits())?;
    }
    put_time(writer, "SourceTimestamp", &value.source_timestamp)?;
    if let Some(pico) = value.source_picoseconds {
        writer.name("SourcePicoseconds")?;
        writer.number_value(pico)?;
    }
    put_time(writer, "ServerTimestamp", &value.server_timestamp)?;
    if let Some(pico) = value.server_picoseconds {
        writer.name("ServerPicoseconds")?;
        writer.number_value(pico)?;
    }
    writer.end_object()?;
    Ok(())
}

impl NetworkMessage {
    /// Encodes the message as Part 14 JSON.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        document(|writer| self.write(writer))
    }

    /// The NetworkMessage header and its DataSetMessages; MessageType is always `ua-data`.
    fn write(&self, writer: &mut Writer<'_>) -> Outcome<()> {
        writer.begin_object()?;
        put_str(writer, "MessageId", &self.message_id)?;
        writer.name("MessageType")?;
        writer.string_value(MESSAGE_TYPE_DATA)?;
        put_str(writer, "PublisherId", &self.publisher_id)?;
        put_str(writer, "WriterGroupName", &self.writer_group_name)?;
        put_str(writer, "DataSetClassId", &self.data_set_class_id)?;
        writer.name("Messages")?;
        writer.begin_array()?;
        for message in &self.messages {
            message.write(writer)?;
        }
        writer.end_array()?;
        writer.end_object()?;
        Ok(())
    }

    /// Decodes a Part 14 JSON NetworkMessage, or a header-less DataSetMessage
    /// or array of them. A message of another MessageType, such as
    /// `ua-metadata`, is refused.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        read_document(data, read_network)
    }

    /// The publisher of one of its DataSetMessages: the message's own, or
    /// the header's.
    pub fn publisher_of<'a>(&'a self, message: &'a DataSetMessage) -> Option<&'a str> {
        message
            .publisher_id
            .as_deref()
            .or(self.publisher_id.as_deref())
    }
}

impl DataSetMessage {
    /// One DataSetMessage object, writing only the fields that are set.
    fn write(&self, writer: &mut Writer<'_>) -> Outcome<()> {
        writer.begin_object()?;
        if let Some(id) = self.data_set_writer_id {
            writer.name("DataSetWriterId")?;
            writer.number_value(id)?;
        }
        put_str(writer, "DataSetWriterName", &self.data_set_writer_name)?;
        put_str(writer, "PublisherId", &self.publisher_id)?;
        put_str(writer, "WriterGroupName", &self.writer_group_name)?;
        if let Some(sequence) = self.sequence_number {
            writer.name("SequenceNumber")?;
            writer.number_value(sequence)?;
        }
        put(writer, "MetaDataVersion", &self.meta_data_version)?;
        put_time(writer, "Timestamp", &self.timestamp)?;
        if let Some(status) = self.status {
            writer.name("Status")?;
            writer.number_value(status.bits())?;
        }
        put_str(writer, "MessageType", &self.message_type)?;
        writer.name("Payload")?;
        writer.begin_object()?;
        for (name, value) in &self.payload {
            writer.name(name)?;
            put_data_value(writer, value)?;
        }
        writer.end_object()?;
        writer.end_object()?;
        Ok(())
    }

    /// Whether this is a keep-alive rather than data.
    pub fn is_keepalive(&self) -> bool {
        self.message_type.as_deref() == Some(MESSAGE_TYPE_KEEPALIVE)
    }
}

impl MetaDataMessage {
    /// Encodes the message as Part 14 JSON.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        document(|writer| {
            writer.begin_object()?;
            put_str(writer, "MessageId", &self.message_id)?;
            writer.name("MessageType")?;
            writer.string_value(MESSAGE_TYPE_METADATA)?;
            put_str(writer, "PublisherId", &self.publisher_id)?;
            if let Some(id) = self.data_set_writer_id {
                writer.name("DataSetWriterId")?;
                writer.number_value(id)?;
            }
            put_str(writer, "WriterGroupName", &self.writer_group_name)?;
            put_str(writer, "DataSetWriterName", &self.data_set_writer_name)?;
            put_time(writer, "Timestamp", &self.timestamp)?;
            writer.name("MetaData")?;
            self.meta_data.encode(writer, &CONTEXT.context())?;
            writer.end_object()?;
            Ok(())
        })
    }

    /// Decodes a Part 14 JSON DataSetMetaData message.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        read_document(data, |reader| {
            let context = CONTEXT.context();
            let mut out = MetaDataMessage::default();
            let mut kind = None;
            let mut meta = None;
            reader.begin_object()?;
            while reader.has_next()? {
                let name = reader.next_name_owned()?;
                match name.as_str() {
                    "MessageId" => out.message_id = text(reader)?,
                    "MessageType" => kind = text(reader)?,
                    "PublisherId" => out.publisher_id = text(reader)?,
                    "DataSetWriterId" => out.data_set_writer_id = number(reader)?,
                    "WriterGroupName" | "DataSetWriterGroup" => {
                        out.writer_group_name = text(reader)?
                    }
                    "DataSetWriterName" => out.data_set_writer_name = text(reader)?,
                    "Timestamp" => out.timestamp = timestamp(reader)?,
                    "MetaData" => {
                        // Non-reversible encoders write a LocalizedText as
                        // its text alone; async-opcua reads the object form.
                        let mut raw: Value = serde_json::from_slice(&consume_raw_value(reader)?)?;
                        localized_texts(&mut raw);
                        meta = Some(from_bytes::<DataSetMetaDataType>(
                            &serde_json::to_vec(&raw)?,
                            &context,
                        )?);
                    }
                    _ => reader.skip_value()?,
                }
            }
            reader.end_object()?;
            if kind.as_deref() != Some(MESSAGE_TYPE_METADATA) {
                return Err(
                    format!("the MessageType is {kind:?}, not {MESSAGE_TYPE_METADATA:?}").into(),
                );
            }
            out.meta_data = meta.ok_or("the message carries no MetaData")?;
            Ok(out)
        })
    }
}

/// Rewrites every `Description` or `DisplayName` given as a bare string as
/// the LocalizedText object `{"Text": ...}`.
fn localized_texts(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                if matches!(key.as_str(), "Description" | "DisplayName")
                    && let Value::String(text) = item
                {
                    *item = json!({ "Text": std::mem::take(text) });
                } else {
                    localized_texts(item);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(localized_texts),
        _ => {}
    }
}

/// A string, or a number written as one (some publishers send a numeric
/// PublisherId), or nothing.
fn text(reader: &mut Reader<'_>) -> Outcome<Option<String>> {
    Ok(match reader.peek()? {
        ValueType::Null => {
            reader.next_null()?;
            None
        }
        ValueType::Number => Some(reader.next_number_as_string()?),
        _ => Some(reader.next_string()?),
    })
}

/// A number given as JSON number or as string (Part 6 writes 64-bit ones as
/// strings); null is `None`.
fn number<T>(reader: &mut Reader<'_>) -> Outcome<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    Ok(match reader.peek()? {
        ValueType::Null => {
            reader.next_null()?;
            None
        }
        ValueType::String => Some(reader.next_str()?.parse()?),
        _ => Some(reader.next_number::<T>()??),
    })
}

/// An RFC 3339 string; any other JSON type is skipped as absent.
fn timestamp(reader: &mut Reader<'_>) -> Outcome<Option<DateTime>> {
    match reader.peek()? {
        ValueType::String => Ok(Some(
            DateTime::parse_from_rfc3339(reader.next_str()?).map_err(|e| e.to_string())?,
        )),
        _ => {
            reader.skip_value()?;
            Ok(None)
        }
    }
}

/// A StatusCode as a number (1.04) or as `{"Code": n, "Symbol": ...}` (1.05).
fn status(reader: &mut Reader<'_>) -> Outcome<Option<StatusCode>> {
    match reader.peek()? {
        ValueType::Number => Ok(Some(StatusCode::from(reader.next_number::<u32>()??))),
        ValueType::Object => {
            let mut code = None;
            reader.begin_object()?;
            while reader.has_next()? {
                if reader.next_name()? == "Code" {
                    code = Some(StatusCode::from(reader.next_number::<u32>()??));
                } else {
                    reader.skip_value()?;
                }
            }
            reader.end_object()?;
            Ok(Some(code.unwrap_or(StatusCode::Good)))
        }
        _ => {
            reader.skip_value()?;
            Ok(None)
        }
    }
}

/// Accepts a NetworkMessage, a header-less DataSetMessage, or an array of them,
/// refusing any MessageType other than data.
fn read_network(reader: &mut Reader<'_>) -> Outcome<NetworkMessage> {
    let mut network = NetworkMessage::default();
    match reader.peek()? {
        ValueType::Array => {
            network.messages = read_messages(reader)?;
            Ok(network)
        }
        ValueType::Object => {
            // A NetworkMessage, or a DataSetMessage sent without one: the
            // fields of both are read, and which it was decided at the end.
            let mut lone = DataSetMessage::default();
            let mut lone_fields = false;
            let mut kind = None;
            reader.begin_object()?;
            while reader.has_next()? {
                let name = reader.next_name_owned()?;
                match name.as_str() {
                    "MessageId" => network.message_id = text(reader)?,
                    "MessageType" => kind = text(reader)?,
                    "PublisherId" => network.publisher_id = text(reader)?,
                    // OPC Publisher writes the WriterGroup as DataSetWriterGroup.
                    "WriterGroupName" | "DataSetWriterGroup" => {
                        network.writer_group_name = text(reader)?
                    }
                    "DataSetClassId" => network.data_set_class_id = text(reader)?,
                    "Messages" => network.messages = read_messages(reader)?,
                    other => {
                        if read_message_field(reader, &mut lone, other)? {
                            lone_fields = true;
                        } else {
                            reader.skip_value()?;
                        }
                    }
                }
            }
            reader.end_object()?;
            match kind.as_deref() {
                None | Some(MESSAGE_TYPE_DATA) => {}
                Some("ua-keyframe" | "ua-deltaframe" | "ua-event" | MESSAGE_TYPE_KEEPALIVE)
                    if lone_fields =>
                {
                    lone.message_type = kind.clone();
                }
                Some(other) => {
                    return Err(
                        format!("this is a {other:?} message, not {MESSAGE_TYPE_DATA:?}").into(),
                    );
                }
            }
            if network.messages.is_empty() && lone_fields {
                lone.publisher_id = lone.publisher_id.or(network.publisher_id.clone());
                lone.writer_group_name =
                    lone.writer_group_name.or(network.writer_group_name.clone());
                network.messages.push(lone);
            }
            Ok(network)
        }
        other => Err(format!("a NetworkMessage is a JSON object, not {other:?}").into()),
    }
}

/// A `Messages` value: an array, a lone object, or anything else read as none.
fn read_messages(reader: &mut Reader<'_>) -> Outcome<Vec<DataSetMessage>> {
    match reader.peek()? {
        ValueType::Array => {
            let mut messages = Vec::new();
            reader.begin_array()?;
            while reader.has_next()? {
                messages.push(read_message(reader)?);
            }
            reader.end_array()?;
            Ok(messages)
        }
        ValueType::Object => Ok(vec![read_message(reader)?]),
        _ => {
            reader.skip_value()?;
            Ok(Vec::new())
        }
    }
}

/// One DataSetMessage object, skipping fields this crate does not read.
fn read_message(reader: &mut Reader<'_>) -> Outcome<DataSetMessage> {
    let mut message = DataSetMessage::default();
    reader.begin_object()?;
    while reader.has_next()? {
        let name = reader.next_name_owned()?;
        if !read_message_field(reader, &mut message, &name)? {
            reader.skip_value()?;
        }
    }
    reader.end_object()?;
    Ok(message)
}

/// Reads one DataSetMessage field into `message`, or returns false, leaving
/// the value unread, when `name` is not one.
fn read_message_field(
    reader: &mut Reader<'_>,
    message: &mut DataSetMessage,
    name: &str,
) -> Outcome<bool> {
    match name {
        "DataSetWriterId" => message.data_set_writer_id = number(reader)?,
        "DataSetWriterName" => message.data_set_writer_name = text(reader)?,
        "SequenceNumber" => message.sequence_number = number(reader)?,
        "MetaDataVersion" => {
            message.meta_data_version =
                Option::<ConfigurationVersionDataType>::decode(reader, &CONTEXT.context())?
        }
        "Timestamp" => message.timestamp = timestamp(reader)?,
        "Status" => message.status = status(reader)?,
        "MessageType" => message.message_type = text(reader)?,
        "Payload" => message.payload = read_payload(reader)?,
        // Read only where a DataSetMessage travels without a header; inside
        // one, the header's own fields are what the caller looks at.
        "PublisherId" => message.publisher_id = text(reader)?,
        "WriterGroupName" | "DataSetWriterGroup" => message.writer_group_name = text(reader)?,
        _ => return Ok(false),
    }
    Ok(true)
}

/// The Payload object's fields in the order written, each read as a DataValue.
fn read_payload(reader: &mut Reader<'_>) -> Outcome<Vec<(String, DataValue)>> {
    if reader.peek()? != ValueType::Object {
        return Err("a Payload is a JSON object of fields by name".into());
    }
    let mut fields = Vec::new();
    reader.begin_object()?;
    while reader.has_next()? {
        let name = reader.next_name_owned()?;
        let raw = consume_raw_value(reader)?;
        let value = field_value(&raw).map_err(|e| format!("field {name:?}: {e}"))?;
        fields.push((name, value));
    }
    reader.end_object()?;
    Ok(fields)
}

/// One Payload field as a DataValue, in whichever form it was written.
pub(crate) fn field_value(raw: &[u8]) -> Outcome<DataValue> {
    let context = CONTEXT.context();
    // The forms async-opcua reads as they are, this crate's own among them;
    // `raw` was rewritten compactly, so the prefix is exact.
    if raw.starts_with(br#"{"Value":{"Type":"#) || raw == b"{}" {
        return Ok(from_bytes::<DataValue>(raw, &context)?);
    }
    if raw.starts_with(br#"{"Type":"#) {
        return Ok(DataValue::value_only(from_bytes::<opcua::types::Variant>(
            raw, &context,
        )?));
    }
    let normal = normalize(serde_json::from_slice(raw)?)?;
    Ok(from_bytes::<DataValue>(
        &serde_json::to_vec(&normal)?,
        &context,
    )?)
}

/// Rewrites a field in any of the forms [`field_value`] reads as a reversible
/// 1.04 DataValue, which async-opcua decodes.
fn normalize(value: Value) -> Outcome<Value> {
    const KEYS: [&str; 7] = [
        "Value",
        "Status",
        "StatusCode",
        "SourceTimestamp",
        "SourcePicoseconds",
        "ServerTimestamp",
        "ServerPicoseconds",
    ];
    let Value::Object(map) = value else {
        return Ok(json!({ "Value": infer(&value)? }));
    };
    let mut out = Map::new();
    if let Some(ty) = map.get("UaType") {
        // 1.05: the Variant's type and value inline, beside the DataValue's.
        let mut variant = Map::new();
        variant.insert("Type".into(), ty.clone());
        variant.insert(
            "Body".into(),
            map.get("Value").cloned().unwrap_or(Value::Null),
        );
        if let Some(dims) = map.get("Dimensions") {
            variant.insert("Dimensions".into(), dims.clone());
        }
        out.insert("Value".into(), Value::Object(variant));
    } else if map.contains_key("Type") && !map.contains_key("Value") {
        return Ok(json!({ "Value": Value::Object(map) }));
    } else if !KEYS.iter().any(|key| map.contains_key(*key)) {
        return Err("the field is a structure, not a DataValue or Variant".into());
    } else if let Some(inner) = map.get("Value") {
        let variant = match inner {
            Value::Object(v) if v.contains_key("UaType") => json!({
                "Type": v["UaType"],
                "Body": v.get("Value").cloned().unwrap_or(Value::Null),
            }),
            Value::Object(v) if v.contains_key("Type") => inner.clone(),
            other => infer(other)?,
        };
        out.insert("Value".into(), variant);
    }
    let code = map.get("Status").or_else(|| map.get("StatusCode"));
    match code {
        Some(Value::Object(status)) => {
            out.insert(
                "Status".into(),
                status.get("Code").cloned().unwrap_or(json!(0)),
            );
        }
        Some(number @ Value::Number(_)) => {
            out.insert("Status".into(), number.clone());
        }
        _ => {}
    }
    for key in &KEYS[3..] {
        if let Some(value) = map.get(*key) {
            out.insert((*key).into(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

/// A reversible Variant for a plain JSON value from a non-reversible encoder:
/// Boolean, Int64 (UInt64 past its range), Double, or String, and arrays of
/// one of them.
fn infer(value: &Value) -> Outcome<Value> {
    /// The reversible type id (Part 6 built-in type) a scalar JSON value maps to.
    fn type_of(value: &Value) -> Outcome<u32> {
        Ok(match value {
            Value::Bool(_) => 1,
            Value::Number(n) if n.is_i64() => 8,
            Value::Number(n) if n.is_u64() => 9,
            Value::Number(_) => 11,
            Value::String(_) => 12,
            _ => return Err("a nested array or structure has no inferable type".into()),
        })
    }
    Ok(match value {
        Value::Null => Value::Null,
        Value::Array(items) => {
            let mut ty = 0;
            for item in items {
                let this = type_of(item)?;
                ty = match (ty, this) {
                    (0, t) => t,
                    (a, b) if a == b => a,
                    // Integers and fractions in one array are all Double.
                    (8 | 9 | 11, 8 | 9 | 11) => 11,
                    _ => return Err("an array mixes types".into()),
                };
            }
            json!({ "Type": if ty == 0 { 12 } else { ty }, "Body": items })
        }
        other => json!({ "Type": type_of(other)?, "Body": other }),
    })
}

#[cfg(test)]
mod tests {
    use opcua::types::{FieldMetaData, Variant};

    use super::*;

    /// A NetworkMessage with two DataSetMessages, for the encode and decode tests.
    fn sample() -> NetworkMessage {
        let at = DateTime::parse_from_rfc3339("2026-11-09T10:00:00Z").unwrap();
        NetworkMessage {
            message_id: Some("01JMSG".into()),
            publisher_id: Some("plant-1".into()),
            writer_group_name: Some("line-a".into()),
            data_set_class_id: None,
            messages: vec![
                DataSetMessage {
                    data_set_writer_id: Some(7),
                    data_set_writer_name: Some("boiler".into()),
                    sequence_number: Some(41),
                    timestamp: Some(at),
                    meta_data_version: Some(ConfigurationVersionDataType {
                        major_version: 3,
                        minor_version: 4,
                    }),
                    payload: vec![
                        ("Temperature".into(), DataValue::value_only(21.5f64)),
                        (
                            "Running".into(),
                            DataValue {
                                value: Some(Variant::from(true)),
                                status: Some(StatusCode::UncertainLastUsableValue),
                                source_timestamp: Some(at),
                                ..DataValue::default()
                            },
                        ),
                    ],
                    ..DataSetMessage::default()
                },
                DataSetMessage {
                    data_set_writer_id: Some(7),
                    data_set_writer_name: Some("boiler".into()),
                    sequence_number: Some(42),
                    payload: vec![("Count".into(), DataValue::value_only(-3i32))],
                    ..DataSetMessage::default()
                },
            ],
        }
    }

    #[test]
    fn round_trips_a_batch() {
        let message = sample();
        let json = message.to_json().unwrap();
        assert_eq!(NetworkMessage::from_json(&json).unwrap(), message);
    }

    #[test]
    fn writes_part_14_field_names() {
        let json: Value = serde_json::from_slice(&sample().to_json().unwrap()).unwrap();
        assert_eq!(json["MessageType"], "ua-data");
        assert_eq!(json["MessageId"], "01JMSG");
        assert_eq!(json["PublisherId"], "plant-1");
        assert_eq!(json["WriterGroupName"], "line-a");
        assert!(
            json.get("DataSetClassId").is_none(),
            "absent fields are omitted"
        );
        let first = &json["Messages"][0];
        assert_eq!(first["DataSetWriterId"], 7);
        assert_eq!(first["DataSetWriterName"], "boiler");
        assert_eq!(first["SequenceNumber"], 41);
        assert_eq!(first["Timestamp"], "2026-11-09T10:00:00Z");
        assert_eq!(first["MetaDataVersion"]["MajorVersion"], 3);
        // A DataValue in Part 6's reversible form: the Variant keeps its type.
        assert_eq!(
            first["Payload"]["Temperature"],
            json!({"Value": {"Type": 11, "Body": 21.5}})
        );
        assert_eq!(first["Payload"]["Running"]["Value"]["Type"], 1);
        assert_eq!(
            first["Payload"]["Running"]["Status"],
            StatusCode::UncertainLastUsableValue.bits()
        );
        assert_eq!(json["Messages"][1]["SequenceNumber"], 42);
        assert_eq!(
            json["Messages"][1]["Payload"]["Count"]["Value"],
            json!({"Type": 6, "Body": -3})
        );
    }

    #[test]
    fn reads_foreign_field_forms() {
        let cases: [(&str, Variant); 7] = [
            // A bare 1.04 Variant.
            (r#"{"Type":10,"Body":1.5}"#, Variant::from(1.5f32)),
            // 1.05, the Variant inline.
            (r#"{"UaType":7,"Value":12}"#, Variant::from(12u32)),
            // A 1.04 non-reversible DataValue.
            (
                r#"{"Value":21,"SourceTimestamp":"2026-01-01T00:00:00Z"}"#,
                Variant::from(21i64),
            ),
            // Plain values, typed by inference.
            ("3.25", Variant::from(3.25f64)),
            (r#""on""#, Variant::from("on")),
            ("true", Variant::from(true)),
            ("[1,2]", Variant::from(vec![1i64, 2])),
        ];
        for (raw, want) in cases {
            let value = field_value(raw.as_bytes()).unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(value.value, Some(want), "{raw}");
        }
        let value = field_value(br#"{"Value":1,"StatusCode":{"Code":2149842944}}"#).unwrap();
        assert_eq!(value.status, Some(StatusCode::from(2149842944u32)));
        assert!(field_value(br#"{"x":1}"#).is_err());
    }

    #[test]
    fn reads_a_message_without_its_header() {
        let lone =
            br#"{"DataSetWriterId":3,"SequenceNumber":9,"Payload":{"a":{"Type":6,"Body":1}}}"#;
        let message = NetworkMessage::from_json(lone).unwrap();
        assert_eq!(message.messages.len(), 1);
        assert_eq!(message.messages[0].sequence_number, Some(9));
        assert_eq!(message.messages[0].data_set_writer_id, Some(3));
        let array = br#"[{"SequenceNumber":1,"Payload":{}},{"SequenceNumber":2,"Payload":{}}]"#;
        assert_eq!(NetworkMessage::from_json(array).unwrap().messages.len(), 2);
        // A single DataSetMessage object under Messages, and a numeric
        // PublisherId.
        let single = br#"{"MessageType":"ua-data","PublisherId":12,"Messages":{"Payload":{}}}"#;
        let message = NetworkMessage::from_json(single).unwrap();
        assert_eq!(message.publisher_id.as_deref(), Some("12"));
        assert_eq!(message.messages.len(), 1);
    }

    #[test]
    fn refuses_what_is_not_data() {
        for raw in [
            &br#"{"MessageType":"ua-metadata","MetaData":{}}"#[..],
            b"42",
            b"{\"Messages\":[]} trailing",
            br#"{"Messages":[{"Payload":[1]}]}"#,
        ] {
            assert!(
                matches!(NetworkMessage::from_json(raw), Err(StreamError::Codec(_))),
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// Captured from Microsoft OPC Publisher 2.9.15 (`--strict --mm=PubSub
    /// --me=JsonReversible`) reading opc-plc, as docker/compose.opcua.pubsub.yaml
    /// runs it.
    const OPC_PUBLISHER_DATA: &str = r#"{"MessageId":"9c484127-94a8-4977-8293-6f464b4460da","MessageType":"ua-data","PublisherId":"opc-publisher","DataSetWriterGroup":"plc","Messages":[{"SequenceNumber":49,"MetaDataVersion":{"MajorVersion":1,"MinorVersion":1143887952},"Timestamp":"2026-11-09T05:14:25.8619034Z","MessageType":"ua-deltaframe","DataSetWriterName":"simulation","Payload":{"StepUp":{"Value":{"Type":7,"Body":275},"SourceTimestamp":"2026-11-09T05:14:25.798419Z","ServerTimestamp":"2026-11-09T05:14:25.7984391Z"},"RandomSignedInt32":{"Value":{"Type":6,"Body":-670934554},"SourceTimestamp":"2026-11-09T05:14:25.7984578Z","ServerTimestamp":"2026-11-09T05:14:25.7984703Z"}}}]}"#;
    const OPC_PUBLISHER_METADATA: &str = r#"{"MessageId":"40e1afec-1bbd-4fa4-8765-db9d4e943b3b","MessageType":"ua-metadata","PublisherId":"opc-publisher","DataSetWriterGroup":"plc","MetaData":{"Namespaces":["http://opcfoundation.org/UA/","urn:OpcPlc:opc-plc","http://opcfoundation.org/UA/DI/","http://microsoft.com/Opc/OpcPlc/","http://microsoft.com/Opc/OpcPlc/Boiler","http://microsoft.com/Opc/OpcPlc/BoilerInstance","http://microsoft.com/Opc/OpcPlc/ReferenceTest","http://opcfoundation.org/UA/Diagnostics"],"StructureDataTypes":[],"EnumDataTypes":[],"SimpleDataTypes":[],"Fields":[{"Name":"StepUp","Description":"Constantly increasing value","BuiltInType":7,"DataType":{"Id":7},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"2334228f-4e90-46e5-99c4-ccba6ab0bf88","Properties":[]},{"Name":"AlternatingBoolean","Description":"Alternating boolean value","BuiltInType":1,"DataType":{"Id":1},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"cb6461e9-4df5-4c4a-ac2f-6919b8514f4e","Properties":[]},{"Name":"RandomSignedInt32","Description":"Random signed 32 bit integer value","BuiltInType":6,"DataType":{"Id":6},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"c36fbf3f-393e-4258-8b0d-4909cf4fbf92","Properties":[]}],"ConfigurationVersion":{"MajorVersion":1,"MinorVersion":1143887952}},"DataSetWriterName":"simulation"}"#;

    #[test]
    fn reads_opc_publisher() {
        let network = NetworkMessage::from_json(OPC_PUBLISHER_DATA.as_bytes()).unwrap();
        assert_eq!(network.publisher_id.as_deref(), Some("opc-publisher"));
        assert_eq!(network.writer_group_name.as_deref(), Some("plc"));
        let message = &network.messages[0];
        assert_eq!(message.data_set_writer_name.as_deref(), Some("simulation"));
        assert_eq!(message.sequence_number, Some(49));
        assert_eq!(message.message_type.as_deref(), Some("ua-deltaframe"));
        assert_eq!(message.meta_data_version.as_ref().unwrap().major_version, 1);
        assert_eq!(message.payload[0].0, "StepUp");
        assert_eq!(message.payload[0].1.value, Some(Variant::from(275u32)));
        // Seven decimal places of a .NET DateTime survive.
        let at = message.payload[0].1.server_timestamp.unwrap();
        assert_eq!(at.ticks() % 10_000_000, 7_984_391);
        assert_eq!(
            message.payload[1].1.value,
            Some(Variant::from(-670934554i32))
        );

        let meta = MetaDataMessage::from_json(OPC_PUBLISHER_METADATA.as_bytes()).unwrap();
        assert_eq!(meta.writer_group_name.as_deref(), Some("plc"));
        assert_eq!(meta.data_set_writer_name.as_deref(), Some("simulation"));
        let fields = meta.meta_data.fields.unwrap();
        assert_eq!(fields[0].name.as_ref(), "StepUp");
        assert_eq!(fields[0].built_in_type, 7);
    }

    #[test]
    fn round_trips_metadata() {
        let message = MetaDataMessage {
            message_id: Some("m".into()),
            publisher_id: Some("plant-1".into()),
            data_set_writer_id: Some(7),
            writer_group_name: Some("line-a".into()),
            data_set_writer_name: Some("boiler".into()),
            timestamp: None,
            meta_data: DataSetMetaDataType {
                name: "boiler".into(),
                fields: Some(vec![FieldMetaData {
                    name: "Temperature".into(),
                    built_in_type: 11,
                    value_rank: -1,
                    ..FieldMetaData::default()
                }]),
                configuration_version: ConfigurationVersionDataType {
                    major_version: 1,
                    minor_version: 2,
                },
                ..DataSetMetaDataType::default()
            },
        };
        let json = message.to_json().unwrap();
        let parsed: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["MessageType"], "ua-metadata");
        assert_eq!(parsed["MetaData"]["Fields"][0]["Name"], "Temperature");
        assert_eq!(MetaDataMessage::from_json(&json).unwrap(), message);
        assert!(NetworkMessage::from_json(&json).is_err());
    }
}
