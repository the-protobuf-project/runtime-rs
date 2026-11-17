//! A column's value domain and how a key is generated. Go's `store.Kind`.

/// A column's value domain, independent of any one backend's type system, so a driver
/// decides how to encode it without parsing backend type strings. Go's `store.Kind`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Kind {
    /// Unclassified; read as a string, as Go's zero `KindUnknown` is.
    #[default]
    Unknown,
    /// A string.
    String,
    /// A signed integer (proto `int32`/`int64`/`sint*`/`sfixed*`).
    Int,
    /// An unsigned integer (proto `uint32`/`uint64`/`fixed*`).
    Uint,
    /// A boolean.
    Bool,
    /// Bytes.
    Bytes,
    /// A float (proto `float`/`double`).
    Float,
    /// A `google.protobuf.Timestamp`, bridged as a UTC time.
    Timestamp,
    /// An enum, bridged as its `i32` number.
    Enum,
}

impl std::fmt::Display for Kind {
    /// Names the kind as an error reads it: "a string", not "1".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Kind::String => "a string",
            Kind::Int => "a signed integer",
            Kind::Uint => "an unsigned integer",
            Kind::Bool => "a boolean",
            Kind::Bytes => "bytes",
            Kind::Float => "a float",
            Kind::Timestamp => "a timestamp",
            Kind::Enum => "an enum",
            Kind::Unknown => "of unknown kind",
        })
    }
}

/// How a value is generated for a key column the message does not carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Generated {
    /// A ULID, as Go's `"ulid"`.
    Ulid,
    /// A random UUID, as Go's `"uuid"`.
    Uuid,
}
