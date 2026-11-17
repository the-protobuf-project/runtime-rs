//! The runtime descriptor of one proto resource: Go's `store.Resource`, `store.Column` and
//! `store.ForeignKey`.

use prost_reflect::{DynamicMessage, MessageDescriptor};

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

/// One field of a resource: how it appears in the message (`field`) and in the backend
/// (`name`, `kind`, `sql_type`). Go's `store.Column`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Column {
    /// The backend column identifier, e.g. `author_id`.
    pub name: String,
    /// The proto field carrying the value; empty for a column the driver supplies.
    pub field: String,
    /// The backend-neutral value domain.
    pub kind: Kind,
    /// The canonical SQL type from protorm's IR, e.g. `VARCHAR(255)`; read by SQL drivers.
    pub sql_type: String,
    /// Whether this is the key column; exactly one per resource is expected.
    pub primary_key: bool,
    /// Whether the field is required; when false an unset optional field is stored as null.
    pub not_null: bool,
    /// Whether the column is under a unique constraint, which drives `AlreadyExists`.
    pub unique: bool,
    /// The generation strategy of a synthesized key column, filled on create.
    pub generated: Option<Generated>,
    /// Set to the current time on create.
    pub auto_create: bool,
    /// Set to the current time on create and update.
    pub auto_update: bool,
}

impl Column {
    /// A column named `name` carried by the proto field of the same name.
    pub fn new(name: impl Into<String>, kind: Kind) -> Self {
        let name = name.into();
        Self {
            field: name.clone(),
            name,
            kind,
            ..Self::default()
        }
    }

    /// Marks this the primary key.
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.not_null = true;
        self
    }

    /// Marks this not null.
    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    /// Marks this unique.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Sets the SQL type.
    pub fn sql_type(mut self, sql_type: impl Into<String>) -> Self {
        self.sql_type = sql_type.into();
        self
    }

    /// Whether a driver, not the message, supplies this column's value: a generated key or an
    /// audit timestamp. The bridge skips such columns. Go's `Column.Managed`.
    pub fn managed(&self) -> bool {
        self.generated.is_some() || self.auto_create || self.auto_update
    }
}

/// A column referencing another resource's key. Go's `store.ForeignKey`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ForeignKey {
    /// The referencing column in this resource.
    pub column: String,
    /// The `name` of the referenced resource.
    pub referenced_name: String,
    /// The referenced column, usually its primary key.
    pub referenced_field: String,
}

/// The runtime descriptor of one proto resource message: the runtime mirror of protorm's
/// `schema.Table`, and Go's `store.Resource`. The message descriptor stands in for Go's
/// `New`: drivers build and read records as [`DynamicMessage`]s of it.
#[derive(Clone, Debug)]
pub struct Resource {
    /// The resource identifier, the message's simple name (`Book`): the registry key.
    pub name: String,
    /// The logical namespace the resource lives in, e.g. `bookstore_v1`.
    pub schema: String,
    /// The backend table or collection name, e.g. `books`.
    pub table: String,
    /// The `name` of the primary-key column.
    pub pk_column: String,
    /// The message type records of this resource are.
    pub message: MessageDescriptor,
    /// Whether the resource describes a connection between records rather than a record.
    pub is_edge: bool,
    /// The layout fingerprint the resource was generated with; empty disables checks.
    pub schema_version: String,
    /// The columns, in declaration order.
    pub columns: Vec<Column>,
    /// The foreign keys.
    pub fks: Vec<ForeignKey>,
}

impl Resource {
    /// A resource of `message`, stored in `table`, keyed by the column named `pk_column`.
    pub fn new(
        message: MessageDescriptor,
        table: impl Into<String>,
        pk_column: impl Into<String>,
        columns: Vec<Column>,
    ) -> Self {
        Self {
            name: message.name().to_owned(),
            schema: String::new(),
            table: table.into(),
            pk_column: pk_column.into(),
            message,
            is_edge: false,
            schema_version: String::new(),
            columns,
            fks: Vec::new(),
        }
    }

    /// The primary-key column, if one is defined.
    pub fn pk(&self) -> Option<&Column> {
        self.lookup_column(&self.pk_column)
    }

    /// The column with the given backend name.
    pub fn lookup_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// An empty record of this resource's message type. Go's `Resource.New()`.
    pub fn new_message(&self) -> DynamicMessage {
        DynamicMessage::new(self.message.clone())
    }

    /// The schema-qualified table, `schema.table`, or the bare table without a schema.
    pub fn qualified_table(&self) -> String {
        if self.schema.is_empty() {
            self.table.clone()
        } else {
            format!("{}.{}", self.schema, self.table)
        }
    }
}
