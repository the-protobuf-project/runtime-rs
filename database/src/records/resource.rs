//! The runtime descriptor of one proto resource: Go's `store.Resource`, `store.Column` and
//! `store.ForeignKey`.

use prost_reflect::{DynamicMessage, MessageDescriptor};

pub use super::kind::{Generated, Kind};

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
    /// The label each enum number is stored as, for an enum field stored by label rather than
    /// number, as Go's gorm target stores it (protokit `naming.EnumLabel`: `FORMAT_EBOOK` of
    /// enum `Format` is `EBOOK`). Empty stores the number. A number without a label (the
    /// zero `UNSPECIFIED`, an unknown one) is stored as null, or left out of an insert when
    /// the column is `not_null` so the column's default applies, as Go omits it; a stored
    /// label without a number reads as 0. Give such a column [`Kind::String`]. On Postgres a
    /// non-empty `sql_type` names the enum type: values are cast to it, and `ensure_schema`
    /// creates it from these labels when it is missing.
    pub labels: Vec<(i32, String)>,
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

    /// The label enum number `n` is stored as; see [`labels`](Self::labels).
    pub fn label_of(&self, n: i32) -> Option<&str> {
        self.labels
            .iter()
            .find(|(k, _)| *k == n)
            .map(|(_, l)| l.as_str())
    }

    /// The enum number stored label `label` stands for.
    pub fn number_of(&self, label: &str) -> Option<i32> {
        self.labels
            .iter()
            .find(|(_, l)| l == label)
            .map(|(k, _)| *k)
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
    /// The columns a free-text filter term searches, by backend name: a bareword such as
    /// `dune` in a filter matches a record when any of these string columns contains it,
    /// ignoring case. Empty refuses free-text terms. Go's `filterx.Spec.Search`, which
    /// protoc-gen-store fills, sorted, with the string fields marked
    /// `(store.v1.query) = {search: true}`.
    pub search: Vec<String>,
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
            search: Vec::new(),
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
