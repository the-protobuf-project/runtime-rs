//! What differs between the SQL engines this backend claims: identifier quoting and the
//! column type a kind gets when the descriptor names none. Go's `quote`, `defaultSQLType`
//! and `dialect` in `orm/schema.go`.

use crate::records::Kind;

/// A SQL dialect. Go reads it from GORM's dialector name; here the pool's type decides it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// PostgreSQL: a selected database is a schema.
    Postgres,
    /// MySQL: a selected database is a database, which is what MySQL calls a schema.
    MySql,
    /// SQLite: a selected database is an attached one.
    Sqlite,
}

impl Dialect {
    /// Its name, as a backend error reports it.
    pub fn name(self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::MySql => "mysql",
            Dialect::Sqlite => "sqlite",
        }
    }

    /// `ident` quoted for this dialect, a quote inside it doubled. MySQL reads a double-quoted
    /// name as a string literal unless `ANSI_QUOTES` is set, so `"id" = ?` there compares a
    /// constant and silently matches nothing: backticks are MySQL's, double quotes everyone
    /// else's. Names come from a descriptor, not a request, so this is defense in depth.
    /// Go's `Driver.quote`.
    pub fn quote(self, ident: &str) -> String {
        match self {
            Dialect::MySql => format!("`{}`", ident.replace('`', "``")),
            _ => format!("\"{}\"", ident.replace('"', "\"\"")),
        }
    }

    /// `schema.table` quoted, or the bare table without a schema. Go's `quotedTable`.
    pub fn quote_table(self, schema: &str, table: &str) -> String {
        if schema.is_empty() {
            self.quote(table)
        } else {
            format!("{}.{}", self.quote(schema), self.quote(table))
        }
    }

    /// The column type for a kind whose descriptor names no `sql_type`. Dialect-aware
    /// because a type name is not portable: `BLOB` is not a PostgreSQL type, `TIMESTAMPTZ` is
    /// not a MySQL one. A descriptor's own `sql_type` wins: it was written for a known target.
    /// Go's `defaultSQLType`.
    pub fn default_sql_type(self, kind: Kind) -> &'static str {
        use Dialect::*;
        match (kind, self) {
            (Kind::Int | Kind::Enum, _) => "BIGINT",
            // No unsigned 64-bit column every engine agrees on: signed outside MySQL, so a
            // value past 2^63 does not fit there.
            (Kind::Uint, MySql) => "BIGINT UNSIGNED",
            (Kind::Uint, _) => "BIGINT",
            (Kind::Bool, _) => "BOOLEAN",
            (Kind::Bytes, Postgres) => "BYTEA",
            (Kind::Bytes, MySql) => "LONGBLOB",
            (Kind::Bytes, Sqlite) => "BLOB",
            (Kind::Float, _) => "DOUBLE PRECISION",
            (Kind::Timestamp, Postgres) => "TIMESTAMPTZ",
            (Kind::Timestamp, MySql) => "DATETIME(6)",
            (Kind::Timestamp, Sqlite) => "TIMESTAMP",
            // MySQL cannot index TEXT without a prefix length, and a unique column must be.
            (Kind::String | Kind::Unknown, MySql) => "VARCHAR(255)",
            (Kind::String | Kind::Unknown, _) => "TEXT",
        }
    }

    /// The column definitions of `CREATE TABLE`: each column's type, then `PRIMARY KEY`, or
    /// else `UNIQUE`, and `NOT NULL` for a required non-key column. Go's `EnsureSchema`.
    pub fn column_defs(self, res: &crate::records::Resource) -> String {
        res.columns
            .iter()
            .map(|c| {
                let ty = if c.sql_type.is_empty() {
                    self.default_sql_type(c.kind)
                } else {
                    &c.sql_type
                };
                let mut def = format!("{} {ty}", self.quote(&c.name));
                if c.primary_key {
                    def.push_str(" PRIMARY KEY");
                } else if c.unique {
                    def.push_str(" UNIQUE");
                }
                if c.not_null && !c.primary_key {
                    def.push_str(" NOT NULL");
                }
                def
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}
