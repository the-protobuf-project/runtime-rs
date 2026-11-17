//! What differs between engines, without one: identifier quoting, the column types a kind
//! gets, and the `CREATE TABLE` columns a descriptor makes. The two bugs runtime-go's
//! `dialect_test.go` names (quoting that only works outside MySQL, types that only exist
//! outside Postgres) pinned here; `orm_live` proves them against the engines.

mod support {
    #[path = "orm.rs"]
    pub mod orm;
}

use database::orm::Dialect::{self, MySql, Postgres, Sqlite};
use database::records::Kind;
use support::orm::{book_res, unique_res};

#[test]
fn identifiers_are_quoted_for_each_dialect() {
    assert_eq!(MySql.quote("id"), "`id`");
    assert_eq!(Postgres.quote("id"), "\"id\"");
    assert_eq!(Sqlite.quote("id"), "\"id\"");
    // A quote inside a name is doubled, never closes it.
    assert_eq!(MySql.quote("a`b"), "`a``b`");
    assert_eq!(
        Postgres.quote("a\"; DROP TABLE x; --"),
        "\"a\"\"; DROP TABLE x; --\""
    );
    assert_eq!(
        Postgres.quote_table("tenant_a", "books"),
        "\"tenant_a\".\"books\""
    );
    assert_eq!(MySql.quote_table("", "books"), "`books`");
}

#[test]
fn default_types_exist_on_their_dialect() {
    let want: [(Kind, [&str; 3]); 9] = [
        (Kind::Int, ["BIGINT", "BIGINT", "BIGINT"]),
        (Kind::Enum, ["BIGINT", "BIGINT", "BIGINT"]),
        (Kind::Uint, ["BIGINT", "BIGINT UNSIGNED", "BIGINT"]),
        (Kind::Bool, ["BOOLEAN", "BOOLEAN", "BOOLEAN"]),
        (Kind::Bytes, ["BYTEA", "LONGBLOB", "BLOB"]),
        (
            Kind::Float,
            ["DOUBLE PRECISION", "DOUBLE PRECISION", "DOUBLE PRECISION"],
        ),
        (Kind::Timestamp, ["TIMESTAMPTZ", "DATETIME(6)", "TIMESTAMP"]),
        (Kind::String, ["TEXT", "VARCHAR(255)", "TEXT"]),
        (Kind::Unknown, ["TEXT", "VARCHAR(255)", "TEXT"]),
    ];
    for (kind, types) in want {
        for (d, ty) in [Postgres, MySql, Sqlite].into_iter().zip(types) {
            assert_eq!(d.default_sql_type(kind), ty, "{kind:?} on {d:?}");
        }
    }
}

#[test]
fn create_table_columns_come_from_the_descriptor() {
    assert_eq!(
        Postgres.column_defs(&book_res()),
        "\"id\" TEXT PRIMARY KEY, \"title\" TEXT NOT NULL, \"published_year\" BIGINT, \
         \"genre\" BIGINT NOT NULL"
    );
    let mut res = unique_res();
    res.columns[2].sql_type = "SMALLINT".into();
    assert_eq!(
        MySql.column_defs(&res),
        "`id` VARCHAR(255) PRIMARY KEY, `title` VARCHAR(255) UNIQUE NOT NULL, \
         `published_year` SMALLINT, `genre` BIGINT NOT NULL",
        "a descriptor's own type wins"
    );
}

#[test]
fn dialects_name_themselves() {
    let names: Vec<_> = [Postgres, MySql, Sqlite].map(Dialect::name).into();
    assert_eq!(names, ["postgres", "mysql", "sqlite"]);
}
