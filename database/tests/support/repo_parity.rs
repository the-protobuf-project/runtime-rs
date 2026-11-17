//! What a Rust write leaves in a table Go's repository also writes, read back raw: a root's
//! `id` is its name's last segment, a nested record's `id` a fresh ULID and its parent
//! column its parent's id, a reference its bare id, an enum its label (Go's gorm goldens:
//! `m.ID = id`, `m.OrganisationID = parentIDs[0]`, `m.UserID = repox.LastSegment(v)`,
//! `<Enum>FromProto`).

#![allow(dead_code)]

use std::sync::Arc;

use database::Error;
use database::records::DB;
use database::repository::{RecordsRepository, Repository};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::library::{self, Novel, Writer, novel};
use crate::fails;

#[path = "repo_parity_enums.rs"]
mod enums;
#[allow(unused_imports)] // the suites without parity cases include this module too
pub use enums::enums_store_their_label;

/// One stored column, as text: `(table, column, name)`; `None` for null.
pub type Raw = Arc<dyn Fn(&str, &str, &str) -> BoxFuture<'static, Option<String>> + Send + Sync>;

/// A parity case: a database, how to read it raw, and the Postgres enum type (empty
/// elsewhere).
pub type Parity = fn(DB, Raw, String) -> BoxFuture<'static, ()>;

async fn writers(db: &DB) -> RecordsRepository<Writer> {
    let res = library::writers();
    db.schema.ensure_schema(&res).await.unwrap();
    RecordsRepository::new(db, res, library::writer_rules()).unwrap()
}

async fn novels(db: &DB, enum_type: &str) -> RecordsRepository<Novel> {
    let res = library::novels(enum_type);
    db.schema.ensure_schema(&res).await.unwrap();
    RecordsRepository::new(db, res, library::novel_rules()).unwrap()
}

/// A root's key is its name's last id; one created without a name gets a minted ULID as
/// both, as Go's `Create`.
pub fn root_id_is_the_names_last_segment(db: DB, raw: Raw, _: String) -> BoxFuture<'static, ()> {
    async move {
        let repo = writers(&db).await;
        let named = Writer {
            name: "writers/w1".into(),
            display_name: "Ann".into(),
        };
        repo.create(&named).await.unwrap();
        assert_eq!(
            raw("writers", "id", "writers/w1").await.as_deref(),
            Some("w1")
        );
        let minted = repo
            .create(&Writer {
                display_name: "Bo".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let id = minted.name.strip_prefix("writers/").expect("a minted name");
        assert_eq!(id.len(), 26, "a ULID: {id}");
        assert_eq!(
            raw("writers", "id", &minted.name).await.as_deref(),
            Some(id)
        );
    }
    .boxed()
}

/// A nested record is keyed by a surrogate ULID (its id is unique only under its parent)
/// and points at its parent through the parent column; an empty id is minted.
pub fn nested_records_get_a_surrogate_and_their_parents_id(
    db: DB,
    raw: Raw,
    t: String,
) -> BoxFuture<'static, ()> {
    async move {
        let repo = novels(&db, &t).await;
        repo.create(&novel("p1", "n1", "Dune")).await.unwrap();
        let id = raw("novels", "id", "publishers/p1/novels/n1")
            .await
            .unwrap();
        assert!(id.len() == 26 && id != "n1", "a surrogate: {id}");
        assert_eq!(
            raw("novels", "publisher_id", "publishers/p1/novels/n1")
                .await
                .as_deref(),
            Some("p1")
        );
        let minted = repo.create(&novel("p2", "", "Emma")).await.unwrap();
        assert!(minted.name.starts_with("publishers/p2/novels/") && minted.name.len() > 21);
        assert_eq!(
            raw("novels", "publisher_id", &minted.name).await.as_deref(),
            Some("p2")
        );
    }
    .boxed()
}

/// A reference to a root resource is stored as its bare id and read back as its name.
pub fn references_store_the_bare_id(db: DB, raw: Raw, t: String) -> BoxFuture<'static, ()> {
    async move {
        let repo = novels(&db, &t).await;
        let name = "publishers/p1/novels/n1";
        let created = repo
            .create(&Novel {
                author: "writers/w1".into(),
                ..novel("p1", "n1", "Dune")
            })
            .await
            .unwrap();
        assert_eq!(created.author, "writers/w1");
        assert_eq!(raw("novels", "author", name).await.as_deref(), Some("w1"));
        let input = Novel {
            author: "writers/w2".into(),
            ..novel("p1", "n1", "")
        };
        let updated = repo.update(&input, &["author".into()]).await.unwrap();
        assert_eq!(
            (updated.author.as_str(), updated.title.as_str()),
            ("writers/w2", "Dune")
        );
        assert_eq!(raw("novels", "author", name).await.as_deref(), Some("w2"));
        let listed = repo.list(&Default::default()).await.unwrap();
        assert_eq!(listed.items[0].author, "writers/w2");
        repo.update(&novel("p1", "n1", "Dune"), &["author".into()])
            .await
            .unwrap();
        assert_eq!(raw("novels", "author", name).await.as_deref(), Some(""));
    }
    .boxed()
}

/// A name that does not fit the pattern is `Invalid`, on every call, as Go's `SplitName`.
pub fn names_are_checked_against_the_pattern(db: DB, _: Raw, t: String) -> BoxFuture<'static, ()> {
    async move {
        let repo = novels(&db, &t).await;
        for bad in [
            "novels/n1",
            "publishers/p1/books/n1",
            "publishers//novels/n1",
            "x",
        ] {
            fails!(repo.get(bad).await, Error::Invalid(_));
            fails!(
                repo.create(&Novel {
                    name: bad.into(),
                    ..Default::default()
                })
                .await,
                Error::Invalid(_)
            );
            fails!(repo.delete(bad, None).await, Error::Invalid(_));
        }
        fails!(repo.create(&Novel::default()).await, Error::Invalid(_));
        fails!(repo.get("publishers/p1/novels/none").await, Error::NotFound);
    }
    .boxed()
}
