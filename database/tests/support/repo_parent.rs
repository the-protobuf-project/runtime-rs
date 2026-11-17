//! Listing under a parent ([`ListInput::parent`]), as Go's `parentScope`: by the parent
//! column when the parent's id is given, everything when it is `-` (AIP-159), by name when
//! the resource has no parent column; composed with the caller's filter and soft delete.

#![allow(dead_code)]

use database::Error;
use database::records::DB;
use database::repository::{ListInput, RecordsRepository, Repository, Rules};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::library::{self, Novel, Writer, novel};
use crate::fails;

async fn shelf(db: &DB, rules: Rules) -> RecordsRepository<Novel> {
    let mut res = (*library::novels("")).clone();
    if rules.parent_column.is_none() {
        res.table = "loose_novels".into();
        res.columns.retain(|c| c.name != "publisher_id");
    }
    let res = std::sync::Arc::new(res);
    db.schema.ensure_schema(&res).await.unwrap();
    let repo = RecordsRepository::new(db, res, rules).unwrap();
    for (p, id) in [("p1", "a"), ("p1", "b"), ("p2", "c"), ("p3", "d")] {
        repo.create(&novel(p, id, &format!("{p}{id}")))
            .await
            .unwrap();
    }
    repo
}

fn under(parent: &str) -> ListInput {
    ListInput {
        parent: parent.into(),
        ..Default::default()
    }
}

async fn names(repo: &RecordsRepository<Novel>, input: ListInput) -> Vec<String> {
    let page = repo.list(&input).await.unwrap();
    page.items.into_iter().map(|n| n.name).collect()
}

async fn scoped(db: DB, rules: Rules) {
    let repo = shelf(&db, rules).await;
    let p1 = ["publishers/p1/novels/a", "publishers/p1/novels/b"];
    assert_eq!(names(&repo, under("publishers/p1")).await, p1);
    assert_eq!(repo.count(&under("publishers/p1")).await.unwrap(), 2);
    assert_eq!(names(&repo, under("publishers/-")).await.len(), 4);
    assert_eq!(repo.count(&under("publishers/-")).await.unwrap(), 4);
    assert_eq!(repo.count(&under("")).await.unwrap(), 4);
    assert!(names(&repo, under("publishers/none")).await.is_empty());
    let paged = ListInput {
        page_size: 1,
        ..under("publishers/p1")
    };
    let first = repo.list(&paged).await.unwrap();
    assert_eq!(first.items[0].name, p1[0]);
    let second = ListInput {
        page_token: first.next_page_token,
        ..paged
    };
    assert_eq!(names(&repo, second).await, [p1[1]]);
    for bad in ["p1", "publishers", "shelves/p1", "publishers/p1/novels/a"] {
        fails!(repo.list(&under(bad)).await, Error::Invalid(_));
        fails!(repo.count(&under(bad)).await, Error::Invalid(_));
    }
}

/// With a parent column: `publisher_id = "p1"`, as Go narrows.
pub fn lists_scope_to_a_parent_by_its_column(db: DB) -> BoxFuture<'static, ()> {
    scoped(db, library::novel_rules()).boxed()
}

/// Without one: the records' names.
pub fn lists_scope_to_a_parent_by_name(db: DB) -> BoxFuture<'static, ()> {
    let rules = Rules {
        parent_column: None,
        ..library::novel_rules()
    };
    scoped(db, rules).boxed()
}

/// The caller's filter holds within the parent.
pub fn parent_scope_composes_with_a_filter(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = shelf(&db, library::novel_rules()).await;
        let input = ListInput {
            filter: "title = \"p1b\"".into(),
            ..under("publishers/p1")
        };
        assert_eq!(names(&repo, input).await, ["publishers/p1/novels/b"]);
        let other = ListInput {
            filter: "title = \"p2c\"".into(),
            ..under("publishers/p1")
        };
        assert!(names(&repo, other).await.is_empty());
    }
    .boxed()
}

/// A root resource has no parent to list under.
pub fn a_root_has_no_parent(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let res = library::writers();
        db.schema.ensure_schema(&res).await.unwrap();
        let repo: RecordsRepository<Writer> =
            RecordsRepository::new(&db, res, library::writer_rules()).unwrap();
        fails!(repo.list(&under("publishers/p1")).await, Error::Invalid(_));
        assert_eq!(repo.count(&under("")).await.unwrap(), 0);
    }
    .boxed()
}
