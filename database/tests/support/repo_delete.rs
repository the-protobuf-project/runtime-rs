//! AIP-164 soft delete as Go's repository does it, and Go's `TestIfMatchDelete` ported case
//! for case; then a resource without soft delete, and one without an etag.

#![allow(dead_code)]

use std::time::Duration;

use database::Error;
use database::records::DB;
use database::repository::{Repository, Rules};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::articles::{self, Article, article};
use super::repo_suite::{RETENTION, open, paths, soft};
use crate::fails;

fn secs(t: &Option<prost_types::Timestamp>) -> (i64, i32) {
    let t = t.as_ref().expect("a time");
    (t.seconds, t.nanos)
}

/// Delete sets `delete_time`, moves `update_time` to the same instant, sets `expire_time`
/// to `delete_time` plus the retention, and mints an etag; `get` still returns the record.
/// Undelete clears both; purge removes it.
pub fn soft_delete_marks_and_undelete_restores(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let live = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        repo.delete("articles/a", None).await.unwrap();
        let gone = repo.get("articles/a").await.unwrap();
        assert_eq!(secs(&gone.delete_time), secs(&gone.update_time));
        let (s, n) = secs(&gone.delete_time);
        assert_eq!(secs(&gone.expire_time), (s + RETENTION.as_secs() as i64, n));
        assert_ne!(gone.etag, live.etag);
        assert_eq!(gone.create_time, live.create_time);
        fails!(repo.delete("articles/a", None).await, Error::NotFound);
        let back = repo.undelete("articles/a", None).await.unwrap();
        assert!(back.delete_time.is_none() && back.expire_time.is_none());
        assert_ne!(back.etag, gone.etag);
        assert_eq!(repo.get("articles/a").await.unwrap(), back);
        fails!(
            repo.undelete("articles/a", None).await,
            Error::AlreadyExists
        );
        repo.delete("articles/a", None).await.unwrap();
        repo.purge("articles/a").await.unwrap();
        fails!(repo.get("articles/a").await, Error::NotFound);
        fails!(repo.purge("articles/a").await, Error::NotFound);
        fails!(repo.delete("articles/none", None).await, Error::NotFound);
        fails!(repo.undelete("articles/none", None).await, Error::NotFound);
        fails!(repo.delete("", None).await, Error::Invalid(_));
        repo.create(&article("b", "Beta", 1)).await.unwrap();
        repo.purge("articles/b").await.unwrap();
        fails!(repo.get("articles/b").await, Error::NotFound);
    }
    .boxed()
}

/// Go's `TestIfMatchDelete`.
pub fn if_match_guards_delete_and_undelete(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let b = repo.create(&article("a", "Dune", 10)).await.unwrap();
        // A write lands between the caller's read (b) and its delete.
        let updated = repo
            .update(&article("a", "Dune Messiah", 0), &paths(&["title"]))
            .await
            .unwrap();
        fails!(repo.delete(&b.name, Some(&b.etag)).await, Error::Conflict);
        assert!(repo.get(&b.name).await.unwrap().delete_time.is_none());
        repo.delete(&b.name, Some(&updated.etag)).await.unwrap();
        let deleted = repo.get(&b.name).await.unwrap();
        assert!(deleted.delete_time.is_some());
        // Deleted already: not found, whatever the etag.
        fails!(
            repo.delete(&b.name, Some(&deleted.etag)).await,
            Error::NotFound
        );
        fails!(
            repo.undelete(&b.name, Some(&updated.etag)).await,
            Error::Conflict
        );
        assert!(repo.get(&b.name).await.unwrap().delete_time.is_some());
        let restored = repo.undelete(&b.name, Some(&deleted.etag)).await.unwrap();
        assert!(restored.delete_time.is_none());
        // Live already: not deleted, whatever the etag.
        fails!(
            repo.undelete(&b.name, Some("stale")).await,
            Error::AlreadyExists
        );
        // An empty etag carries no precondition.
        repo.delete(&b.name, Some("")).await.unwrap();
    }
    .boxed()
}

/// A zero retention leaves `expire_time` unset, as Go's `ExpireAt` does.
pub fn zero_retention_leaves_expire_time_unset(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = open(&db, articles::rules(Duration::ZERO)).await;
        repo.create(&article("a", "Alpha", 1)).await.unwrap();
        repo.delete("articles/a", None).await.unwrap();
        let gone = repo.get("articles/a").await.unwrap();
        assert!(gone.delete_time.is_some() && gone.expire_time.is_none());
    }
    .boxed()
}

/// Without a `delete_time` rule, delete removes the record (guarded by `if_match`), undelete
/// is refused, and nothing is filtered from a listing.
pub fn without_soft_delete_delete_removes(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = open(&db, articles::hard_rules()).await;
        let a = repo.create(&article("a", "Alpha", 1)).await.unwrap();
        let b = repo
            .update(&article("a", "Beta", 1), &paths(&["title"]))
            .await
            .unwrap();
        fails!(
            repo.delete("articles/a", Some(&a.etag)).await,
            Error::Conflict
        );
        repo.get("articles/a").await.unwrap();
        repo.delete("articles/a", Some(&b.etag)).await.unwrap();
        fails!(repo.get("articles/a").await, Error::NotFound);
        fails!(repo.delete("articles/a", None).await, Error::NotFound);
        fails!(repo.delete("articles/a", Some("x")).await, Error::NotFound);
        fails!(
            repo.undelete("articles/a", None).await,
            Error::Unimplemented { .. }
        );
        let fresh = Article {
            delete_time: Some(prost_types::Timestamp::default()),
            ..article("c", "Gamma", 1)
        };
        repo.create(&fresh).await.unwrap();
        let input = database::repository::ListInput::default();
        assert_eq!(repo.count(&input).await.unwrap(), 1);
    }
    .boxed()
}

/// A resource without an etag ignores `if_match` and `m`'s etag, as Go's does.
pub fn without_an_etag_if_match_is_ignored(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let rules = Rules {
            etag: None,
            ..articles::rules(RETENTION)
        };
        let repo = open(&db, rules).await;
        let a = repo.create(&article("a", "Alpha", 1)).await.unwrap();
        assert!(a.etag.is_empty());
        let stale = Article {
            etag: "stale".into(),
            ..article("a", "Beta", 1)
        };
        // No guard, and no minting: without the rule, `etag` is an ordinary field.
        assert_eq!(repo.update(&stale, &[]).await.unwrap().etag, "stale");
        repo.delete("articles/a", Some("stale")).await.unwrap();
        repo.undelete("articles/a", Some("stale")).await.unwrap();
    }
    .boxed()
}
