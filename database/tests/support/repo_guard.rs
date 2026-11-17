//! The etag guard `update` and `update_output_only` share, and `update_output_only`'s own
//! mask: the output-only fields only.

#![allow(dead_code)]

use database::Error;
use database::records::DB;
use database::repository::Repository;
use futures::FutureExt;
use futures::future::BoxFuture;
use prost_types::Timestamp;

use super::articles::{Article, article};
use super::repo_suite::{paths, soft};
use crate::fails;

fn stamp(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

/// A stale etag is `Conflict` and changes nothing; the current one, or none, passes.
pub fn etag_guards_update(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let first = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        let second = repo
            .update(
                &Article {
                    etag: first.etag.clone(),
                    ..article("a", "Beta", 0)
                },
                &paths(&["title"]),
            )
            .await
            .unwrap();
        let stale = Article {
            etag: first.etag.clone(),
            ..article("a", "Gamma", 0)
        };
        fails!(
            repo.update(&stale, &paths(&["title"])).await,
            Error::Conflict
        );
        fails!(
            repo.update_output_only(&stale, &paths(&["views"])).await,
            Error::Conflict
        );
        assert_eq!(repo.get("articles/a").await.unwrap(), second);
        let unguarded = repo
            .update(&article("a", "Delta", 0), &paths(&["title"]))
            .await
            .unwrap();
        assert_eq!(unguarded.title, "Delta");
    }
    .boxed()
}

/// `update_output_only` writes the output-only fields it names, ignores the rest, needs a
/// path, and leaves the soft-delete times to `delete`.
pub fn update_output_only_writes_only_output_fields(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let stored = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        let input = Article {
            views: 42,
            delete_time: stamp(5),
            ..article("a", "Ignored", 0)
        };
        fails!(
            repo.update_output_only(&input, &[]).await,
            Error::Invalid(_)
        );
        let got = repo
            .update_output_only(&input, &paths(&["views", "title", "delete_time"]))
            .await
            .unwrap();
        assert_eq!(
            (got.views, got.title.as_str(), got.pages),
            (42, "Alpha", 10)
        );
        assert!(got.delete_time.is_none());
        assert_ne!(got.etag, stored.etag);
    }
    .boxed()
}
