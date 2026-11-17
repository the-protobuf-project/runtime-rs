//! AIP-134 field masks over the stored record, as Go's generated `apply<Model>Mask` merges
//! them, and the etag guard `update` and `update_output_only` share.

#![allow(dead_code)]

use database::Error;
use database::records::DB;
use database::repository::Repository;
use futures::FutureExt;
use futures::future::BoxFuture;
use prost_types::Timestamp;

use super::articles::{Article, Format, article};
use super::repo_suite::{paths, soft};
use crate::fails;

fn stamp(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

/// Only the named fields change; the etag and update time move, the create time stays.
pub fn mask_writes_only_its_paths(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let mut m = article("a", "Alpha", 10);
        m.summary = Some("kept".into());
        let stored = repo.create(&m).await.unwrap();
        let input = Article {
            title: "Alpha 2".into(),
            pages: 99,
            ..article("a", "", 0)
        };
        let got = repo.update(&input, &paths(&["title"])).await.unwrap();
        assert_eq!((got.title.as_str(), got.pages), ("Alpha 2", 10));
        assert_eq!(got.summary.as_deref(), Some("kept"));
        assert_ne!(got.etag, stored.etag);
        assert_eq!(got.create_time, stored.create_time);
        let at = |t: &Option<Timestamp>| t.as_ref().map(|t| (t.seconds, t.nanos));
        assert!(at(&got.update_time) >= at(&stored.update_time));
        assert_eq!(repo.get("articles/a").await.unwrap(), got);
    }
    .boxed()
}

/// An empty mask, or `*`, replaces every mutable field and no output-only one.
pub fn empty_mask_replaces_every_mutable_field(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let mut m = article("a", "Alpha", 10);
        m.summary = Some("gone".into());
        m.publish_time = stamp(100);
        repo.create(&m).await.unwrap();
        repo.update_output_only(
            &Article {
                views: 7,
                ..article("a", "", 0)
            },
            &paths(&["views"]),
        )
        .await
        .unwrap();
        let input = Article {
            views: 1000,
            ..article("a", "Beta", 0)
        };
        let got = repo.update(&input, &[]).await.unwrap();
        assert_eq!(
            (got.title.as_str(), got.pages, got.summary),
            ("Beta", 0, None)
        );
        assert_eq!((got.publish_time, got.views), (None, 7));
        let star = repo
            .update(&article("a", "Gamma", 3), &paths(&["*"]))
            .await
            .unwrap();
        assert_eq!((star.title.as_str(), star.pages), ("Gamma", 3));
    }
    .boxed()
}

/// An output-only path in `update`'s mask is ignored (AIP-203), as are the etag and the
/// audit times; the rest of the mask is written.
pub fn output_only_paths_are_ignored(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let stored = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        let input = Article {
            views: 5,
            etag: String::new(),
            create_time: stamp(1),
            delete_time: stamp(2),
            ..article("a", "Beta", 0)
        };
        let mask = paths(&["views", "etag", "create_time", "delete_time", "title"]);
        let got = repo.update(&input, &mask).await.unwrap();
        assert_eq!((got.title.as_str(), got.views), ("Beta", 0));
        assert_eq!(got.create_time, stored.create_time);
        assert!(got.delete_time.is_none());
        assert_eq!(got.etag.len(), 26);
    }
    .boxed()
}

/// A path naming no field, or reaching into a scalar, is `Invalid`, and nothing is written.
pub fn unknown_paths_are_refused(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let stored = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        let input = article("a", "Beta", 1);
        for bad in [
            &["nope"][..],
            &["title", "nope"],
            &["title.length"],
            &["publish_time.x"],
            &[""],
        ] {
            fails!(repo.update(&input, &paths(bad)).await, Error::Invalid(_));
        }
        fails!(
            repo.update(&input, &paths(&["*", "title"])).await,
            Error::Invalid(_)
        );
        fails!(
            repo.update_output_only(&input, &paths(&["nope"])).await,
            Error::Invalid(_)
        );
        assert_eq!(repo.get("articles/a").await.unwrap(), stored);
        fails!(
            repo.update(&article("missing", "X", 1), &[]).await,
            Error::NotFound
        );
        let unnamed = Article {
            title: "X".into(),
            ..Default::default()
        };
        fails!(repo.update(&unnamed, &[]).await, Error::Invalid(_));
    }
    .boxed()
}

/// A path into a message field writes the whole field, and one arm of a oneof the whole
/// oneof (Go's `GroupTouched`).
pub fn oneof_and_message_fields_replace_whole(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let mut m = article("a", "Alpha", 10);
        m.format = Some(Format::Isbn("978-0".into()));
        m.publish_time = Some(Timestamp {
            seconds: 100,
            nanos: 5000,
        });
        repo.create(&m).await.unwrap();
        let input = Article {
            format: Some(Format::Doi("10.1000/1".into())),
            publish_time: stamp(200),
            ..article("a", "", 0)
        };
        let got = repo
            .update(&input, &paths(&["isbn", "publish_time.seconds"]))
            .await
            .unwrap();
        assert_eq!(got.format, Some(Format::Doi("10.1000/1".into())));
        assert_eq!(got.publish_time, stamp(200));
        assert_eq!(got.title, "Alpha");
        let cleared = repo
            .update(&article("a", "", 0), &paths(&["doi"]))
            .await
            .unwrap();
        assert_eq!(cleared.format, None);
    }
    .boxed()
}
