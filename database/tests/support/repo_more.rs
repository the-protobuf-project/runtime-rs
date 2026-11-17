//! Hooks around each call, writers racing on one etag (Go's `TestGormEtagGuardsLockTheRow`,
//! run rather than read), and what `RecordsRepository::new` refuses.

#![allow(dead_code)]

use std::sync::Arc;

use database::Error;
use database::records::DB;
use database::repository::{Hooks, RecordsRepository, Repository, Rules};
use futures::FutureExt;
use futures::future::{BoxFuture, join_all};

use super::articles::{self, Article, article};
use super::repo_suite::{RETENTION, paths, soft};
use crate::fails;

/// `before_create` normalizes, `after_read` derives, `before_update` sees both versions and
/// changes the merged one, `before_delete` vetoes.
pub fn hooks_run_around_each_call(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let hooks = Hooks::<Article>::default()
            .before_create(|mut m| async move {
                m.title = m.title.trim().to_owned();
                Ok(m)
            })
            .after_read(|mut m| async move {
                m.summary = Some(format!("read {}", m.title));
                Ok(m)
            })
            .before_update(|old, mut new, paths| async move {
                assert_eq!(paths, ["title"]);
                new.pages = old.pages + 1;
                Ok(new)
            })
            .before_delete(|name| async move {
                match name.as_str() {
                    "articles/keep" => Err(Error::Invalid("kept".into())),
                    _ => Ok(()),
                }
            });
        let repo = soft(&db).await.with_hooks(hooks);
        let a = repo.create(&article("keep", "  Alpha ", 10)).await.unwrap();
        assert_eq!(a.title, "Alpha");
        assert_eq!(a.summary.as_deref(), Some("read Alpha"));
        let b = repo
            .update(&article("keep", "Beta", 0), &paths(&["title"]))
            .await
            .unwrap();
        assert_eq!((b.pages, b.summary.as_deref()), (11, Some("read Beta")));
        let listed = repo.list(&Default::default()).await.unwrap();
        assert_eq!(listed.items[0].summary.as_deref(), Some("read Beta"));
        fails!(repo.delete("articles/keep", None).await, Error::Invalid(_));
        fails!(repo.purge("articles/keep").await, Error::Invalid(_));
        assert!(
            repo.get("articles/keep")
                .await
                .unwrap()
                .delete_time
                .is_none()
        );
    }
    .boxed()
}

/// Several writers holding one etag: exactly one update lands, the rest are `Conflict`.
pub fn concurrent_updates_with_one_etag_admit_one(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = Arc::new(soft(&db).await);
        let a = repo.create(&article("a", "Alpha", 1)).await.unwrap();
        let writers = (0..4).map(|i| {
            let (repo, etag) = (repo.clone(), a.etag.clone());
            async move {
                let m = Article {
                    etag,
                    ..article("a", &format!("Writer {i}"), 0)
                };
                repo.update(&m, &paths(&["title"])).await
            }
        });
        let results = join_all(writers).await;
        let won = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(won, 1, "{results:?}");
        assert!(
            results
                .iter()
                .all(|r| matches!(r, Ok(_) | Err(Error::Conflict)))
        );
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(repo.get("articles/a").await.unwrap(), winner);
    }
    .boxed()
}

/// Deletes guarded by one etag, racing an update holding it too: exactly one write lands.
pub fn concurrent_deletes_with_one_etag_admit_one(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = Arc::new(soft(&db).await);
        let a = repo.create(&article("a", "Alpha", 1)).await.unwrap();
        let (r1, r2, e1, e2) = (repo.clone(), repo.clone(), a.etag.clone(), a.etag.clone());
        let delete = async move { r1.delete("articles/a", Some(&e1)).await.map(drop) };
        let update = async move {
            let m = Article {
                etag: e2,
                ..article("a", "Beta", 0)
            };
            r2.update(&m, &paths(&["title"])).await.map(drop)
        };
        let (d, u) = futures::join!(delete, update);
        assert!(d.is_ok() != u.is_ok(), "delete {d:?}, update {u:?}");
        let loser = if d.is_ok() { u } else { d };
        fails!(loser, Error::Conflict);
    }
    .boxed()
}

/// A resource of another message, or rules naming a field the message lacks, the resource
/// does not store, or of the wrong type, are refused.
pub fn new_refuses_what_does_not_fit(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let res = articles::resource("articles");
        let with = |f: fn(&mut Rules)| {
            let mut rules = articles::rules(RETENTION);
            f(&mut rules);
            RecordsRepository::<Article>::new(&db, res.clone(), rules).map(drop)
        };
        with(|_| {}).unwrap();
        fails!(with(|r| r.etag = Some("nope".into())), Error::Invalid(_));
        fails!(with(|r| r.etag = Some("pages".into())), Error::Invalid(_));
        fails!(
            with(|r| r.delete_time = Some("title".into())),
            Error::Invalid(_)
        );
        fails!(
            with(|r| r.output_only.push("nope".into())),
            Error::Invalid(_)
        );
        let mut narrow = (*res).clone();
        narrow.columns.retain(|c| c.name != "expire_time");
        fails!(
            RecordsRepository::<Article>::new(&db, Arc::new(narrow), articles::rules(RETENTION))
                .map(drop),
            Error::Invalid(_)
        );
        fails!(
            RecordsRepository::<prost_types::Duration>::new(&db, res, Rules::default()).map(drop),
            Error::Invalid(_)
        );
    }
    .boxed()
}
