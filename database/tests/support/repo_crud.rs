//! Create, get, list and count through a repository: etags minted as Go mints them, audit
//! times from the provider, pages from the provider's tokens, and soft-deleted records left
//! out of a listing by a clause ANDed onto the caller's filter.

use database::Error;
use database::records::DB;
use database::repository::{ListInput, Repository};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::articles::article;
use super::repo_suite::soft;
use crate::fails;

/// Every write mints a 26-character ULID etag, later ones sorting after earlier ones; the
/// provider fills both audit times; a taken name or unique title is `AlreadyExists`.
pub fn create_mints_etag_and_audit_times(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let a = repo.create(&article("a", "Alpha", 10)).await.unwrap();
        let b = repo.create(&article("b", "Beta", 20)).await.unwrap();
        assert_eq!(a.etag.len(), 26, "a ULID: {}", a.etag);
        assert!(b.etag > a.etag, "monotonic: {} then {}", a.etag, b.etag);
        assert!(a.create_time.is_some() && a.create_time == a.update_time);
        assert!(a.delete_time.is_none() && a.expire_time.is_none());
        assert_eq!(repo.get("articles/a").await.unwrap(), a);
        fails!(
            repo.create(&article("a", "Other", 1)).await,
            Error::AlreadyExists
        );
        fails!(
            repo.create(&article("c", "Alpha", 1)).await,
            Error::AlreadyExists
        );
    }
    .boxed()
}

/// A caller's etag on create is replaced, as Go's adapter assigns its own.
pub fn create_replaces_a_callers_etag(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        let mut m = article("a", "Alpha", 10);
        m.etag = "chosen-by-the-client".into();
        let stored = repo.create(&m).await.unwrap();
        assert_ne!(stored.etag, m.etag);
        assert_eq!(stored.etag.len(), 26);
    }
    .boxed()
}

/// `get` of a missing record is `NotFound`; of an empty name `Invalid`.
pub fn get_and_its_refusals(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        fails!(repo.get("articles/missing").await, Error::NotFound);
        fails!(repo.get("").await, Error::Invalid(_));
    }
    .boxed()
}

/// Pages follow the provider's tokens to the end, in the order asked for; `count` counts.
pub fn list_pages_and_counts(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        for (i, id) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            let title = format!("T{id}");
            repo.create(&article(id, &title, i as i32)).await.unwrap();
        }
        let mut input = ListInput {
            page_size: 2,
            ..Default::default()
        };
        let mut names = Vec::new();
        loop {
            let page = repo.list(&input).await.unwrap();
            assert!(page.items.len() <= 2);
            names.extend(page.items.into_iter().map(|a| a.name));
            if page.next_page_token.is_empty() {
                break;
            }
            input.page_token = page.next_page_token;
        }
        let want: Vec<String> = ["a", "b", "c", "d", "e"]
            .map(|id| format!("articles/{id}"))
            .to_vec();
        assert_eq!(names, want);
        assert_eq!(repo.count(&ListInput::default()).await.unwrap(), 5);
        let desc = ListInput {
            order_by: "name desc".into(),
            page_size: 1,
            ..Default::default()
        };
        let first = repo.list(&desc).await.unwrap();
        assert_eq!(first.items[0].name, "articles/e");
        assert!(!first.next_page_token.is_empty());
    }
    .boxed()
}

/// A deleted record is left out of `list` and `count` unless `show_deleted`.
pub fn list_skips_deleted_unless_shown(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        for (id, title) in [("a", "A"), ("b", "B"), ("c", "C")] {
            repo.create(&article(id, title, 1)).await.unwrap();
        }
        repo.delete("articles/b", None).await.unwrap();
        let live = repo.list(&ListInput::default()).await.unwrap();
        let names: Vec<_> = live.items.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, ["articles/a", "articles/c"]);
        assert_eq!(repo.count(&ListInput::default()).await.unwrap(), 2);
        let all = ListInput {
            show_deleted: true,
            ..Default::default()
        };
        assert_eq!(repo.list(&all).await.unwrap().items.len(), 3);
        assert_eq!(repo.count(&all).await.unwrap(), 3);
        let paged = ListInput {
            page_size: 1,
            ..Default::default()
        };
        let first = repo.list(&paged).await.unwrap();
        assert_eq!(first.items[0].name, "articles/a");
        let second = repo
            .list(&ListInput {
                page_token: first.next_page_token,
                ..paged
            })
            .await
            .unwrap();
        assert_eq!(second.items[0].name, "articles/c");
        assert!(second.next_page_token.is_empty());
    }
    .boxed()
}

/// The caller's filter and the soft-delete clause both hold.
pub fn list_filter_composes_with_soft_delete(db: DB) -> BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        for (id, pages) in [("a", 1), ("b", 5), ("c", 9)] {
            repo.create(&article(id, &id.to_uppercase(), pages))
                .await
                .unwrap();
        }
        repo.delete("articles/c", None).await.unwrap();
        let input = |show_deleted| ListInput {
            filter: "pages >= 5".into(),
            show_deleted,
            ..Default::default()
        };
        let live = repo.list(&input(false)).await.unwrap().items;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].name, "articles/b");
        assert_eq!(repo.count(&input(false)).await.unwrap(), 1);
        assert_eq!(repo.count(&input(true)).await.unwrap(), 2);
        fails!(
            repo.list(&ListInput {
                filter: "nope = 1".into(),
                ..Default::default()
            })
            .await,
            Error::Invalid(_)
        );
    }
    .boxed()
}
