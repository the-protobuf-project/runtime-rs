//! Transactions, schema, bulk and change streams. Go's `TestTransactionCommitsAndRollsBack`,
//! `TestSchemaLifecycle`, `TestBulk` and `TestWatchDeliversChanges`.

use std::time::Duration;

use database::Error;
use database::records::{Change, ChangeKind, ListOptions, TxWork, WatchOptions};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};

use crate::support::{book, string_field};
use crate::with_db;

fn work<F>(f: F) -> TxWork
where
    F: for<'a> FnOnce(
            &'a database::records::DB,
        ) -> futures::future::BoxFuture<'a, database::Result<()>>
        + Send
        + 'static,
{
    Box::new(f)
}

#[tokio::test]
async fn transaction_commits_and_rolls_back() {
    with_db(|f| async move {
        let res = f.res.clone();
        let r = res.clone();
        let committed = f.db.tx.run(work(move |tx| {
            async move {
                tx.create(&r, &book("a", "A", 1)).await?;
                tx.batch.create_many(&r, &[book("b", "B", 2)]).await?;
                // Reads inside see the transaction's own writes.
                assert_eq!(tx.count(&r, &ListOptions::default()).await?, 2);
                Ok(())
            }
            .boxed()
        }));
        committed.await.unwrap();
        assert_eq!(f.db.count(&res, &ListOptions::default()).await.unwrap(), 2);

        let r = res.clone();
        let rolled = f.db.tx.run(work(move |tx| {
            async move {
                tx.create(&r, &book("c", "C", 3)).await?;
                tx.batch.create_many(&r, &[book("d", "D", 4)]).await?;
                Err(Error::Invalid("second write failed".into()))
            }
            .boxed()
        }));
        let err = rolled.await.unwrap_err();
        assert!(err.to_string().contains("second write failed"), "{err}");
        assert_eq!(
            f.db.count(&res, &ListOptions::default()).await.unwrap(),
            2,
            "the transaction leaked a write"
        );

        let nested = f.db.tx.run(work(|tx| {
            async move { tx.tx.run(work(|_| async { Ok(()) }.boxed())).await }.boxed()
        }));
        assert!(matches!(nested.await, Err(Error::Invalid(_))));
    })
    .await;
}

#[tokio::test]
async fn schema_lifecycle() {
    with_db(|f| async move {
        assert!(f.db.schema.has_schema(&f.res).await.unwrap());
        // Idempotent: it runs at every startup.
        for _ in 0..3 {
            f.db.schema.ensure_schema(&f.res).await.unwrap();
        }
        f.db.schema.drop_schema(&f.res).await.unwrap();
        assert!(!f.db.schema.has_schema(&f.res).await.unwrap());
    })
    .await;
}

#[tokio::test]
async fn bulk() {
    with_db(|f| async move {
        let msgs = [book("a", "A", 1), book("b", "B", 2), book("c", "C", 3)];
        let out = f.db.batch.create_many(&f.res, &msgs).await.unwrap();
        assert_eq!(out.len(), 3);
        assert!(out[0].message.get_field_by_name("update_time").is_some());
        assert!(
            f.db.batch
                .create_many(&f.res, &[])
                .await
                .unwrap()
                .is_empty()
        );

        let keys = ["books/a", "books/missing", "books/c", "books/a"].map(String::from);
        let got = f.db.batch.get_many(&f.res, &keys).await.unwrap();
        assert_eq!(got.len(), 4);
        assert!(got[1].is_none(), "a missing record is None, not an error");
        assert_eq!(string_field(got[0].as_ref().unwrap(), "name"), "books/a");
        assert_eq!(string_field(got[2].as_ref().unwrap(), "name"), "books/c");
        assert_eq!(string_field(got[3].as_ref().unwrap(), "name"), "books/a");

        let clash = [book("d", "D", 4), book("a", "Again", 5), book("e", "E", 6)];
        let err = f.db.batch.create_many(&f.res, &clash).await;
        assert!(matches!(err, Err(Error::AlreadyExists)), "{err:?}");
        // Ordered: what preceded the clash is written, what followed it is not.
        assert!(f.db.exists(&f.res, "books/d").await.unwrap());
        assert!(!f.db.exists(&f.res, "books/e").await.unwrap());
    })
    .await;
}

/// The next change, within ten seconds.
async fn next(changes: &mut BoxStream<'static, database::Result<Change>>) -> Change {
    tokio::time::timeout(Duration::from_secs(10), changes.next())
        .await
        .expect("a change within 10s")
        .expect("the stream is open")
        .unwrap()
}

/// The capability that makes MongoDB worth reaching for when something has to react to
/// writes: the alternative is polling.
#[tokio::test]
async fn watch_delivers_changes() {
    with_db(|f| async move {
        let (db, res) = (&f.db, &f.res);
        let mut changes = db.watch.watch(res, WatchOptions::default()).await.unwrap();
        db.create(res, &book("a", "A", 1)).await.unwrap();
        let created = next(&mut changes).await;
        assert_eq!(created.kind, ChangeKind::Created);
        assert_eq!(created.key, "books/a");
        assert_eq!(
            string_field(created.message.as_ref().unwrap(), "title"),
            "A"
        );
        assert!(
            !created.resume.is_empty(),
            "a change carries a resume token"
        );

        db.update(res, &book("a", "A2", 2)).await.unwrap();
        db.delete(res, "books/a").await.unwrap();
        let updated = next(&mut changes).await;
        assert_eq!(updated.kind, ChangeKind::Updated);
        assert_eq!(
            string_field(updated.message.as_ref().unwrap(), "title"),
            "A2"
        );
        let deleted = next(&mut changes).await;
        assert_eq!(
            (deleted.kind, deleted.key.as_str()),
            (ChangeKind::Deleted, "books/a")
        );
        assert!(deleted.message.is_none());
        drop(changes);

        // Resuming after the insert replays what followed it.
        let opts = WatchOptions {
            resume: created.resume.clone(),
        };
        let mut resumed = db.watch.watch(res, opts).await.unwrap();
        assert_eq!(next(&mut resumed).await.kind, ChangeKind::Updated);
        assert_eq!(next(&mut resumed).await.kind, ChangeKind::Deleted);

        let junk = WatchOptions {
            resume: "not a token".into(),
        };
        assert!(matches!(
            db.watch.watch(res, junk).await,
            Err(Error::Invalid(_))
        ));
    })
    .await;
}
