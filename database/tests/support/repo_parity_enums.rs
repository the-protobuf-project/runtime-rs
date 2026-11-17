//! The enum parity case: an enum stored as its label, as Go's gorm `<Enum>FromProto` stores it.

use database::Error;
use database::records::DB;
use database::repository::{ListInput, Repository};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::super::library::{Novel, novel};
use super::{Raw, novels};
use crate::fails;

/// An enum is stored as its label, read back as its number, and filtered as Go filters an
/// enum (number or value name, `=` and `!=` only); the
/// unspecified zero is stored as null.
pub fn enums_store_their_label(db: DB, raw: Raw, t: String) -> BoxFuture<'static, ()> {
    async move {
        let repo = novels(&db, &t).await;
        let n1 = "publishers/p1/novels/n1";
        let ebook = repo
            .create(&Novel {
                format: 3,
                ..novel("p1", "n1", "Dune")
            })
            .await
            .unwrap();
        assert_eq!(ebook.format, 3);
        assert_eq!(raw("novels", "format", n1).await.as_deref(), Some("EBOOK"));
        let plain = repo.create(&novel("p1", "n2", "Emma")).await.unwrap();
        assert_eq!(plain.format, 0);
        assert_eq!(
            raw("novels", "format", "publishers/p1/novels/n2").await,
            None
        );
        // As Go's enum fields: by number or value name, any case, prefix optional.
        for f in [
            "format = EBOOK",
            "format = ebook",
            "format = FORMAT_EBOOK",
            "format = 3",
        ] {
            let input = ListInput {
                filter: f.into(),
                ..Default::default()
            };
            let found = repo.list(&input).await.unwrap().items;
            assert_eq!(
                found.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
                [n1],
                "{f}"
            );
        }
        let input = ListInput {
            filter: "NOT format = EBOOK".into(),
            ..Default::default()
        };
        assert_eq!(
            repo.count(&input).await.unwrap(),
            1,
            "NOT includes the null one"
        );
        for bad in ["format : EB", "format > 1", "format = SCROLL"] {
            let input = ListInput {
                filter: bad.into(),
                ..Default::default()
            };
            fails!(repo.list(&input).await, Error::Invalid(_));
        }
    }
    .boxed()
}
