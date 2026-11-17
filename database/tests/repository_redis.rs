//! The repository suite against a live Redis, each case under a key prefix of its own,
//! removed afterwards. Redis has no filters and no transactions: soft-deleted records are
//! left out of a listing by the repository scanning, a caller's filter is refused, and the
//! etag check and the write are two steps, so the racing cases do not run here.
//!
//! ```text
//! docker compose -p runtime-database-repository-redis -f database/docker/compose.redis.live.yaml up -d --wait
//! cargo test -p tpp-database --features redis,live-tests --test repository_redis
//! docker compose -p runtime-database-repository-redis -f database/docker/compose.redis.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_REDIS_ADDRESS` defaults to `127.0.0.1:16381`.

mod support {
    #[path = "articles.rs"]
    pub mod articles;
    #[path = "repo_crud.rs"]
    pub mod repo_crud;
    #[path = "repo_delete.rs"]
    pub mod repo_delete;
    #[path = "repo_more.rs"]
    pub mod repo_more;
    #[path = "repo_suite.rs"]
    pub mod repo_suite;
    #[path = "repo_update.rs"]
    pub mod repo_update;
}

use std::panic::AssertUnwindSafe;

use database::Error;
use database::records::{DB, Provider};
use database::redis::RedisProvider;
use database::repository::{ListInput, Repository};
use futures::FutureExt;
use support::repo_suite::{Scenario, soft};
use support::{articles, articles::article};

async fn run(case: Scenario) {
    let at = std::env::var("RUNTIME_DATABASE_REDIS_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:16381".into());
    let client = redis::Client::open(format!("redis://{at}/")).expect("a valid address");
    let conn = redis::aio::ConnectionManager::new(client)
        .await
        .unwrap_or_else(|e| panic!("no Redis at {at}: {e}"));
    let prefix = format!("repo-{}", uuid::Uuid::new_v4().simple());
    let db: DB = RedisProvider::new(conn)
        .with_prefix(prefix)
        .set_database("tenant")
        .await
        .unwrap();
    let outcome = AssertUnwindSafe(case(db.clone())).catch_unwind().await;
    let _ = db.schema.drop_schema(&articles::resource("articles")).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

crate::repository_suite!(run);

fn filters_are_refused(db: DB) -> futures::future::BoxFuture<'static, ()> {
    async move {
        let repo = soft(&db).await;
        repo.create(&article("a", "A", 1)).await.unwrap();
        let input = ListInput {
            filter: "pages = 1".into(),
            ..Default::default()
        };
        let refused = repo.list(&input).await;
        assert!(
            matches!(refused, Err(Error::Unimplemented { .. })),
            "{refused:?}"
        );
    }
    .boxed()
}

#[tokio::test]
async fn a_callers_filter_is_refused() {
    run(filters_are_refused).await;
}
