//! The repository suite every provider runs: each case a function of a fresh [`DB`], and
//! macros that turn the cases into one `#[tokio::test]` each, run through the provider's own
//! `run`, which makes the database and cleans it up.

#![allow(dead_code)]

use std::time::Duration;

use database::records::DB;
use database::repository::{RecordsRepository, Rules};
use futures::future::BoxFuture;

use super::articles::{self, Article};

/// One case: what it does with a fresh database.
pub type Scenario = fn(DB) -> BoxFuture<'static, ()>;

/// How long a deleted article stays restorable in the suite.
pub const RETENTION: Duration = Duration::from_secs(3600);

/// The articles repository over `db` with `rules`, its table made.
pub async fn open(db: &DB, rules: Rules) -> RecordsRepository<Article> {
    let res = articles::resource("articles");
    db.schema.ensure_schema(&res).await.expect("ensure_schema");
    RecordsRepository::new(db, res, rules).expect("a repository")
}

/// The articles repository with soft delete.
pub async fn soft(db: &DB) -> RecordsRepository<Article> {
    open(db, articles::rules(RETENTION)).await
}

/// Mask paths from string literals.
pub fn paths(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| (*s).to_owned()).collect()
}

/// Asserts `r` failed with the error `pat` matches.
#[macro_export]
macro_rules! fails {
    ($r:expr, $pat:pat) => {{
        let r = $r;
        assert!(matches!(r, Err($pat)), "{} = {r:?}", stringify!($r));
    }};
}

/// One test per case, each run through `$run`.
#[macro_export]
macro_rules! cases {
    ($run:path; $($module:ident :: $case:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() {
                $run(crate::support::$module::$case).await;
            }
        )*
    };
}

/// The cases every provider passes.
#[macro_export]
macro_rules! repository_suite {
    ($run:path) => {
        $crate::cases!($run;
            repo_crud::create_mints_etag_and_audit_times,
            repo_crud::create_replaces_a_callers_etag,
            repo_crud::get_and_its_refusals,
            repo_crud::list_pages_and_counts,
            repo_crud::list_skips_deleted_unless_shown,
            repo_update::mask_writes_only_its_paths,
            repo_update::empty_mask_replaces_every_mutable_field,
            repo_update::output_only_paths_are_ignored,
            repo_update::unknown_paths_are_refused,
            repo_update::oneof_and_message_fields_replace_whole,
            repo_update::etag_guards_update,
            repo_update::update_output_only_writes_only_output_fields,
            repo_delete::soft_delete_marks_and_undelete_restores,
            repo_delete::if_match_guards_delete_and_undelete,
            repo_delete::zero_retention_leaves_expire_time_unset,
            repo_delete::without_soft_delete_delete_removes,
            repo_delete::without_an_etag_if_match_is_ignored,
            repo_more::hooks_run_around_each_call,
            repo_more::new_refuses_what_does_not_fit,
        );
    };
}

/// The cases needing a provider that filters: the caller's filter composed with soft delete.
#[macro_export]
macro_rules! repository_filters {
    ($run:path) => {
        $crate::cases!($run; repo_crud::list_filter_composes_with_soft_delete);
    };
}

/// The cases needing the etag check and the write to be one step (a transaction).
#[macro_export]
macro_rules! repository_races {
    ($run:path) => {
        $crate::cases!($run;
            repo_more::concurrent_updates_with_one_etag_admit_one,
            repo_more::concurrent_deletes_with_one_etag_admit_one,
        );
    };
}
