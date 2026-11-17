//! The repository suite on the Tauri store, over Tauri's mock runtime: every case, its
//! transactions holding the store's lock so racing writers take turns.

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

use database::records::Provider;
use database::tauri_store::TauriStoreProvider;
use support::repo_suite::Scenario;
use tauri::test::{mock_builder, mock_context, noop_assets};

async fn run(case: Scenario) {
    let app = mock_builder()
        .plugin(tauri_plugin_store::Builder::default().build())
        .build(mock_context(noop_assets()))
        .expect("a mock app");
    let dir = std::env::temp_dir().join(format!("tpp-repository-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = TauriStoreProvider::open(app.handle(), dir.join("records.json")).unwrap();
    case(provider.set_database("").await.unwrap()).await;
    let _ = std::fs::remove_dir_all(&dir);
}

crate::repository_suite!(run);
crate::repository_filters!(run);
crate::repository_races!(run);
