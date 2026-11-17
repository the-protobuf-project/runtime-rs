//! The Tauri store backend on Tauri's mock runtime, each store a file in its own temp
//! directory: CRUD and its outcomes, uniqueness under concurrency, listing, transactions,
//! bulk, tenants, and what reaches the file.

#[path = "../support/filter_conformance.rs"]
mod filter_conformance;
#[path = "../support/records.rs"]
mod support;

mod crud;
mod list;
mod tx;

use std::path::PathBuf;

use database::records::{DB, Provider};
use database::tauri_store::TauriStoreProvider;
use tauri::test::{MockRuntime, mock_builder, mock_context, noop_assets};

pub(crate) fn app() -> tauri::App<MockRuntime> {
    mock_builder()
        .plugin(tauri_plugin_store::Builder::default().build())
        .build(mock_context(noop_assets()))
        .expect("a mock app")
}

/// A provider over a fresh store file, the app that owns it, and the file's path.
pub(crate) fn provider() -> (
    tauri::App<MockRuntime>,
    TauriStoreProvider<MockRuntime>,
    PathBuf,
) {
    let app = app();
    let dir = std::env::temp_dir().join(format!("tpp-tauri-store-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("records.json");
    let provider = TauriStoreProvider::open(app.handle(), &path).unwrap();
    (app, provider, path)
}

pub(crate) async fn db(p: &TauriStoreProvider<MockRuntime>) -> DB {
    p.set_database("").await.unwrap()
}

pub(crate) fn on_disk(path: &PathBuf) -> serde_json::Map<String, serde_json::Value> {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
