//! Records in a Tauri app's store: [`database::records`](crate::records) over
//! [tauri-plugin-store](https://v2.tauri.app/plugin/store/), for desktop and mobile apps that
//! keep their data in a JSON file in the app's data directory. Feature `tauri-store`.
//!
//! runtime-go has no counterpart: Tauri is Rust's. The key layout follows
//! `database::redis`, the other key-value backend.
//!
//! ```ignore
//! tauri::Builder::default()
//!     .plugin(tauri_plugin_store::Builder::default().build())
//!     .setup(|app| {
//!         let provider = TauriStoreProvider::open(app.handle(), "records.json")?;
//!         let db = tauri::async_runtime::block_on(provider.set_database(""))?;
//!         app.manage(db);
//!         Ok(())
//!     })
//! ```
//!
//! What it does, given that the store is one in-memory map saved whole to a file:
//!
//! - **Durability:** each write is saved before it returns ([`TauriStoreProvider::durable`]
//!   turns that off). The plugin writes the file in place, not through a rename, so a crash
//!   during a save can leave it truncated.
//! - **Uniqueness:** a unique column's value is reserved under its own key, checked and
//!   written under the store's lock, so `AlreadyExists` holds under concurrent writers in
//!   the process. Another process writing the same file is not coordinated with.
//! - **Transactions:** the work holds the lock; it commits with one save and rolls back by
//!   restoring the selected database's keys. Nesting is refused.
//! - **Listing:** filters, `order_by` and paging run in memory over the resource's records,
//!   with the AIP-160 tree every backend shares, evaluated by
//!   [`Expr::matches`](crate::records::core::filter::Expr::matches). Nulls sort first.
//! - **Bulk:** `create_many` is all or nothing, in one save.
//! - **Watching:** refused; the plugin's change events go to the webview.

mod codec;
mod driver;
mod keys;
mod list;
mod ops;
mod provider;
mod tx;

pub use driver::TauriStoreDriver;
pub use provider::TauriStoreProvider;
