//! The D1 client alone, with no descriptor and no proto anywhere: SQL in, rows out, against
//! the same stand-in API over in-process SQLite that `tests/d1.rs` uses.

mod support {
    #[path = "d1_fake.rs"]
    pub mod d1_fake;
}

use database::Error;
use database::d1::Statement;
use support::d1_fake::{MAIN, serve};

#[tokio::test]
async fn d1_client_runs_sql_without_proto() {
    let (_, client) = serve(&[("main", MAIN)]).await;
    let create = Statement::new(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, words INTEGER)",
        vec![],
    );
    client.query(MAIN, &[create]).await.unwrap();

    let put = |id: &str, words: &str| {
        Statement::new(
            "INSERT INTO notes VALUES (?, ?)",
            vec![id.into(), words.into()],
        )
    };
    let done = client
        .query(MAIN, &[put("a", "3"), put("b", "10")])
        .await
        .unwrap();
    assert_eq!(done.len(), 2);
    assert_eq!(done[0].changes, 1);

    let select = Statement::new("SELECT id, words FROM notes ORDER BY words DESC", vec![]);
    let rows = client.query(MAIN, &[select]).await.unwrap().remove(0).rows;
    // Bound as text, stored as numbers: 10 sorts above 3.
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "b");
    assert_eq!(rows[0]["words"], 10);

    let dup = client.query(MAIN, &[put("a", "1")]).await;
    assert!(matches!(dup, Err(Error::AlreadyExists)), "{dup:?}");
    assert!(client.ping("nope").await.is_err());
    assert_eq!(client.lookup("main").await.unwrap(), MAIN);
}
