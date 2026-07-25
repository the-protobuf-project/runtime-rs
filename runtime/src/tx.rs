use network::{BatchOp, GraphQLClient, Result};

/// Batches several mutations into one GraphQL document so they commit atomically — the engine
/// runs all of them in a single transaction (all succeed or all roll back), removing the need for
/// best-effort compensation when a write spans tables. Build one from a [`GraphQLClient`], queue
/// ops, then commit:
///
/// ```no_run
/// # use network::{BatchOp, FieldArg};
/// # use runtime::Tx;
/// # async fn example(gql: &network::GraphQLClient) -> runtime::Result<()> {
/// let mut tx = Tx::new(gql);
/// tx.add(BatchOp { field: "insertPromocode".into(), args: vec![], selection: "{ id }".into() });
/// tx.add(BatchOp { field: "insertBookingMoney".into(), args: vec![], selection: "{ id }".into() });
/// let results = tx.commit().await?; // results[0] and results[1] are the decoded per-op responses
/// # Ok(())
/// # }
/// ```
///
/// Unlike Go's `Tx.Commit`, which fills pre-registered `BatchOp.Result` pointers via reflection
/// and returns only an error, `commit` returns the decoded results directly (in the order queued)
/// since Rust has no equivalent in-place pointer mutation across heterogeneous result types — see
/// [`GraphQLClient::batch_mutate`].
pub struct Tx<'a> {
    gql: &'a GraphQLClient,
    ops: Vec<BatchOp>,
}

impl<'a> Tx<'a> {
    /// Returns a `Tx` that will commit through `gql`.
    pub fn new(gql: &'a GraphQLClient) -> Self {
        Self {
            gql,
            ops: Vec::new(),
        }
    }

    /// Queues a mutation op and returns the `Tx` for chaining.
    pub fn add(&mut self, op: BatchOp) -> &mut Self {
        self.ops.push(op);
        self
    }

    /// Reports how many ops are queued.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Reports whether no ops are queued.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Runs every queued op as one atomic GraphQL mutation and returns the decoded results in
    /// input order. An empty `Tx` commits nothing and returns an empty vector.
    pub async fn commit(&self) -> Result<Vec<serde_json::Value>> {
        if self.ops.is_empty() {
            return Ok(Vec::new());
        }
        self.gql.batch_mutate(&self.ops).await
    }
}
