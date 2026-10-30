//! The interceptor that rewrites a handler's error with its [`Translator`].

use tonic::Status;

use super::Translator;
use crate::intercept::{Interceptor, Next, UnaryCall, UnaryReply};

/// Translates every error a handler returns with [`Translator::translate`], so handlers can
/// return their [`super::Problem`]s and sentinel errors as they are. A reply passes through
/// untouched.
///
/// Register it with [`crate::HybridServer::with_interceptor`].
#[derive(Debug, Clone)]
pub struct ProblemInterceptor {
    translator: Translator,
}

impl ProblemInterceptor {
    /// Translates with `translator`.
    #[must_use]
    pub fn new(translator: Translator) -> Self {
        Self { translator }
    }
}

#[async_trait::async_trait]
impl Interceptor for ProblemInterceptor {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        next.run(call)
            .await
            .map_err(|status| self.translator.translate(status))
    }
}
