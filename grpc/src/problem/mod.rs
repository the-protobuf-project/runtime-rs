//! Turns a service's typed errors into `google.rpc.Status` errors a developer can act on
//! (AIP-193): a status code, a message that says what is wrong and what to do, an
//! `ErrorInfo` with a stable reason, and the detail the error calls for — `BadRequest` for
//! a field, `PreconditionFailure` for a blocked call, `ResourceInfo` for a missing
//! resource.
//!
//! The vocabulary is the service's: it registers its sentinel errors, each with a status
//! code and a default reason, on a [`Translator`], declares the reasons it may return, and
//! returns [`Problem`] values (or errors wrapping its sentinels) from its handlers. Nothing
//! here names a reason of its own.
//!
//! ```
//! use grpc::problem::{Kind, Problem, Translator};
//! use tonic::Code;
//!
//! let errs = Translator::new("shop.example.com", [
//!     Kind::new("not found", Code::NotFound, "NOT_FOUND"),
//!     Kind::new("sold out", Code::FailedPrecondition, "SOLD_OUT"),
//! ]);
//! let status = errs.to_status(
//!     &Problem::new("sold out")
//!         .reason("SHELF_EMPTY")
//!         .resource("shop.example.com/Shelf", "shelves/s")
//!         .what("shelf shelves/s is empty")
//!         .fix("Restock it first"),
//! );
//! assert_eq!(status.code(), Code::FailedPrecondition);
//! assert_eq!(status.message(), "Shelf shelves/s is empty. Restock it first.");
//! ```
//!
//! # Differences from Go
//!
//! Go's sentinels are error values matched by identity with `errors.Is`. Rust errors have
//! no identity to match, and a sentinel crossing tonic becomes text anyway, so a [`Kind`]
//! names its sentinel by its message: an error matches when its text, or the text of an
//! error in its [`source`](std::error::Error::source) chain, is the sentinel or wraps it the
//! way Go's `"%w: ..."` does (`"not found: shelf s"`).
//!
//! A tonic handler can only return a [`Status`], so the interceptor
//! ([`Translator::interceptor`]) works on statuses: a [`Problem`] converted with `into()`
//! travels as a status the interceptor recognises and renders, and a plain error converted
//! with [`Status::from_error`] arrives as `UNKNOWN` — tonic's code for "not a status" — and
//! is classified by its sentinel. Any other status passes through, as in Go.

mod interceptor;

use std::collections::{BTreeMap, HashMap};
use std::error::Error as StdError;
use std::sync::Arc;

use tonic::{Code, Status};
use tonic_types::{
    BadRequest, ErrorDetail, ErrorInfo, FieldViolation, PreconditionFailure, PreconditionViolation,
    ResourceInfo, StatusExt,
};

pub use interceptor::ProblemInterceptor;

/// The `ErrorInfo` domain marking a status that carries an untranslated [`Problem`] from a
/// handler to the interceptor. It never reaches a caller through [`ProblemInterceptor`].
const CARRIER_DOMAIN: &str = "problem.tpp-grpc.internal";
/// The `ErrorInfo` reason of that carrier.
const CARRIER_REASON: &str = "UNTRANSLATED_PROBLEM";
/// The prefix the carrier gives a problem's own metadata, to keep it apart from its fields.
const CARRIER_METADATA: &str = "metadata.";

/// An error a caller can act on. It names one of the service's sentinels, `kind`, which
/// decides its status code.
///
/// Return it from a handler with `Err(problem.into())`; the [`ProblemInterceptor`]
/// renders it. Build it with [`Problem::new`] and the chained setters, or as a struct
/// literal over [`Default`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Problem {
    /// The sentinel registered on the [`Translator`], by its message. `None` takes the code
    /// of a declared [`Reason`].
    pub kind: Option<String>,
    /// The stable, machine-readable cause (`ErrorInfo.reason`). Empty uses the kind's
    /// default reason.
    pub reason: String,
    /// What is wrong, in one sentence, naming the values involved.
    pub what: String,
    /// What to do about it. Empty only when the caller can do nothing.
    pub fix: String,
    /// The request field at fault (a `BadRequest` field violation).
    pub field: String,
    /// The type of the resource missing or in the way (`ResourceInfo`).
    pub resource_type: String,
    /// The name of the resource missing or in the way (`ResourceInfo`, or
    /// `PreconditionFailure` on `FAILED_PRECONDITION`).
    pub resource_name: String,
    /// Extra facts for `ErrorInfo.metadata`.
    pub metadata: HashMap<String, String>,
}

impl Problem {
    /// A problem of the sentinel `kind`.
    #[must_use]
    pub fn new(kind: impl ToString) -> Self {
        Self {
            kind: Some(kind.to_string()),
            ..Self::default()
        }
    }

    /// Sets the reason.
    #[must_use]
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }

    /// Sets what is wrong.
    #[must_use]
    pub fn what(mut self, what: impl Into<String>) -> Self {
        self.what = what.into();
        self
    }

    /// Sets what to do about it.
    #[must_use]
    pub fn fix(mut self, fix: impl Into<String>) -> Self {
        self.fix = fix.into();
        self
    }

    /// Sets the request field at fault.
    #[must_use]
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = field.into();
        self
    }

    /// Sets the resource missing or in the way.
    #[must_use]
    pub fn resource(mut self, kind: impl Into<String>, name: impl Into<String>) -> Self {
        self.resource_type = kind.into();
        self.resource_name = name.into();
        self
    }

    /// Adds one fact to `ErrorInfo.metadata`.
    #[must_use]
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Recovers a problem a handler returned with `into()`, from the status carrying it.
    fn from_carrier(status: &Status) -> Option<Self> {
        let info = status.get_details_error_info()?;
        if info.domain != CARRIER_DOMAIN || info.reason != CARRIER_REASON {
            return None;
        }
        let mut meta = info.metadata;
        let mut take = |key: &str| meta.remove(key).unwrap_or_default();
        let mut problem = Self {
            kind: Some(take("kind")).filter(|k| !k.is_empty()),
            reason: take("reason"),
            what: take("what"),
            fix: take("fix"),
            field: take("field"),
            resource_type: take("resource_type"),
            resource_name: take("resource_name"),
            metadata: HashMap::new(),
        };
        problem.metadata = meta
            .into_iter()
            .filter_map(|(k, v)| k.strip_prefix(CARRIER_METADATA).map(|k| (k.to_string(), v)))
            .collect();
        Some(problem)
    }
}

impl std::fmt::Display for Problem {
    /// What, then fix.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.fix.is_empty() {
            return f.write_str(&self.what);
        }
        write!(
            f,
            "{}. {}",
            self.what.strip_suffix('.').unwrap_or(&self.what),
            self.fix
        )
    }
}

impl StdError for Problem {}

impl From<Problem> for Status {
    /// Carries the problem to the [`ProblemInterceptor`], which renders it with the
    /// translator's codes, reasons and domain.
    ///
    /// Without the interceptor installed the caller sees `UNKNOWN` and the problem's
    /// message — the same thing a plain error converted with [`Status::from_error`] shows.
    fn from(problem: Problem) -> Self {
        let message = problem.to_string();
        let mut meta: HashMap<String, String> = problem
            .metadata
            .into_iter()
            .map(|(k, v)| (format!("{CARRIER_METADATA}{k}"), v))
            .collect();
        for (key, value) in [
            ("kind", problem.kind.unwrap_or_default()),
            ("reason", problem.reason),
            ("what", problem.what),
            ("fix", problem.fix),
            ("field", problem.field),
            ("resource_type", problem.resource_type),
            ("resource_name", problem.resource_name),
        ] {
            meta.insert(key.to_string(), value);
        }
        Status::with_error_details_vec(
            Code::Unknown,
            message,
            vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
                CARRIER_REASON,
                CARRIER_DOMAIN,
                meta,
            ))],
        )
    }
}

/// Binds a sentinel error to the status code and default reason of every error wrapping
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kind {
    /// The sentinel's message, which is how an error is matched to it.
    pub sentinel: String,
    /// The status code of every error wrapping it.
    pub code: Code,
    /// The default `ErrorInfo` reason of every error wrapping it.
    pub reason: String,
}

impl Kind {
    /// Binds `sentinel` — anything whose text is the sentinel's message, such as an error
    /// enum variant — to `code` and `reason`.
    #[must_use]
    pub fn new(sentinel: impl ToString, code: Code, reason: impl Into<String>) -> Self {
        Self {
            sentinel: sentinel.to_string(),
            code,
            reason: reason.into(),
        }
    }
}

/// A reason a service may return, declared so it can be documented and so a [`Problem`]
/// with no kind takes its code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reason {
    /// The reason, `ErrorInfo.reason`.
    pub name: String,
    /// The status code of a problem with this reason and no kind.
    pub code: Code,
    /// What the reason means, for documentation.
    pub description: String,
}

impl Reason {
    /// Declares `name` with `code` and `description`.
    #[must_use]
    pub fn new(name: impl Into<String>, code: Code, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            code,
            description: description.into(),
        }
    }
}

/// A translation tried before the registered kinds; see [`Translator::handle`].
type Handler = Arc<dyn Fn(&(dyn StdError + 'static)) -> Option<Status> + Send + Sync>;

/// Maps a service's errors onto statuses. Build it once at start-up; it is cheap to clone
/// and safe to share after.
#[derive(Clone)]
pub struct Translator {
    domain: String,
    kinds: Vec<Kind>,
    strip: Vec<String>,
    handlers: Vec<Handler>,
    reasons: BTreeMap<String, Reason>,
}

impl std::fmt::Debug for Translator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Translator")
            .field("domain", &self.domain)
            .field("kinds", &self.kinds)
            .field("strip", &self.strip)
            .field("handlers", &self.handlers.len())
            .field("reasons", &self.reasons)
            .finish()
    }
}

impl Translator {
    /// A translator whose `ErrorInfo` details carry `domain`, mapping `kinds` in order: the
    /// first whose sentinel the error wraps decides.
    #[must_use]
    pub fn new(domain: impl Into<String>, kinds: impl IntoIterator<Item = Kind>) -> Self {
        let kinds: Vec<Kind> = kinds.into_iter().collect();
        Self {
            domain: domain.into(),
            strip: kinds.iter().map(|k| k.sentinel.clone()).collect(),
            kinds,
            handlers: Vec::new(),
            reasons: BTreeMap::new(),
        }
    }

    /// The `ErrorInfo` domain.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Adds sentinels whose text `"%w: ..."`-style wrapping puts in front of a message,
    /// beyond the registered kinds', to drop from messages.
    #[must_use]
    pub fn strip<S: ToString>(mut self, sentinels: impl IntoIterator<Item = S>) -> Self {
        self.strip
            .extend(sentinels.into_iter().map(|s| s.to_string()));
        self
    }

    /// Adds a translation tried, in order, after [`Problem`] and before the registered
    /// kinds: it returns the status for the error, or `None` to pass. Use it for the typed
    /// errors of a library — a store's reference errors.
    #[must_use]
    pub fn handle(
        mut self,
        handler: impl Fn(&(dyn StdError + 'static)) -> Option<Status> + Send + Sync + 'static,
    ) -> Self {
        self.handlers.push(Arc::new(handler));
        self
    }

    /// Records reasons the service may return.
    #[must_use]
    pub fn declare(mut self, reasons: impl IntoIterator<Item = Reason>) -> Self {
        for reason in reasons {
            self.reasons.insert(reason.name.clone(), reason);
        }
        self
    }

    /// The declared reasons, sorted by name.
    #[must_use]
    pub fn reasons(&self) -> Vec<Reason> {
        self.reasons.values().cloned().collect()
    }

    /// The message-level interceptor that translates every error a handler returns, so
    /// handlers can return their typed errors as they are. Go's `Translator.Interceptor`.
    #[must_use]
    pub fn interceptor(&self) -> ProblemInterceptor {
        ProblemInterceptor::new(self.clone())
    }

    /// Maps `err` onto a status a developer can act on.
    ///
    /// A [`Problem`] becomes its message — what is wrong, then what to do — with an
    /// `ErrorInfo` carrying its reason and the detail its kind calls for. Any other error
    /// wrapping a registered sentinel keeps its message, less the sentinel's own text
    /// (`"not found: ..."`), which only repeats the status code. Errors that are already
    /// statuses pass through; anything else is `INTERNAL`.
    #[must_use]
    pub fn to_status(&self, err: &(dyn StdError + 'static)) -> Status {
        for current in chain(err) {
            if let Some(problem) = current.downcast_ref::<Problem>() {
                return self.problem_status(problem);
            }
            if let Some(problem) = current
                .downcast_ref::<Status>()
                .and_then(Problem::from_carrier)
            {
                return self.problem_status(&problem);
            }
        }
        if let Some(status) = self.handled(err) {
            return status;
        }
        if let Some((code, reason)) = self.classify(err) {
            return with_reason(code, sentence(&self.plain(err)), reason, &self.domain);
        }
        if let Some(status) = chain(err).find_map(|e| e.downcast_ref::<Status>()) {
            return status.clone();
        }
        tracing::debug!(error = %err, "problem: an unregistered error becomes INTERNAL");
        Status::internal(err.to_string())
    }

    /// Translates a status a handler ended with — the interceptor's half of
    /// [`Translator::to_status`].
    ///
    /// A carried [`Problem`] is rendered; then the handlers are tried; an `UNKNOWN` status,
    /// which is what tonic makes of an error that was not a status, is classified by its
    /// sentinel or becomes `INTERNAL`; and any other status passes through.
    #[must_use]
    pub fn translate(&self, status: Status) -> Status {
        if let Some(problem) = Problem::from_carrier(&status) {
            return self.problem_status(&problem);
        }
        if let Some(out) = self.handled(&status) {
            return out;
        }
        if status.code() != Code::Unknown {
            return status;
        }
        match self.classify_message(status.message()) {
            Some((code, reason)) => with_reason(
                code,
                sentence(&self.plain_message(status.message())),
                reason,
                &self.domain,
            ),
            None => {
                tracing::debug!(
                    message = status.message(),
                    "problem: an unregistered error becomes INTERNAL"
                );
                Status::internal(status.message().to_string())
            }
        }
    }

    /// The status code and default reason of the sentinel `err` wraps, or `None` when it
    /// wraps none of the registered kinds.
    #[must_use]
    pub fn classify(&self, err: &(dyn StdError + 'static)) -> Option<(Code, String)> {
        self.kinds.iter().find_map(|kind| {
            chain(err)
                .any(|e| wraps(&message_of(e), &kind.sentinel))
                .then(|| (kind.code, kind.reason.clone()))
        })
    }

    /// `err`'s message without the sentinel texts in front of it.
    #[must_use]
    pub fn plain(&self, err: &(dyn StdError + 'static)) -> String {
        self.plain_message(&message_of(err))
    }

    fn classify_message(&self, message: &str) -> Option<(Code, String)> {
        self.kinds
            .iter()
            .find(|kind| wraps(message, &kind.sentinel))
            .map(|kind| (kind.code, kind.reason.clone()))
    }

    fn plain_message(&self, message: &str) -> String {
        let mut msg = message;
        let mut again = true;
        while again {
            again = false;
            for sentinel in &self.strip {
                if let Some(rest) = msg
                    .strip_prefix(sentinel.as_str())
                    .and_then(|rest| rest.strip_prefix(": "))
                {
                    msg = rest;
                    again = true;
                }
            }
        }
        msg.to_string()
    }

    fn handled(&self, err: &(dyn StdError + 'static)) -> Option<Status> {
        self.handlers.iter().find_map(|handler| handler(err))
    }

    /// Renders a problem: its message, an `ErrorInfo`, and the detail its kind calls for.
    fn problem_status(&self, problem: &Problem) -> Status {
        let classified = problem
            .kind
            .as_deref()
            .and_then(|kind| self.classify_message(kind));
        let (mut code, mut reason) = classified.clone().unwrap_or((Code::Unknown, String::new()));
        if !problem.reason.is_empty() {
            reason.clone_from(&problem.reason);
            if let (None, Some(declared)) = (&classified, self.reasons.get(&reason)) {
                code = declared.code;
            }
        }

        let text = problem.to_string();
        let mut details = vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
            reason.clone(),
            self.domain.clone(),
            problem.metadata.clone(),
        ))];
        if !problem.field.is_empty() {
            details.push(ErrorDetail::BadRequest(BadRequest::new(vec![
                FieldViolation {
                    reason: reason.clone(),
                    ..FieldViolation::new(problem.field.clone(), text.clone())
                },
            ])));
        } else if code == Code::FailedPrecondition && !problem.resource_name.is_empty() {
            details.push(ErrorDetail::PreconditionFailure(PreconditionFailure::new(
                vec![PreconditionViolation::new(
                    reason.clone(),
                    problem.resource_name.clone(),
                    text.clone(),
                )],
            )));
        } else if !problem.resource_name.is_empty() {
            details.push(ErrorDetail::ResourceInfo(ResourceInfo::new(
                problem.resource_type.clone(),
                problem.resource_name.clone(),
                "",
                problem.fix.clone(),
            )));
        }
        tracing::debug!(?code, %reason, "problem: rendered");
        Status::with_error_details_vec(code, sentence(&text), details)
    }
}

/// A status of `code` and `message` with an `ErrorInfo` of `reason` in `domain`, so
/// clients can branch on a stable reason instead of parsing the message.
#[must_use]
pub fn with_reason(
    code: Code,
    message: impl Into<String>,
    reason: impl Into<String>,
    domain: impl Into<String>,
) -> Status {
    Status::with_error_details_vec(
        code,
        message,
        vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
            reason,
            domain,
            HashMap::new(),
        ))],
    )
}

/// The `ErrorInfo` reason a status carries, or `None`. Go's `ReasonOf`, which returns `""`
/// for the same case.
#[must_use]
pub fn reason_of(status: &Status) -> Option<String> {
    status.get_details_error_info().map(|info| info.reason)
}

/// Makes `message` read as one sentence: a capital first letter and a closing full stop,
/// so every message an API returns is written the same way whoever wrote it. A first word
/// that is not plain lower case — a field name such as `update_mask`, a resource name — is
/// left as it is spelled.
#[must_use]
pub fn sentence(message: &str) -> String {
    let msg = message.trim();
    if msg.is_empty() {
        return String::new();
    }
    let first = msg.split(' ').next().unwrap_or_default();
    let mut out = if first.chars().all(char::is_lowercase) {
        let mut chars = msg.chars();
        chars
            .next()
            .map(|c| c.to_uppercase().chain(chars).collect())
            .unwrap_or_default()
    } else {
        msg.to_string()
    };
    if !out.ends_with('.') && !out.ends_with('?') {
        out.push('.');
    }
    out
}

/// An error and every error in its source chain, outermost first.
fn chain<'a>(
    err: &'a (dyn StdError + 'static),
) -> impl Iterator<Item = &'a (dyn StdError + 'static)> {
    std::iter::successors(Some(err), |e| (*e).source())
}

/// The text an error says, without tonic's `status: ..., message: ...` framing.
fn message_of(err: &(dyn StdError + 'static)) -> String {
    match err.downcast_ref::<Status>() {
        Some(status) => status.message().to_string(),
        None => err.to_string(),
    }
}

/// Whether `message` is `sentinel`, or wraps it the way `"%w: ..."` does.
fn wraps(message: &str, sentinel: &str) -> bool {
    if sentinel.is_empty() {
        return false;
    }
    message == sentinel
        || message.starts_with(&format!("{sentinel}: "))
        || message.contains(&format!(": {sentinel}: "))
        || message.ends_with(&format!(": {sentinel}"))
}

#[cfg(test)]
mod tests;
