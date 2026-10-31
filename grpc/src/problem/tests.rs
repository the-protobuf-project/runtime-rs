use super::*;

const INVALID_ARGUMENT: &str = "invalid argument";
const NOT_FOUND: &str = "not found";
const INVALID_STATE: &str = "invalid state for the requested transition";
const FOREIGN_KEY: &str = "foreign key violation";

/// Stands in for a store library's typed error.
#[derive(Debug)]
struct RefError(String);

impl std::fmt::Display for RefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{FOREIGN_KEY}: {} is referenced", self.0)
    }
}

impl StdError for RefError {}

/// An error that wraps another, the way `fmt.Errorf("...: %w", err)` does.
#[derive(Debug)]
struct Wrapped(String, Box<dyn StdError + Send + Sync>);

impl std::fmt::Display for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0, self.1)
    }
}

impl StdError for Wrapped {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.1.as_ref())
    }
}

/// A plain message, as `errors.New` makes one.
#[derive(Debug)]
struct Plain(String);

impl std::fmt::Display for Plain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for Plain {}

fn translator() -> Translator {
    Translator::new(
        "shop.example.com",
        [
            Kind::new(NOT_FOUND, Code::NotFound, "NOT_FOUND"),
            Kind::new(INVALID_STATE, Code::FailedPrecondition, "INVALID_STATE"),
            Kind::new(INVALID_ARGUMENT, Code::InvalidArgument, "INVALID_ARGUMENT"),
        ],
    )
    .strip([FOREIGN_KEY])
    .handle(|err| {
        std::iter::successors(Some(err), |e| (*e).source())
            .find_map(|e| e.downcast_ref::<RefError>())
            .map(|_| {
                with_reason(
                    Code::FailedPrecondition,
                    "Referenced.",
                    "RESOURCE_REFERENCED",
                    "store",
                )
            })
    })
    .declare([Reason::new(
        "SHELF_EMPTY",
        Code::FailedPrecondition,
        "the shelf has no books",
    )])
}

#[test]
fn a_problem_renders_its_message_reason_and_field() {
    let tr = translator();
    let err = Wrapped(
        "wrapped".into(),
        Box::new(
            Problem::new(INVALID_ARGUMENT)
                .reason("ORGANIZATION_REQUIRED")
                .field("organizational_unit.organization")
                .what("an organizational unit belongs to an organization")
                .fix("Create one first"),
        ),
    );
    let st = tr.to_status(&err);
    assert_eq!(st.code(), Code::InvalidArgument);
    assert_eq!(
        st.message(),
        "An organizational unit belongs to an organization. Create one first."
    );
    let info = st.get_details_error_info().unwrap();
    assert_eq!(info.reason, "ORGANIZATION_REQUIRED");
    assert_eq!(info.domain, "shop.example.com");
    assert_eq!(
        st.get_details_bad_request().unwrap().field_violations[0].field,
        "organizational_unit.organization"
    );
    assert_eq!(reason_of(&st).as_deref(), Some("ORGANIZATION_REQUIRED"));
}

#[test]
fn a_problem_attaches_the_detail_its_kind_calls_for() {
    let tr = translator();
    // A blocked call names its resource in a PreconditionFailure.
    let st = tr.to_status(
        &Problem::new(INVALID_STATE)
            .reason("HOLD_NOT_ACTIVE")
            .resource("", "holds/h1")
            .what("hold holds/h1 is expired"),
    );
    assert_eq!(st.code(), Code::FailedPrecondition);
    assert_eq!(reason_of(&st).as_deref(), Some("HOLD_NOT_ACTIVE"));
    assert_eq!(
        st.get_details_precondition_failure().unwrap().violations[0].subject,
        "holds/h1"
    );

    // A missing one in a ResourceInfo; with no reason, the kind's default.
    let st = tr.to_status(
        &Problem::new(NOT_FOUND)
            .resource("shop/Shelf", "shelves/s")
            .what("shelf shelves/s does not exist"),
    );
    assert_eq!(st.code(), Code::NotFound);
    assert_eq!(reason_of(&st).as_deref(), Some("NOT_FOUND"));
    assert_eq!(
        st.get_details_resource_info().unwrap().resource_name,
        "shelves/s"
    );

    // A declared reason with no kind takes the declared code.
    let st = tr.to_status(&Problem {
        reason: "SHELF_EMPTY".into(),
        what: "the shelf is empty".into(),
        ..Problem::default()
    });
    assert_eq!(st.code(), Code::FailedPrecondition);
    let reasons = tr.reasons();
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0].name, "SHELF_EMPTY");
}

#[test]
fn a_wrapped_sentinel_keeps_its_message_less_the_sentinel() {
    let tr = translator();
    let st = tr.to_status(&Plain(format!(
        "{INVALID_ARGUMENT}: window needs both ends"
    )));
    assert_eq!(st.code(), Code::InvalidArgument);
    assert_eq!(st.message(), "Window needs both ends.");
    assert_eq!(reason_of(&st).as_deref(), Some("INVALID_ARGUMENT"));
    assert_eq!(
        tr.plain(&Plain(format!("{FOREIGN_KEY}: {NOT_FOUND}: x"))),
        "x"
    );
}

#[test]
fn statuses_pass_through_and_anything_else_is_internal() {
    let tr = translator();
    let st = tr.to_status(&Status::invalid_argument("bad request"));
    assert_eq!(st.code(), Code::InvalidArgument);
    assert_eq!(st.message(), "bad request");
    assert_eq!(tr.to_status(&Plain("boom".into())).code(), Code::Internal);

    let st = tr.to_status(&Wrapped(
        "create".into(),
        Box::new(RefError("shelves/s".into())),
    ));
    assert_eq!(st.code(), Code::FailedPrecondition);
    assert_eq!(reason_of(&st).as_deref(), Some("RESOURCE_REFERENCED"));
}

#[test]
fn translate_handles_what_crosses_tonic() {
    let tr = translator();
    // A problem returned with `into()`.
    let carried: Status = Problem::new(NOT_FOUND)
        .resource("shop/Shelf", "shelves/s")
        .what("shelf shelves/s does not exist")
        .metadata("shelf", "s")
        .into();
    assert_eq!(carried.code(), Code::Unknown);
    let st = tr.translate(carried);
    assert_eq!(st.code(), Code::NotFound);
    assert_eq!(st.message(), "Shelf shelves/s does not exist.");
    let info = st.get_details_error_info().unwrap();
    assert_eq!(info.domain, "shop.example.com");
    assert_eq!(info.metadata.get("shelf").map(String::as_str), Some("s"));

    // A plain error converted with `Status::from_error`.
    let st = tr.translate(Status::from_error(Box::new(Plain(format!(
        "{NOT_FOUND}: gone"
    )))));
    assert_eq!(st.code(), Code::NotFound);
    assert_eq!(st.message(), "Gone.");

    // An unregistered one, and a status the handler meant.
    assert_eq!(
        tr.translate(Status::unknown("ERROR: relation does not exist"))
            .code(),
        Code::Internal
    );
    let meant = tr.translate(Status::already_exists("taken"));
    assert_eq!(
        (meant.code(), meant.message()),
        (Code::AlreadyExists, "taken")
    );
}

#[test]
fn messages_read_as_sentences() {
    for (input, want) in [
        (
            "organization does not exist",
            "Organization does not exist.",
        ),
        ("update_mask names nothing", "update_mask names nothing."),
        (
            "organization.display_names is required",
            "organization.display_names is required.",
        ),
        ("Already a sentence.", "Already a sentence."),
        ("  ", ""),
    ] {
        assert_eq!(sentence(input), want, "{input:?}");
    }
}
