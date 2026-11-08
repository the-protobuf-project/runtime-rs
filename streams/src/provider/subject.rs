//! Subject declaration and NATS-style wildcard matching.

use crate::StreamError;

/// Whether `subject` matches `pattern`, in NATS wildcard syntax.
///
/// Tokens are separated by `.`; `*` matches exactly one token and `>` matches
/// one or more trailing tokens. A `>` anywhere but last matches nothing.
pub fn matches(pattern: &str, subject: &str) -> bool {
    if pattern == subject {
        return true;
    }
    let pattern: Vec<&str> = pattern.split('.').collect();
    let subject: Vec<&str> = subject.split('.').collect();
    for (i, token) in pattern.iter().enumerate() {
        match *token {
            ">" => return i == pattern.len() - 1 && subject.len() > i,
            "*" if i >= subject.len() => return false,
            "*" => {}
            literal if subject.get(i) != Some(&literal) => return false,
            _ => {}
        }
    }
    pattern.len() == subject.len()
}

/// Whether `declared` lists `subject` literally.
///
/// For backends without wildcard routing a declared `orders.*` is a name like
/// any other, and treating it as a pattern would accept publishes nobody can
/// subscribe to.
pub fn declares(declared: &[String], subject: &str) -> bool {
    declared.iter().any(|d| d == subject)
}

/// Whether any declared subject, read as a pattern, matches `subject`.
pub fn declares_pattern(declared: &[String], subject: &str) -> bool {
    declared.iter().any(|d| matches(d, subject))
}

/// The error for a subject `stream` does not declare.
pub fn unknown_subject(stream: &str, subject: &str, declared: &[String]) -> StreamError {
    StreamError::UnknownSubject {
        stream: stream.to_owned(),
        subject: subject.to_owned(),
        declared: declared.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcards() {
        let cases = [
            ("orders.placed", "orders.placed", true),
            ("orders.placed", "orders.shipped", false),
            ("orders.*", "orders.placed", true),
            ("orders.*", "orders.us.placed", false),
            ("orders.*.placed", "orders.us.placed", true),
            ("orders.*.placed", "orders.us.eu.placed", false),
            ("orders.>", "orders.us.placed", true),
            ("orders.>", "orders.placed", true),
            ("orders.>", "orders", false),
            (">", "orders.us.placed", true),
            (">", "orders", true),
            ("*", "orders", true),
            ("*", "orders.placed", false),
            ("a.>.b", "a.x.b", false),
            ("orders", "orders.placed", false),
            ("orders.placed", "orders", false),
        ];
        for (pattern, subject, want) in cases {
            assert_eq!(matches(pattern, subject), want, "{pattern} vs {subject}");
        }
    }

    #[test]
    fn declares_is_literal() {
        let declared = vec!["orders.*".to_owned(), "orders.placed".to_owned()];
        assert!(declares(&declared, "orders.*"));
        assert!(!declares(&declared, "orders.us"));
    }

    #[test]
    fn declares_pattern_reads_patterns() {
        let declared = vec!["orders.*".to_owned(), "events.>".to_owned()];
        assert!(declares_pattern(&declared, "orders.placed"));
        assert!(declares_pattern(&declared, "events.us.signup"));
        assert!(!declares_pattern(&declared, "orders.us.placed"));
    }

    #[test]
    fn unknown_subject_names_everything() {
        let error = unknown_subject("s-1", "orders.shipped", &["orders.placed".into()]);
        let text = error.to_string();
        for want in ["orders.shipped", "orders.placed", "s-1"] {
            assert!(text.contains(want), "{text}");
        }
    }
}
