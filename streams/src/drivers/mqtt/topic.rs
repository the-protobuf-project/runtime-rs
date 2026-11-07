//! MQTT topic filters, matched the way the broker matches them.
//!
//! Subjects on this provider *are* MQTT topic levels: `user/created`, with `+`
//! and `#` as the wildcards. They are not translated from NATS-style dotted
//! subjects, because runtime-go does not translate them either, and a Go and a
//! Rust service sharing a broker have to land on the same topic.

/// Whether `topic` satisfies the MQTT topic filter `filter`.
///
/// Levels are separated by `/`. Within a filter, `+` stands for exactly one
/// level and `#` for the remaining levels, including none, so `a/#` matches `a`
/// as well as `a/b/c`. That last part is where MQTT differs from NATS, whose `>`
/// needs at least one token, and why this is not
/// [`provider::matches`](crate::provider::matches).
///
/// `#` is only a wildcard as the final level; anywhere else it is a literal no
/// publishable topic can contain, so the filter simply matches nothing.
pub(crate) fn matches(filter: &str, topic: &str) -> bool {
    if filter == topic {
        return true;
    }
    let filter: Vec<&str> = filter.split('/').collect();
    let topic: Vec<&str> = topic.split('/').collect();
    for (i, level) in filter.iter().enumerate() {
        if *level == "#" {
            return i == filter.len() - 1;
        }
        match topic.get(i) {
            None => return false,
            Some(t) if *level != "+" && level != t => return false,
            Some(_) => {}
        }
    }
    filter.len() == topic.len()
}

/// Whether a subject carries a filter character, which makes it something to
/// subscribe to rather than publish on.
pub(crate) fn has_wildcard(subject: &str) -> bool {
    subject.contains(['+', '#'])
}

/// Whether any declared subject, read as an MQTT filter, matches `subject`.
///
/// A stream declaring `user/+` accepts `user/created`. The shared
/// [`provider::declares_pattern`](crate::provider::declares_pattern) reads NATS
/// wildcards and would not, so this provider keeps its own.
pub(crate) fn declares(declared: &[String], subject: &str) -> bool {
    declared.iter().any(|d| matches(d, subject))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_like_a_broker() {
        let cases = [
            ("identical", "user/created", "user/created", true),
            ("literal mismatch", "user/created", "user/deleted", false),
            ("plus takes one level", "user/+", "user/created", true),
            (
                "plus does not span levels",
                "user/+",
                "user/eu/created",
                false,
            ),
            (
                "plus in the middle",
                "user/+/created",
                "user/eu/created",
                true,
            ),
            ("hash takes the tail", "user/#", "user/eu/created", true),
            ("hash takes one level", "user/#", "user/created", true),
            // Where MQTT differs from NATS: '#' matches the parent level too,
            // so this is true where the equivalent NATS '>' would be false.
            ("hash matches the parent level", "user/#", "user", true),
            ("bare hash takes everything", "#", "user/created", true),
            ("hash is only trailing", "a/#/b", "a/x/b", false),
            ("topic longer than filter", "user", "user/created", false),
            ("filter longer than topic", "user/created", "user", false),
        ];
        for (name, filter, topic, want) in cases {
            assert_eq!(matches(filter, topic), want, "{name}: {filter} vs {topic}");
        }
    }

    #[test]
    fn has_wildcard_flags_filters_only() {
        for s in ["user/+", "user/#", "#", "+"] {
            assert!(has_wildcard(s), "{s}");
        }
        assert!(!has_wildcard("user/created"));
    }

    #[test]
    fn declares_reads_mqtt_filters() {
        let declared = vec!["user/+".to_owned(), "audit/#".to_owned()];
        assert!(declares(&declared, "user/created"));
        assert!(declares(&declared, "user/+"));
        assert!(declares(&declared, "audit"));
        assert!(!declares(&declared, "user/eu/created"));
        assert!(!declares(&declared, "typo"));
    }
}
