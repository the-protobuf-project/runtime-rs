//! AMQP topic-exchange routing rules: binding keys, routing keys, and names.
//!
//! These are pure functions, kept apart from the broker code so the rules that
//! decide what a stream accepts can be tested without a broker.

/// Whether a routing key satisfies an AMQP binding key.
///
/// Words are separated by `.`. Within a binding key, `*` stands for exactly one
/// word and `#` for zero or more, so `user.#` matches `user`, `user.created`,
/// and `user.eu.created` alike.
///
/// That `#` matches zero words is why this is not
/// [`provider::matches`](crate::provider::matches): NATS's `>` requires at least
/// one token, and reading one broker's wildcards with the other's rules is how a
/// publish succeeds against a subject nothing is bound to. Unlike MQTT's `#`,
/// AMQP's may appear anywhere in the key.
pub(crate) fn matches(binding: &str, key: &str) -> bool {
    if binding == key {
        return true;
    }
    let binding: Vec<&str> = binding.split('.').collect();
    let key: Vec<&str> = key.split('.').collect();
    walk(&binding, &key)
}

/// Matches what is left of a binding against what is left of a key.
///
/// `#` can absorb any number of words, so the tail after it has to be tried at
/// every remaining position, which is what makes this a search rather than a
/// walk. Binding keys are short, so the recursion stays shallow.
fn walk(binding: &[&str], key: &[&str]) -> bool {
    let Some((&word, rest)) = binding.split_first() else {
        return key.is_empty();
    };
    match word {
        "#" => (0..=key.len()).any(|skip| walk(rest, &key[skip..])),
        "*" => !key.is_empty() && walk(rest, &key[1..]),
        literal => key.first() == Some(&literal) && walk(rest, &key[1..]),
    }
}

/// Whether any declared subject, read as an AMQP binding key, matches `subject`.
///
/// A stream declaring `user.*` accepts `user.created`, because that is what the
/// exchange would route.
pub(crate) fn declares(declared: &[String], subject: &str) -> bool {
    declared.iter().any(|d| matches(d, subject))
}

/// Whether a subject carries a binding character, which makes it something to
/// bind to rather than publish on.
pub(crate) fn has_wildcard(subject: &str) -> bool {
    subject.split('.').any(|word| word == "*" || word == "#")
}

/// Replaces the characters AMQP reads as routing structure, so a generated id
/// carrying the caller's stream name, or a consumer name, cannot change the
/// topology. Byte-for-byte runtime-go's `safeName`, since queue and exchange
/// names built from it must agree across the two runtimes.
pub(crate) fn safe_name(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '.' | '*' | '#' | ' ' => '-',
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // These need no broker: the binding-key rules are pure.
    #[test]
    fn binding_keys_follow_amqp() {
        let cases = [
            ("identical", "user.created", "user.created", true),
            ("literal mismatch", "user.created", "user.deleted", false),
            ("star takes one word", "user.*", "user.created", true),
            (
                "star does not span words",
                "user.*",
                "user.eu.created",
                false,
            ),
            (
                "star in the middle",
                "user.*.created",
                "user.eu.created",
                true,
            ),
            ("hash takes the tail", "user.#", "user.eu.created", true),
            ("hash takes one word", "user.#", "user.created", true),
            // AMQP's '#' matches zero words as well, unlike NATS's '>'.
            ("hash takes no words", "user.#", "user", true),
            ("bare hash takes everything", "#", "user.created", true),
            ("bare hash takes nothing", "#", "", true),
            // And unlike MQTT's, AMQP's '#' may appear anywhere.
            (
                "hash in the middle",
                "user.#.created",
                "user.eu.west.created",
                true,
            ),
            (
                "hash in the middle spanning nothing",
                "user.#.created",
                "user.created",
                true,
            ),
            (
                "hash in the middle, no match",
                "user.#.created",
                "user.eu.deleted",
                false,
            ),
            ("key longer than binding", "user", "user.created", false),
            ("binding longer than key", "user.created", "user", false),
        ];
        for (name, binding, key, want) in cases {
            assert_eq!(matches(binding, key), want, "{name}: {binding} vs {key}");
        }
    }

    #[test]
    fn has_wildcard_finds_binding_words() {
        for s in ["user.*", "user.#", "#", "*", "a.#.b"] {
            assert!(has_wildcard(s), "{s}");
        }
        for s in ["user.created", "user", "a.b.c", "user*.x"] {
            assert!(!has_wildcard(s), "{s}");
        }
    }

    #[test]
    fn safe_name_strips_routing_structure() {
        for s in ["a.b", "a*b", "a#b", "a b"] {
            assert_eq!(safe_name(s), "a-b", "{s}");
        }
        assert_eq!(safe_name("01J:orders"), "01J:orders");
    }

    #[test]
    fn declares_reads_binding_keys() {
        let declared = vec!["user.*".to_owned(), "audit.#".to_owned()];
        assert!(declares(&declared, "user.created"));
        assert!(declares(&declared, "user.*"));
        assert!(declares(&declared, "audit"));
        assert!(declares(&declared, "audit.eu.login"));
        assert!(!declares(&declared, "user.eu.created"));
        assert!(!declares(&declared, "typo"));
    }
}
