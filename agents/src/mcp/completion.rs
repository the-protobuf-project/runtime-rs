//! Autocomplete for prompt arguments whose proto declared a closed set of values.

use std::collections::HashMap;

use rmcp::model::{CompleteResult, CompletionInfo};

/// Serves autocomplete values for prompt arguments, keyed by `"promptName:argName"`.
///
/// Values are filtered by what the user has typed so far, case-insensitively — a client sends
/// the partial value and expects the list to narrow.
#[derive(Debug, Clone, Default)]
pub struct EnumCompletions(HashMap<String, Vec<String>>);

impl EnumCompletions {
    /// Builds a completion source from the map generated code carries.
    pub fn new(values: HashMap<String, Vec<String>>) -> Self {
        Self(values)
    }

    /// Whether anything was declared.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The completions for one prompt argument, narrowed by `typed`.
    ///
    /// An argument with nothing declared completes to an empty list rather than an error: the
    /// client asked a reasonable question about a field that simply has no suggestions.
    pub fn complete(&self, prompt: &str, argument: &str, typed: &str) -> CompleteResult {
        let key = format!("{prompt}:{argument}");
        let prefix = typed.to_lowercase();
        let values: Vec<String> = self
            .0
            .get(&key)
            .map(|values| {
                values
                    .iter()
                    .filter(|value| value.to_lowercase().starts_with(&prefix))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        // CompletionInfo::new caps the list at the spec's maximum; a longer one is
        // truncated rather than dropped, since some suggestions beat none.
        let info = CompletionInfo::new(values.clone()).unwrap_or_else(|_| {
            CompletionInfo::new(values[..CompletionInfo::MAX_VALUES].to_vec())
                .expect("truncated to the maximum")
        });
        CompleteResult::new(info)
    }
}
