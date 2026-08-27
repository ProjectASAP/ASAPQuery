use serde::{Deserialize, Serialize};

/// Config for a computed label: a metadata column derived from another raw
/// column at ingest time rather than read directly off the row, e.g.
/// extracting the origin ASN (`select: last`) or every ASN (`select: all`,
/// via `type: token_explode`) out of a space-separated `as_path` string.
///
/// Shared between asap-planner-rs (which detects a nested-subquery SQL shape
/// like `arrayFilter(...) AS as_path_array ... as_path_array[-1]` and emits
/// this config automatically) and asap-query-engine (which reads it back to
/// actually compute the label at ingest time in
/// precompute_engine::computed_labels::compute_label_values). Previously the
/// engine owned this type privately and nothing on the planner side could
/// construct it - a human had to notice the pattern and hand-write it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ComputedLabelConfig {
    pub r#type: String,
    pub source_col: String,
    pub tokenizer: Option<String>,
    pub filter_regex: Option<String>,
    pub select: Option<String>,
    pub on_missing: Option<String>,
}

impl Default for ComputedLabelConfig {
    fn default() -> Self {
        Self {
            r#type: "field_alias".to_string(),
            source_col: String::new(),
            tokenizer: None,
            filter_regex: None,
            select: None,
            on_missing: None,
        }
    }
}
