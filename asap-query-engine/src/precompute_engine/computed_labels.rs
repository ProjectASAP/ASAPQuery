use regex::Regex;

// ComputedLabelConfig now lives in asap_types so asap-planner-rs can
// construct it (from detecting a nested-subquery SQL shape) and the engine
// can consume it (to actually compute the label at ingest time) without
// either crate depending on the other - see asap_types::computed_label for
// the shared definition and design note.
pub use asap_types::computed_label::ComputedLabelConfig;

pub fn should_skip_on_missing(rule: &ComputedLabelConfig) -> bool {
    rule.on_missing.as_deref() == Some("skip_sample")
}

fn tokenize(rule: &ComputedLabelConfig, raw_value: &str) -> Result<Vec<String>, String> {
    let tokenizer = rule.tokenizer.as_deref().unwrap_or("whitespace");

    let mut tokens: Vec<String> = match tokenizer {
        "whitespace" => raw_value
            .split_whitespace()
            .filter(|x| !x.is_empty())
            .map(|x| x.to_string())
            .collect(),
        other => {
            return Err(format!(
                "unsupported computed-label tokenizer {:?}; only whitespace is supported",
                other
            ));
        }
    };

    if let Some(pat) = rule.filter_regex.as_deref() {
        let re = Regex::new(pat)
            .map_err(|e| format!("invalid computed-label filter_regex {:?}: {}", pat, e))?;
        tokens.retain(|x| re.is_match(x));
    }

    Ok(tokens)
}

pub fn compute_label_values(
    rule: &ComputedLabelConfig,
    raw_value: &str,
) -> Result<Vec<String>, String> {
    match rule.r#type.as_str() {
        "field_alias" => {
            if raw_value.is_empty() && should_skip_on_missing(rule) {
                Ok(vec![])
            } else {
                Ok(vec![raw_value.to_string()])
            }
        }

        "token_select" => {
            let tokens = tokenize(rule, raw_value)?;
            if tokens.is_empty() {
                return Ok(vec![]);
            }

            let selected = match rule.select.as_deref().unwrap_or("last") {
                "first" => tokens.first().cloned(),
                "last" => tokens.last().cloned(),
                sel if sel.starts_with("nth:") => {
                    let idx: usize = sel["nth:".len()..].parse().map_err(|e| {
                        format!("invalid token_select index in select={:?}: {}", sel, e)
                    })?;
                    tokens.get(idx).cloned()
                }
                other => {
                    return Err(format!(
                        "unsupported token_select selector {:?}; use first, last, or nth:N",
                        other
                    ));
                }
            };

            Ok(selected.into_iter().collect())
        }

        "token_explode" => tokenize(rule, raw_value),

        other => Err(format!("unsupported computed-label type {:?}", other)),
    }
}

