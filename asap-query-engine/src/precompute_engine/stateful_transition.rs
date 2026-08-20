use std::collections::HashMap;

// StatefulTransitionConfig now lives in asap_types so asap-planner-rs can
// construct it (from detecting the SQL pattern) and the engine can consume
// it (to drive ingest-time state tracking) without either crate depending
// on the other - see asap_types::stateful_transition for the shared
// definition and design note.
pub use asap_types::stateful_transition::StatefulTransitionConfig;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PartitionKey(Vec<String>);

#[derive(Debug, Default)]
pub struct StatefulTransitionOperator {
    cfg: StatefulTransitionConfig,
    last_value: HashMap<PartitionKey, String>,
}

impl StatefulTransitionOperator {
    pub fn new(cfg: StatefulTransitionConfig) -> Self {
        Self {
            cfg,
            last_value: HashMap::new(),
        }
    }

    pub fn process_row(&mut self, row: &HashMap<String, String>) -> Option<String> {
        let key = PartitionKey(
            self.cfg
                .partition_by
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or_default())
                .collect(),
        );

        let curr = row.get(&self.cfg.state_column).cloned().unwrap_or_default();

        let prev = self.last_value.get(&key).cloned();
        self.last_value.insert(key, curr.clone());

        let prev = prev?;

        if !eval_transition_predicate(
            &self.cfg.predicate,
            &self.cfg.previous_alias,
            &self.cfg.state_column,
            &prev,
            &curr,
            row,
        ) {
            return None;
        }

        Some(build_label_string(
            &self.cfg.metric_name,
            &self.cfg.emit_labels,
            row,
        ))
    }
}

fn build_label_string(metric: &str, labels: &[String], row: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return metric.to_string();
    }

    let mut out = String::with_capacity(64);
    out.push_str(metric);
    out.push('{');

    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let value = row.get(label).map(String::as_str).unwrap_or("");
        out.push_str(label);
        out.push_str("=\"");
        out.push_str(value);
        out.push('"');
    }

    out.push('}');
    out
}

fn eval_transition_predicate(
    raw: &str,
    previous_alias: &str,
    state_column: &str,
    prev: &str,
    curr: &str,
    row: &HashMap<String, String>,
) -> bool {
    // V0: support conjunctions of simple equality/inequality expressions.
    // This is generic: no BGP-specific column names or metric names.
    raw.split(" AND ")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .all(|clause| eval_simple_clause(clause, previous_alias, state_column, prev, curr, row))
}

fn eval_simple_clause(
    clause: &str,
    previous_alias: &str,
    state_column: &str,
    prev: &str,
    curr: &str,
    row: &HashMap<String, String>,
) -> bool {
    if let Some((lhs, rhs)) = clause.split_once("!=") {
        return resolve_value(lhs.trim(), previous_alias, state_column, prev, curr, row)
            != resolve_value(rhs.trim(), previous_alias, state_column, prev, curr, row);
    }

    if let Some((lhs, rhs)) = clause.split_once("=") {
        return resolve_value(lhs.trim(), previous_alias, state_column, prev, curr, row)
            == resolve_value(rhs.trim(), previous_alias, state_column, prev, curr, row);
    }

    false
}

fn resolve_value(
    expr: &str,
    previous_alias: &str,
    state_column: &str,
    prev: &str,
    curr: &str,
    row: &HashMap<String, String>,
) -> String {
    let expr = expr.trim();

    if expr == previous_alias {
        return prev.to_string();
    }

    if expr == state_column {
        return curr.to_string();
    }

    if expr.len() >= 2 && expr.starts_with('\'') && expr.ends_with('\'') {
        return expr[1..expr.len() - 1].to_string();
    }

    row.get(expr).cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_generic_transition() {
        let cfg = StatefulTransitionConfig {
            metric_name: "__derived_q11_path_changes".to_string(),
            partition_by: vec![
                "prefix".to_string(),
                "collector".to_string(),
                "peer_ip".to_string(),
            ],
            state_column: "as_path".to_string(),
            previous_alias: "previous_path".to_string(),
            predicate: "previous_path != '' AND previous_path != as_path".to_string(),
            emit_labels: vec!["prefix".to_string()],
        };

        let mut op = StatefulTransitionOperator::new(cfg);

        let mut row1 = HashMap::new();
        row1.insert("prefix".to_string(), "1.2.3.0/24".to_string());
        row1.insert("collector".to_string(), "rrc00".to_string());
        row1.insert("peer_ip".to_string(), "peer1".to_string());
        row1.insert("as_path".to_string(), "1 2 3".to_string());

        let mut row2 = row1.clone();
        row2.insert("as_path".to_string(), "1 2 4".to_string());

        assert!(op.process_row(&row1).is_none());
        assert_eq!(
            op.process_row(&row2),
            Some("__derived_q11_path_changes{prefix=\"1.2.3.0/24\"}".to_string())
        );
    }
}
