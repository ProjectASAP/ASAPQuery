//! SQL compliance runner: plans each suite query in isolation and builds the
//! engine's execution context for it, without ingesting data or executing.
//!
//! A query is `supported` when both the planner accepts it and the engine
//! resolves an execution context against that plan. Each suite case declares
//! the expected outcome, so a query that starts or stops being supported
//! fails the run until its suite entry is updated.

use asap_planner::{SQLController, SQLRuntimeOptions, StreamingEngine};
use asap_types::enums::{CleanupPolicy, QueryLanguage};
use query_engine_rust::{SimpleEngine, SimpleMapStore};
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SUITES_DIR: &str = "tests/sql_compliance";
const SUITE_EXTENSION: &str = "yaml";
/// Fixed evaluation time (seconds) so NOW()-relative queries plan and match
/// deterministically.
const QUERY_EVALUATION_TIME_SECS: f64 = 1_000_000.0;
const ACCURACY_SLA: f64 = 0.95;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Suite {
    /// Planner table definitions, passed through verbatim.
    tables: serde_json::Value,
    data_ingestion_interval_ms: u64,
    repetition_delay_ms: u64,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    query: String,
    expected: Outcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Supported,
    Unsupported,
}

/// Where a query stopped, with the reason. `None` means it is supported.
type Failure = Option<String>;

fn run_case(suite: &Suite, query: &str) -> Failure {
    let planner_config = json!({
        "tables": suite.tables,
        "query_groups": [{
            "queries": [query],
            "repetition_delay_ms": suite.repetition_delay_ms,
            "controller_options": { "accuracy_sla": ACCURACY_SLA },
        }],
    });
    let planner_yaml = serde_yaml::to_string(&planner_config).expect("planner config serializes");
    let opts = SQLRuntimeOptions {
        streaming_engine: StreamingEngine::Precompute,
        query_evaluation_time: Some(QUERY_EVALUATION_TIME_SECS),
        data_ingestion_interval_ms: suite.data_ingestion_interval_ms,
    };
    let output = match SQLController::from_yaml(&planner_yaml, opts).and_then(|c| c.generate()) {
        Ok(output) => output,
        Err(error) => return Some(format!("plan: {error}")),
    };

    let inference_config = output
        .to_inference_config(QueryLanguage::sql)
        .expect("planner output converts to inference config");
    let streaming_config = Arc::new(
        output
            .to_streaming_config(QueryLanguage::sql)
            .expect("planner output converts to streaming config"),
    );
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    let engine = SimpleEngine::new(
        store,
        inference_config,
        streaming_config,
        suite.data_ingestion_interval_ms,
        QueryLanguage::sql,
    );
    match engine.build_query_execution_context_sql(query.to_string(), QUERY_EVALUATION_TIME_SECS) {
        Some(_) => None,
        None => Some("engine: no execution context for planned query".to_string()),
    }
}

fn suite_paths() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(SUITES_DIR);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("suite dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == SUITE_EXTENSION))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no suites in {}", dir.display());
    paths
}

#[test]
fn sql_queries_match_expected_support() {
    let mut mismatches = Vec::new();
    for path in suite_paths() {
        let suite_name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let contents = std::fs::read_to_string(&path).expect("read suite");
        let suite: Suite = serde_yaml::from_str(&contents)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));

        for case in &suite.cases {
            let failure = run_case(&suite, &case.query);
            let actual = match failure {
                None => Outcome::Supported,
                Some(_) => Outcome::Unsupported,
            };
            let status = if actual == case.expected {
                "ok"
            } else {
                "MISMATCH"
            };
            let reason = failure.as_deref().unwrap_or("");
            println!("[{status}] {suite_name}/{}: {actual:?} {reason}", case.name);
            if actual != case.expected {
                mismatches.push(format!(
                    "{suite_name}/{}: expected {:?}, got {actual:?} {reason}",
                    case.name, case.expected
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "SQL compliance mismatches:\n{}",
        mismatches.join("\n")
    );
}
