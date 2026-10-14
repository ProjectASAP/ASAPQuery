//! `--planner saved` on a real sketch-bench eval table: the `all` workload of
//! sketch-bench's `rqe-optimizer/results/e2e-rate10` (sketch-bench c1fd812),
//! whose e2e queries carry RQE trees, and its saved plans.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use asap_planner::optimizer::{saved_plan_output, PlanSelector};
use asap_planner::PlannerOutput;
use asap_types::enums::QueryLanguage;
use serde_json::Value;

/// The table's scrape interval: instant RQEs read one 100 ms scrape.
const SCRAPE_INTERVAL_MS: u64 = 100;

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test_data/saved_plan")
}

fn table_path() -> PathBuf {
    data_dir().join("synthetic-templatesall-shared1.json")
}

fn convert(table: &Path, language: QueryLanguage) -> PlannerOutput {
    let selector = PlanSelector {
        method: "asap",
        weights: "cpu",
        sla_ms: None,
    };
    saved_plan_output(
        table,
        &data_dir().join("synthetic-templatesall-shared1.plans.json.gz"),
        &selector,
        language,
        SCRAPE_INTERVAL_MS,
    )
    .unwrap()
}

fn table_queries() -> Vec<Value> {
    let table: Value =
        serde_json::from_str(&std::fs::read_to_string(table_path()).unwrap()).unwrap();
    table["workloads"][0]["e2e"]["queries"]
        .as_array()
        .unwrap()
        .clone()
}

fn language_key(language: QueryLanguage) -> &'static str {
    match language {
        QueryLanguage::promql => "promql",
        QueryLanguage::sql => "sql",
        other => panic!("{other:?}"),
    }
}

fn leaf_ids(expr: &Value, out: &mut BTreeSet<String>) {
    if let Some(rqe) = expr.get("rqe") {
        out.insert(rqe["id"].as_str().unwrap().to_string());
    }
    for child in ["of", "lhs", "rhs"] {
        if let Some(child) = expr.get(child) {
            leaf_ids(child, out);
        }
    }
}

/// Every tree leaf of the language's queries gets an `rqe_configs` entry
/// reading what its single-RQE query reads.
#[test]
fn writes_rqe_configs_for_every_tree_leaf() {
    for language in [QueryLanguage::promql, QueryLanguage::sql] {
        let output = convert(&table_path(), language);
        let inference = output.to_inference_config(language).unwrap();
        let queries: Vec<Value> = table_queries()
            .into_iter()
            .filter(|q| q.get(language_key(language)).is_some())
            .collect();

        let mut expected = BTreeSet::new();
        for q in &queries {
            if let Some(expr) = q.get("expr") {
                leaf_ids(expr, &mut expected);
            }
        }
        assert!(!expected.is_empty(), "{language:?}");
        let written: BTreeSet<String> = inference.rqe_configs.keys().cloned().collect();
        assert_eq!(written, expected, "{language:?}");

        let query_configs: HashMap<&str, _> = inference
            .query_configs
            .iter()
            .map(|c| (c.query.as_str(), c))
            .collect();
        for q in &queries {
            let [rqe] = q["rqes"].as_array().unwrap().as_slice() else {
                continue;
            };
            let Some(rqe) = inference.rqe_configs.get(rqe.as_str().unwrap()) else {
                continue;
            };
            let query = query_configs[q[language_key(language)].as_str().unwrap()];
            let ids = |refs: &[asap_types::AggregationReference]| {
                refs.iter()
                    .map(|a| (a.aggregation_id, a.num_aggregates_to_retain))
                    .collect::<Vec<_>>()
            };
            assert_eq!(ids(&rqe.aggregations), ids(&query.aggregations), "{q}");
            assert_eq!(
                rqe.query_time_aggregations.len(),
                query.query_time_aggregations.len(),
                "{q}"
            );
        }
    }
}

/// The trees add only `rqe_configs`: the table without them converts to the
/// same deployments and `query_configs`.
#[test]
fn trees_add_only_rqe_configs() {
    let mut table: Value =
        serde_json::from_str(&std::fs::read_to_string(table_path()).unwrap()).unwrap();
    for q in table["workloads"][0]["e2e"]["queries"]
        .as_array_mut()
        .unwrap()
    {
        q.as_object_mut().unwrap().remove("expr");
    }
    let dir = tempfile::tempdir().unwrap();
    let stripped = dir.path().join("table.json");
    std::fs::write(&stripped, table.to_string()).unwrap();

    for language in [QueryLanguage::promql, QueryLanguage::sql] {
        let with = convert(&table_path(), language);
        let without = convert(&stripped, language);
        let yaml = |s: String| serde_yaml::from_str::<serde_yaml::Value>(&s).unwrap();
        assert_eq!(
            yaml(with.to_streaming_yaml_string().unwrap()),
            yaml(without.to_streaming_yaml_string().unwrap()),
            "{language:?}"
        );
        let mut with_inference = yaml(with.to_inference_yaml_string().unwrap());
        let without_inference = yaml(without.to_inference_yaml_string().unwrap());
        assert!(without_inference.get("rqe_configs").is_none());
        assert!(with_inference
            .as_mapping_mut()
            .unwrap()
            .remove("rqe_configs")
            .is_some());
        assert_eq!(with_inference, without_inference, "{language:?}");
    }
}
