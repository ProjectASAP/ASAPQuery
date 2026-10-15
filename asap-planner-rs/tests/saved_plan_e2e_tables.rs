//! `--planner saved` on real sketch-bench eval tables and their saved plans:
//! the `all` workload of sketch-bench's `rqe-optimizer/results/e2e-rate10`
//! (sketch-bench c1fd812), whose e2e queries carry RQE trees, and the pilot
//! random workload `seed-862/workload-01-pilot` (sketch-bench PR #226), whose
//! top-k queries are `topk by (region, service)`.

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
    convert_with_plans(
        table,
        "synthetic-templatesall-shared1.plans.json.gz",
        language,
    )
    .unwrap()
}

fn convert_with_plans(
    table: &Path,
    plans: &str,
    language: QueryLanguage,
) -> anyhow::Result<PlannerOutput> {
    let selector = PlanSelector {
        method: "asap",
        weights: "cpu",
        sla_ms: None,
    };
    saved_plan_output(
        table,
        &data_dir().join(plans),
        &selector,
        language,
        SCRAPE_INTERVAL_MS,
    )
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

const PILOT_PLANS: &str = "random-seed862-workload01-pilot.plans.json.gz";

/// The pilot table without its `avg by (...) (quantile_over_time(...))`
/// queries, which `--planner saved` can't write yet.
fn pilot_table() -> Value {
    let path = data_dir().join("random-seed862-workload01-pilot.json");
    let mut table: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    table["workloads"][0]["e2e"]["queries"]
        .as_array_mut()
        .unwrap()
        .retain(|q| !q["promql"].as_str().unwrap().starts_with("avg by"));
    table
}

/// The pilot table with a tree on each `topk by (region, service) (4,
/// sum_over_time(data[w]))` query: a leaf over every series label, ranked
/// within `topk_by` buckets. Returns the table and the top-k RQE ids.
fn pilot_with_topk_trees(topk_by: Option<&[&str]>) -> (Value, Vec<String>) {
    let mut table = pilot_table();
    let mut ids = Vec::new();
    for q in table["workloads"][0]["e2e"]["queries"]
        .as_array_mut()
        .unwrap()
    {
        let promql = q["promql"].as_str().unwrap();
        if !promql.starts_with("topk by (region, service) (4, sum_over_time(data[") {
            continue;
        }
        let id = q["rqes"][0].as_str().unwrap().to_string();
        let lookback_ms = if promql.contains("[1m]") {
            60_000
        } else {
            assert!(promql.contains("[20m]"), "{promql}");
            1_200_000
        };
        let mut leaf = serde_json::json!({
            "id": id, "metric": "data", "capability": "topk_by_value", "k": 4,
            "lookback_ms": lookback_ms,
            "grouping": ["region", "service", "job", "label_0"],
        });
        if let Some(topk_by) = topk_by {
            leaf["topk_by"] = serde_json::json!(topk_by);
        }
        q["expr"] = serde_json::json!({"rqe": leaf});
        ids.push(id);
    }
    assert_eq!(ids.len(), 2, "the pilot has two top-k queries");
    (table, ids)
}

fn write_table(dir: &tempfile::TempDir, table: &Value) -> PathBuf {
    let path = dir.path().join("table.json");
    std::fs::write(&path, table.to_string()).unwrap();
    path
}

/// A `topk by (region, service)` leaf is the plan's top-k RQE, grouped by
/// its buckets: it reads the same per-bucket heap as its PromQL query and
/// adds only its `rqe_configs` entry.
#[test]
fn pilot_topk_by_leaf_reads_its_promql_querys_heap() {
    let dir = tempfile::tempdir().unwrap();
    let (with_trees, ids) = pilot_with_topk_trees(Some(&["region", "service"]));
    let with = convert_with_plans(
        &write_table(&dir, &with_trees),
        PILOT_PLANS,
        QueryLanguage::promql,
    )
    .unwrap();
    let without_dir = tempfile::tempdir().unwrap();
    let without = convert_with_plans(
        &write_table(&without_dir, &pilot_table()),
        PILOT_PLANS,
        QueryLanguage::promql,
    )
    .unwrap();

    let yaml = |s: String| serde_yaml::from_str::<serde_yaml::Value>(&s).unwrap();
    assert_eq!(
        yaml(with.to_streaming_yaml_string().unwrap()),
        yaml(without.to_streaming_yaml_string().unwrap())
    );
    let inference = with.to_inference_config(QueryLanguage::promql).unwrap();
    let written: BTreeSet<&String> = inference.rqe_configs.keys().collect();
    assert_eq!(written, ids.iter().collect());
    let streaming = with.to_streaming_config(QueryLanguage::promql).unwrap();
    for q in pilot_with_topk_trees(None).0["workloads"][0]["e2e"]["queries"]
        .as_array()
        .unwrap()
    {
        let Some(rqe) = inference.rqe_configs.get(q["rqes"][0].as_str().unwrap()) else {
            continue;
        };
        let query = inference
            .query_configs
            .iter()
            .find(|c| c.query == q["promql"].as_str().unwrap())
            .unwrap();
        let refs = |refs: &[asap_types::AggregationReference]| {
            refs.iter()
                .map(|a| (a.aggregation_id, a.num_aggregates_to_retain))
                .collect::<Vec<_>>()
        };
        assert_eq!(refs(&rqe.aggregations), refs(&query.aggregations), "{q}");
        assert!(rqe.query_time_aggregations.is_empty(), "{q}");
        // One heap per (region, service) bucket, keyed by the other labels.
        let heap = streaming
            .get_aggregation_config(rqe.aggregations[0].aggregation_id)
            .unwrap();
        assert_eq!(heap.grouping_labels.labels, ["region", "service"], "{q}");
        assert_eq!(heap.aggregated_labels.labels, ["job", "label_0"], "{q}");
    }
}

/// Without `topk_by` the leaf ranks every series as one bucket: not the
/// plan's per-(region, service) RQE, so the deployment its PromQL query
/// shares can't serve it.
#[test]
fn pilot_topk_leaf_without_topk_by_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (table, _) = pilot_with_topk_trees(None);
    let Err(error) = convert_with_plans(
        &write_table(&dir, &table),
        PILOT_PLANS,
        QueryLanguage::promql,
    ) else {
        panic!("a single-bucket leaf is not the plan's RQE");
    };
    assert!(
        error.to_string().contains("need different sketches"),
        "{error:#}"
    );
}
