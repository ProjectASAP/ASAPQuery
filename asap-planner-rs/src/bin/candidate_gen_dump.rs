//! Dump all candidate configs produced by enumerate_candidates for each AQE,
//! before greedy selection. Useful for auditing what the candidate space looks like.

use std::collections::HashMap;
use std::path::PathBuf;

use asap_planner::{
    optimizer::{
        enumerate_candidates, extract_aqes, load_atomic_cost_table, resolve_atomic_costs,
        AtomicCostTable, AtomicCosts, CandidateConfig, RQE,
    },
    ControllerConfig,
};
use asap_types::enums::WindowType;
use clap::Parser;
use promql_utilities::query_logics::enums::AggregationType;
use serde_json::Value;

#[derive(Parser)]
#[command(
    name = "candidate-gen-dump",
    about = "Print all candidates from enumerate_candidates (pre-selection) for a workload config"
)]
struct Args {
    #[arg(long = "input_config")]
    input_config: PathBuf,

    #[arg(long = "data-ingestion-interval-ms")]
    scrape_interval_ms: u64,

    /// Path to sketch-bench's exported atomic-cost table (see ASAPQuery#524).
    /// When given, each params row also prints its resolved AtomicCosts --
    /// real (from the table) or the flat stub (unbenchmarked family, or this
    /// exact param point missing from the table) -- labeled which.
    #[arg(long = "atomic-costs")]
    atomic_costs: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let atomic_cost_table = match &args.atomic_costs {
        Some(path) => load_atomic_cost_table(path)?,
        None => AtomicCostTable::default(),
    };

    let yaml_str = std::fs::read_to_string(&args.input_config)?;
    let config: ControllerConfig = serde_yaml::from_str(&yaml_str)?;
    let schema = config.schema_from_hints();

    let rqes: Vec<RQE> = config
        .query_groups
        .iter()
        .flat_map(|qg| {
            qg.queries.iter().map(|q| RQE {
                query_string: q.clone(),
                t_repeat_ms: qg.repetition_delay_ms,
            })
        })
        .collect();

    let aqes = extract_aqes(&rqes, &schema, args.scrape_interval_ms);
    println!("=== {} AQE(s) ===", aqes.len());

    for (i, aqe) in aqes.iter().enumerate() {
        println!(
            "\n--- AQE #{i}: metric={} stat={:?} range={}ms min_t={}ms gcd_t={}ms freq={:.4}Hz ---",
            aqe.requirements.metric,
            aqe.requirements.statistics,
            aqe.requirements.data_range_ms,
            aqe.min_t_repeat_ms,
            aqe.t_repeat_gcd_ms,
            aqe.query_frequency_hz,
        );
        println!("  queries: {:?}", aqe.query_strings);

        let candidates = enumerate_candidates(aqe, args.scrape_interval_ms);
        print_candidates_grouped(&candidates, &atomic_cost_table);
    }

    Ok(())
}

/// Group the flat candidate list by (agg_type, sub_type) and print hierarchically:
///   SketchType (sub_type)
///     windows (N):
///       ...
///     params (M)  [× N windows = NM total]:
///       ...
/// EXACT is printed last as a single line.
fn print_candidates_grouped(candidates: &[CandidateConfig], atomic_cost_table: &AtomicCostTable) {
    // Collect unique (agg_type_str, sub_type) keys in first-seen order.
    let mut group_order: Vec<(String, String)> = Vec::new();
    // (agg_type_str, sub_type) -> (agg_type, unique windows, unique params)
    type WindowKey = (WindowType, u64, u64, u64); // (type, size, slide, n)
    #[allow(clippy::type_complexity)]
    let mut groups: HashMap<
        (String, String),
        (AggregationType, Vec<WindowKey>, Vec<Vec<(String, Value)>>),
    > = HashMap::new();

    let mut has_exact = false;

    for c in candidates {
        let Some(cfg) = &c.config else {
            has_exact = true;
            continue;
        };

        let key = (
            format!("{:?}", cfg.aggregation_type),
            cfg.aggregation_sub_type.clone(),
        );

        let entry = groups.entry(key.clone()).or_insert_with(|| {
            group_order.push(key);
            (cfg.aggregation_type, Vec::new(), Vec::new())
        });

        let wk: WindowKey = (
            cfg.window_type,
            cfg.window_size_ms,
            cfg.slide_interval_ms,
            c.n_windows,
        );
        if !entry.1.contains(&wk) {
            entry.1.push(wk);
        }

        let mut params: Vec<(String, Value)> = cfg
            .parameters
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        params.sort_by(|(a, _), (b, _)| a.cmp(b));
        if !entry.2.contains(&params) {
            entry.2.push(params);
        }
    }

    let total_sketch: usize = group_order
        .iter()
        .map(|k| {
            let (_, ws, ps) = &groups[k];
            ws.len() * ps.len()
        })
        .sum();
    let total = total_sketch + if has_exact { 1 } else { 0 };
    println!(
        "  {} candidate(s) across {} sketch group(s):",
        total,
        group_order.len()
    );

    for key in &group_order {
        let (agg_type, windows, params) = &groups[key];
        let (agg_type_str, sub_type) = key;
        let sub = if sub_type.is_empty() {
            String::new()
        } else {
            format!(" ({})", sub_type)
        };
        println!(
            "\n    {}{} — {} window(s) × {} param(s) = {} candidate(s):",
            agg_type_str,
            sub,
            windows.len(),
            params.len(),
            windows.len() * params.len(),
        );

        println!("      windows:");
        for (wtype, size, slide, n) in windows {
            let window_str = match wtype {
                WindowType::Tumbling => format!("Tumbling {}ms", size),
                WindowType::Sliding => format!("Sliding  {}ms / slide {}ms", size, slide),
            };
            println!(
                "        {} n={} → {:?}",
                window_str,
                n,
                // Re-derive method label from n and window type for display.
                if *n == 1 { "Direct" } else { "Merge/Subtract" }
            );
        }

        println!("      params:");
        for p in params {
            let kv: Vec<_> = p.iter().map(|(k, v)| format!("{k}: {v}")).collect();
            let costs_str = if atomic_cost_table.is_empty() {
                String::new()
            } else {
                let param_map: HashMap<String, Value> =
                    p.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                match resolve_atomic_costs(atomic_cost_table, *agg_type, &param_map) {
                    Some(costs) => {
                        let label = if costs == AtomicCosts::default() {
                            "stub"
                        } else {
                            "real"
                        };
                        format!("  -> [{label}] {}", format_costs(&costs))
                    }
                    None => "  -> DROPPED (no matching table row)".to_string(),
                }
            };
            println!("        {{{}}}{costs_str}", kv.join(", "));
        }
    }

    if has_exact {
        println!("\n    [EXACT]");
    }
}

fn format_costs(costs: &AtomicCosts) -> String {
    format!(
        "mem={:.0}B insert={:.3e}s merge={:.3e}s subtract={:.3e}s query={:.3e}s",
        costs.mem_bytes_per_instance,
        costs.insert_cpu_secs,
        costs.merge_cpu_secs,
        costs.subtract_cpu_secs,
        costs.query_cpu_secs,
    )
}
