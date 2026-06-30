//! Dump all candidate configs produced by enumerate_candidates for each AQE,
//! before greedy selection. Useful for auditing what the candidate space looks like.

use std::collections::HashMap;
use std::path::PathBuf;

use asap_planner::{
    optimizer::{enumerate_candidates, extract_aqes, CandidateConfig, RQE},
    ControllerConfig,
};
use asap_types::enums::WindowType;
use clap::Parser;
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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

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
        print_candidates_grouped(&candidates);
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
fn print_candidates_grouped(candidates: &[CandidateConfig]) {
    // Collect unique (agg_type_str, sub_type) keys in first-seen order.
    let mut group_order: Vec<(String, String)> = Vec::new();
    // (agg_type_str, sub_type) -> (unique windows, unique params)
    type WindowKey = (WindowType, u64, u64, u64); // (type, size, slide, n)
    #[allow(clippy::type_complexity)]
    let mut groups: HashMap<(String, String), (Vec<WindowKey>, Vec<Vec<(String, Value)>>)> =
        HashMap::new();

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
            (Vec::new(), Vec::new())
        });

        let wk: WindowKey = (
            cfg.window_type,
            cfg.window_size_ms,
            cfg.slide_interval_ms,
            c.n_windows,
        );
        if !entry.0.contains(&wk) {
            entry.0.push(wk);
        }

        let mut params: Vec<(String, Value)> = cfg
            .parameters
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        params.sort_by(|(a, _), (b, _)| a.cmp(b));
        if !entry.1.contains(&params) {
            entry.1.push(params);
        }
    }

    let total_sketch: usize = group_order
        .iter()
        .map(|k| {
            let (ws, ps) = &groups[k];
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
        let (windows, params) = &groups[key];
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
            println!("        {{{}}}", kv.join(", "));
        }
    }

    if has_exact {
        println!("\n    [EXACT]");
    }
}
