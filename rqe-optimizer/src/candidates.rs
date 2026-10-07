//! Candidate generation and eligibility (§3).

use crate::analytical_cost_model;
use crate::{
    config_topk_k, has_heap, heap_capacity, heap_needed, Accuracy, AtomicCostEntry, Capability,
    Deployment, LabelSet, MetricFacts, Millis, Raqe, WorkloadFacts, KEY_TRACKER_FAMILY,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn gcd(a: Millis, b: Millis) -> Millis {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

fn divisors(value: Millis) -> Vec<Millis> {
    let mut out = Vec::new();
    let mut factor = 1;
    while factor <= value / factor {
        if value.is_multiple_of(factor) {
            out.push(factor);
            if factor != value / factor {
                out.push(value / factor);
            }
        }
        factor += 1;
    }
    out.sort_unstable();
    out
}

/// The gcd of every non-empty subset of `values`, in one pass without listing
/// subsets: `gcd(A ∪ {v}) = gcd(gcd(A), v)`, so each new value extends every
/// gcd found so far.
fn subset_gcds(values: impl IntoIterator<Item = Millis>) -> BTreeSet<Millis> {
    let mut gcds = BTreeSet::new();
    for value in values {
        let extended: Vec<_> = gcds.iter().map(|&found| gcd(found, value)).collect();
        gcds.insert(value);
        gcds.extend(extended);
    }
    gcds
}

/// Whether a fixed-size sketch shared by all groups was measured holding at
/// least `groups` of them. Its accuracy degrades as groups grow, and the row
/// holds one measured point (`subpopulations`). Models a sketch keyed by the
/// joined `G` value alone, so it holds exactly `card(G)` subpopulations; the
/// current rows fan out to every label subset (sketch-bench#165).
// ponytail: one point, so larger groupings are never planned. Rows at more
// group counts (sketch-bench#143 S5) loosen this with no code change.
fn measured_at_group_count(config: &AtomicCostEntry, groups: u64) -> bool {
    config
        .query_accuracy
        .get("subpopulations")
        .is_some_and(|&measured| groups as f64 <= measured)
}

/// Windows and slides are multiples of the metric's scrape interval: anything
/// finer only splits one scrape's samples.
fn candidate_deployments(
    group: &[&Raqe],
    costs: &[AtomicCostEntry],
    metric_facts: &MetricFacts,
    allow_undeployable_families: bool,
) -> Vec<Deployment> {
    if group.is_empty() {
        return Vec::new();
    }
    let capability = group[0].capability;
    let scrape_interval_ms = metric_facts.scrape_interval_ms;
    // The tracker is deployable whenever the family needing it is: ASAPQuery
    // runs it as DeltaSetAggregator.
    let tracker_rows: Vec<_> = costs
        .iter()
        .filter(|c| c.sketch == KEY_TRACKER_FAMILY)
        .collect();
    assert!(
        tracker_rows.len() <= 1,
        "{} {KEY_TRACKER_FAMILY} rows in the cost table; the tracker's price would depend on row order",
        tracker_rows.len()
    );
    let tracker_row = tracker_rows.first().copied();
    let family_rows: Vec<&AtomicCostEntry> = costs
        .iter()
        .filter(|c| {
            capability
                .candidate_families(allow_undeployable_families)
                .any(|family| family == c.sketch)
        })
        .collect();
    let heap_rows = heap_rows_by_shape(&family_rows);
    let windows = group
        .iter()
        .flat_map(|r| divisors(r.lookback_ms / scrape_interval_ms))
        .map(|scrapes| scrapes * scrape_interval_ms)
        .collect::<BTreeSet<_>>();
    let mut deployments = Vec::new();
    for window_ms in windows {
        // gcd(x, T) is the largest slide that serves an RAQE. For RAQEs sharing a
        // deployment, the gcd of theirs is strictly better than any finer
        // slide that serves them all. Any subset might share.
        let slides = subset_gcds(
            group
                .iter()
                .filter(|r| r.lookback_ms % window_ms == 0)
                .map(|r| gcd(window_ms, r.interval_ms)),
        );
        // A heap family keeps one candidate per (heap, k) a RAQE on this
        // window needs, m · k for its m = L / x merged windows, priced by
        // interpolating the measured heap sizes. Other rows are used as they are.
        let heaps: BTreeSet<(u64, u64)> = group
            .iter()
            .filter_map(|r| {
                Some((
                    heap_needed(r.lookback_ms, window_ms, r.topk_k())?,
                    r.topk_k(),
                ))
            })
            .collect();
        let configs: Vec<AtomicCostEntry> = family_rows
            .iter()
            .filter(|c| !has_heap(&c.sketch))
            .map(|c| (*c).clone())
            .chain(heap_rows.values().flat_map(|rows| {
                // Needs that one measured row serves collapse onto it.
                let by_heap: BTreeMap<(u64, u64), AtomicCostEntry> = heaps
                    .iter()
                    .filter_map(|&(heap, k)| heap_row(rows, heap, k))
                    .filter_map(|entry| {
                        Some(((heap_capacity(&entry)?, config_topk_k(&entry)), entry))
                    })
                    .collect();
                by_heap.into_values()
            }))
            .collect();
        for config in &configs {
            let key_tracker =
                if crate::family_properties(&config.sketch).needs_delta_set_key_tracker {
                    match tracker_row {
                        Some(tracker) => Some(tracker.clone()),
                        None => continue,
                    }
                } else {
                    None
                };
            for &slide_ms in &slides {
                if slide_ms.is_multiple_of(scrape_interval_ms) {
                    deployments.push(Deployment {
                        capability,
                        metric: group[0].metric.clone(),
                        spatial_filter: group[0].spatial_filter.clone(),
                        grouping_labels: group[0].grouping_labels.clone(),
                        config: config.clone(),
                        window_ms,
                        slide_ms,
                        key_tracker: key_tracker.clone(),
                    });
                }
            }
        }
    }
    deployments
}

/// Whether `capability` asks for a top-k list.
pub(crate) fn is_topk(capability: Capability) -> bool {
    matches!(
        capability,
        Capability::TopKByValue | Capability::TopKByCount
    )
}

/// A heap family's rows, by family and counter shape (every param but
/// `heap`), then by heap capacity.
pub(crate) fn heap_rows_by_shape<'a>(
    rows: &[&'a AtomicCostEntry],
) -> BTreeMap<(String, String), BTreeMap<u64, &'a AtomicCostEntry>> {
    let mut out: BTreeMap<_, BTreeMap<u64, &AtomicCostEntry>> = BTreeMap::new();
    for &row in rows {
        let Some(heap) = heap_capacity(row) else {
            continue;
        };
        let mut shape = row.sketch_config["params"].clone();
        if let Some(params) = shape.as_object_mut() {
            params.remove("heap");
        }
        let shape = shape.to_string();
        if out
            .entry((row.sketch.clone(), shape.clone()))
            .or_default()
            .insert(heap, row)
            .is_some()
        {
            panic!(
                "two {} {shape} cost rows at heap {heap} (e.g. no heap and heap={heap}); \
                 keep one",
                row.sketch
            );
        }
    }
    out
}

/// `rows` (one family and counter shape, by heap) priced for a top-`k`
/// answer needing `heap`: at `heap`, but never below the smallest measured
/// heap (nothing is extrapolated below it). The config names its heap
/// explicitly and its `topk_k`, so the deployed sketch answers `k` with the
/// heap that was priced. The MILP's candidates and AutoSketch both price
/// heaps here.
pub(crate) fn heap_row(
    rows: &BTreeMap<u64, &AtomicCostEntry>,
    heap: u64,
    k: u64,
) -> Option<AtomicCostEntry> {
    let (&smallest, _) = rows.first_key_value()?;
    let heap = heap.max(smallest);
    let mut entry = at_heap(rows, heap)?;
    // The priced heap, explicit: with no `heap`, a `topk_k` below it would
    // read (and build) as a heap of `k`.
    let priced = heap_capacity(&entry)?;
    let params = entry.sketch_config["params"].as_object_mut()?;
    params.insert("heap".into(), priced.into());
    params.remove("topk_k");
    if let Some(k) = crate::topk_k_param(k) {
        params.insert("topk_k".into(), k.into());
    }
    Some(entry)
}

/// `rows` (one family and counter shape, by heap) priced at `heap`: each
/// per-op cost linear in the heap between the measured sizes around it, and
/// extended from the two nearest past them. With one measured size, the
/// smallest row holding at least `heap` serves at its own size and cost.
/// The accuracy fields are the smallest heap's: curves are measured at
/// `TOPK_K`. Panics when one measured heap is smaller than `heap`: the
/// table needs the heap sizes measured.
fn at_heap(rows: &BTreeMap<u64, &AtomicCostEntry>, heap: u64) -> Option<AtomicCostEntry> {
    let (_, &base) = rows.first_key_value()?;
    // A measured heap's costs, with the base's accuracy like every other.
    let measured = |row: &AtomicCostEntry| AtomicCostEntry {
        query_accuracy: base.query_accuracy.clone(),
        ..row.clone()
    };
    if let Some(row) = rows.get(&heap) {
        return Some(measured(row));
    }
    if rows.len() < 2 {
        let Some((_, row)) = rows.range(heap..).next() else {
            panic!(
                "{} {} is priced at one heap only, smaller than the heap {heap} a \
                 candidate needs; rerun study_saturation.py --phase optimizer-cost to \
                 measure the top-k heap sizes",
                base.sketch, base.sketch_config["params"]
            );
        };
        return Some(measured(row));
    }
    let mut below = rows.range(..heap).rev();
    let mut above = rows.range(heap..);
    let ((h0, a), (h1, b)) = match (below.next(), above.next()) {
        (Some(lo), Some(hi)) => (lo, hi),
        (Some(hi), None) => (below.next()?, hi),
        (None, Some(lo)) => (lo, above.next()?),
        (None, None) => return None,
    };
    let t = (heap as f64 - *h0 as f64) / (*h1 as f64 - *h0 as f64);
    let line = |x: f64, y: f64| (x + t * (y - x)).max(0.0);
    let mut entry = AtomicCostEntry {
        mem_bytes_per_instance: line(a.mem_bytes_per_instance, b.mem_bytes_per_instance),
        insert_cpu_secs: line(a.insert_cpu_secs, b.insert_cpu_secs),
        merge_cpu_secs: line(a.merge_cpu_secs, b.merge_cpu_secs),
        query_cpu_secs: line(a.query_cpu_secs, b.query_cpu_secs),
        ..(*base).clone()
    };
    entry.sketch_config["params"]["heap"] = heap.into();
    Some(entry)
}

/// Configurations come from [`Capability::candidate_families`].
pub fn build_all_candidates(
    raqes: &[Raqe],
    costs: &[AtomicCostEntry],
    facts: &WorkloadFacts,
    allow_undeployable_families: bool,
    accuracy: &Accuracy,
) -> Vec<Deployment> {
    prune_dominated_candidates(
        raqes,
        facts,
        build_all_candidates_unpruned(raqes, costs, facts, allow_undeployable_families),
        accuracy,
    )
}

/// Generate the complete v1 candidate set before dominance pruning.
///
/// The public optimizer entry point is [`build_all_candidates`]. This helper
/// exists so diagnostics can report exactly how much safe pruning removed.
pub fn build_all_candidates_unpruned(
    raqes: &[Raqe],
    costs: &[AtomicCostEntry],
    facts: &WorkloadFacts,
    allow_undeployable_families: bool,
) -> Vec<Deployment> {
    let mut groups: BTreeMap<(Capability, &str, &str, &LabelSet), Vec<&Raqe>> = BTreeMap::new();
    for raqe in raqes {
        groups
            .entry((
                raqe.capability,
                &raqe.metric,
                &raqe.spatial_filter,
                &raqe.grouping_labels,
            ))
            .or_default()
            .push(raqe);
    }
    groups
        .iter()
        .flat_map(|(&(_, metric, _, _), group)| {
            candidate_deployments(group, costs, &facts[metric], allow_undeployable_families)
        })
        .collect()
}

/// Remove a candidate only when another candidate can replace it in every
/// mapping without making any modeled objective worse.
///
/// This comparison is local to a (capability, metric, spatial_filter,
/// grouping) group, where query output size is common. It compares ingest CPU
/// and memory, then latency (merge and query CPU), merge memory and stored
/// memory for each RAQE the dominated candidate can serve, all as the
/// analytical cost model prices them: a sketch shared by all groups and a
/// sketch per group don't scale alike with `card(G)`.
pub fn prune_dominated_candidates(
    raqes: &[Raqe],
    facts: &WorkloadFacts,
    candidates: Vec<Deployment>,
    accuracy: &Accuracy,
) -> Vec<Deployment> {
    let costs: Vec<CandidateCosts> = candidates
        .iter()
        .map(|candidate| CandidateCosts::new(candidate, raqes, facts, accuracy))
        .collect();

    candidates
        .iter()
        .enumerate()
        .filter_map(|(candidate_index, candidate)| {
            let dominated = (0..candidates.len()).any(|other_index| {
                if other_index == candidate_index {
                    return false;
                }
                match dominance(
                    &candidates[other_index],
                    &costs[other_index],
                    candidate,
                    &costs[candidate_index],
                ) {
                    Dominance::StrictlyBetter => true,
                    // The earlier of two identical candidates wins, so a tie
                    // never removes both.
                    Dominance::Equal => other_index < candidate_index,
                    Dominance::NotDominating => false,
                }
            });
            (!dominated).then(|| candidate.clone())
        })
        .collect()
}

/// One candidate's costs, computed once for every pairwise comparison.
struct CandidateCosts {
    /// Ingest CPU and memory.
    ingest: [f64; 2],
    /// Per RAQE: latency, merge memory and stored memory if the candidate
    /// serves it, else `None`.
    per_raqe: Vec<Option<[f64; 3]>>,
}

impl CandidateCosts {
    fn new(
        candidate: &Deployment,
        raqes: &[Raqe],
        facts: &WorkloadFacts,
        accuracy: &Accuracy,
    ) -> Self {
        let ingest = analytical_cost_model::ingest(candidate, facts);
        let per_raqe = raqes
            .iter()
            .map(|raqe| {
                is_eligible(raqe, candidate, facts, accuracy).then(|| {
                    [
                        analytical_cost_model::query_latency_ms(raqe, candidate, facts),
                        analytical_cost_model::merge(raqe, candidate, facts).memory_bytes,
                        analytical_cost_model::storage_bytes(raqe, candidate, facts),
                    ]
                })
            })
            .collect();
        Self {
            ingest: [ingest.cpu_secs_per_sec, ingest.memory_bytes],
            per_raqe,
        }
    }
}

enum Dominance {
    StrictlyBetter,
    Equal,
    NotDominating,
}

/// Whether `replacement` can stand in for `original`: it serves every RAQE
/// `original` serves and is no worse on any cost.
fn dominance(
    replacement: &Deployment,
    replacement_costs: &CandidateCosts,
    original: &Deployment,
    original_costs: &CandidateCosts,
) -> Dominance {
    let same_group = replacement.capability == original.capability
        && replacement.metric == original.metric
        && replacement.spatial_filter == original.spatial_filter
        && replacement.grouping_labels == original.grouping_labels;
    let covers_original = original_costs
        .per_raqe
        .iter()
        .zip(&replacement_costs.per_raqe)
        .all(|(original_serves, replacement_serves)| {
            original_serves.is_none() || replacement_serves.is_some()
        });
    if !same_group || !covers_original {
        return Dominance::NotDominating;
    }

    // Each pair is (replacement's cost, original's cost), over the RAQEs
    // `original` serves.
    let mut costs: Vec<(f64, f64)> = replacement_costs
        .ingest
        .into_iter()
        .zip(original_costs.ingest)
        .collect();
    for (replacement_raqe, original_raqe) in replacement_costs
        .per_raqe
        .iter()
        .zip(&original_costs.per_raqe)
    {
        if let (Some(replacement_raqe), Some(original_raqe)) = (replacement_raqe, original_raqe) {
            costs.extend(
                replacement_raqe
                    .iter()
                    .copied()
                    .zip(original_raqe.iter().copied()),
            );
        }
    }

    let serves_more = original_costs
        .per_raqe
        .iter()
        .zip(&replacement_costs.per_raqe)
        .any(|(original_serves, replacement_serves)| {
            original_serves.is_none() && replacement_serves.is_some()
        });
    if costs
        .iter()
        .any(|(replacement_cost, original_cost)| replacement_cost > original_cost)
    {
        Dominance::NotDominating
    } else if serves_more
        || costs
            .iter()
            .any(|(replacement_cost, original_cost)| replacement_cost < original_cost)
    {
        Dominance::StrictlyBetter
    } else {
        Dominance::Equal
    }
}

pub fn eligible_deployments_for(
    r: &Raqe,
    deployments: &[Deployment],
    facts: &WorkloadFacts,
    accuracy: &Accuracy,
) -> Vec<usize> {
    deployments
        .iter()
        .enumerate()
        .filter(|(_, d)| is_eligible(r, d, facts, accuracy))
        .map(|(i, _)| i)
        .collect()
}

/// Whether `d` can serve `r`. Every rule lives here, so the MILP, enumeration
/// and candidate pruning agree on what is valid.
pub fn is_eligible(r: &Raqe, d: &Deployment, facts: &WorkloadFacts, accuracy: &Accuracy) -> bool {
    let properties = d.properties();
    r.capability == d.capability
        && r.metric == d.metric
        && r.spatial_filter == d.spatial_filter
        && r.grouping_labels == d.grouping_labels
        && d.window_ms != 0
        && d.slide_ms != 0
        && d.window_ms.is_multiple_of(d.slide_ms)
        && r.lookback_ms.is_multiple_of(d.window_ms)
        && r.interval_ms.is_multiple_of(d.slide_ms)
        && (properties.mergeable_across_windows || r.lookback_ms == d.window_ms)
        && properties.needs_delta_set_key_tracker == d.key_tracker.is_some()
        && (!properties.one_fixed_size_sketch_for_all_groups
            || measured_at_group_count(&d.config, facts[&d.metric].cardinality[&d.grouping_labels]))
        && heap_capacity(&d.config).is_none_or(|heap| {
            d.heap_needed(r.lookback_ms, r.topk_k())
                .is_some_and(|need| heap >= need)
        })
        && (!is_topk(r.capability) || crate::serves_topk_k(&d.config, r.topk_k()))
        && r.meets_sla(&d.config.sketch, accuracy(r, d))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{facts, measured_at, metric_of, perfect_accuracy, METRIC};
    use crate::{table_accuracy, LabelSet, TOPK_K};
    use std::collections::BTreeMap;
    fn raqe(id: &str, lookback: Millis, interval: Millis) -> Raqe {
        Raqe {
            id: id.into(),
            capability: Capability::TopKByValue,
            lookback_ms: lookback,
            interval_ms: interval,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_sla: 0.5,
            latency_sla_ms: None,
            topk_k: None,
        }
    }
    fn cost() -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "cms-heap-topk-fastpath-vector2d".into(),
            // A heap that holds any merge, so heap top-k fixtures merge freely.
            sketch_config: serde_json::json!({"params": {"heap": 1u64 << 40}}),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 1.0,
            merge_cpu_secs: 1.0,
            query_cpu_secs: 1.0,
            query_accuracy: perfect_accuracy(),
            accuracy_metric: "precision_at_k".into(),
            measured_at: measured_at(),
        }
    }
    /// A heap top-k row measured at `heap`, its costs `heap` per op.
    fn heap_row(heap: u64) -> AtomicCostEntry {
        let h = heap as f64;
        AtomicCostEntry {
            sketch_config: serde_json::json!({"params": {"rows": 3, "cols": 256, "heap": heap}}),
            mem_bytes_per_instance: 1000.0 + h,
            insert_cpu_secs: h,
            merge_cpu_secs: 2.0 * h,
            query_cpu_secs: 3.0 * h,
            ..cost()
        }
    }

    #[test]
    fn heap_costs_are_linear_between_and_past_the_measured_sizes() {
        let (a, b, c) = (heap_row(32), heap_row(128), heap_row(512));
        let rows = BTreeMap::from([(32, &a), (128, &b), (512, &c)]);
        let at = |heap| at_heap(&rows, heap).unwrap();
        assert_eq!(at(128), b);
        let between = at(64);
        assert_eq!(between.mem_bytes_per_instance, 1064.0);
        assert_eq!(between.merge_cpu_secs, 128.0);
        assert_eq!(heap_capacity(&between), Some(64));
        // Past the largest: the line through the two largest.
        assert_eq!(at(2048).query_cpu_secs, 3.0 * 2048.0);
        // One measured size: the smallest row holding the heap serves as is.
        let single = BTreeMap::from([(128, &b)]);
        assert_eq!(at_heap(&single, 64), Some(b.clone()));
    }

    /// A table priced at one heap can't price a larger one: planning stops
    /// and names the rerun, rather than dropping the candidate.
    #[test]
    #[should_panic(expected = "rerun study_saturation.py --phase optimizer-cost")]
    fn one_measured_heap_cannot_price_a_larger_one() {
        let b = heap_row(128);
        at_heap(&BTreeMap::from([(128, &b)]), 256);
    }

    /// No heap and heap = k are one heap: two such rows would overwrite each
    /// other.
    #[test]
    #[should_panic(expected = "cost rows at heap 32")]
    fn two_rows_at_one_heap_are_refused() {
        let explicit = heap_row(32);
        let mut implicit = heap_row(32);
        implicit.sketch_config["params"]
            .as_object_mut()
            .unwrap()
            .remove("heap");
        heap_rows_by_shape(&[&explicit, &implicit]);
    }

    /// Each window gets the heaps its RAQEs need, m · k for m = L / x, and a
    /// RAQE is eligible only where the heap holds its m · k.
    #[test]
    fn a_heap_candidate_holds_m_times_k_for_its_merges() {
        let raqes = vec![raqe("a", 60_000, 60_000), raqe("b", 240_000, 60_000)];
        let rows = [heap_row(32), heap_row(128), heap_row(512)];
        let candidates = build_all_candidates_unpruned(&raqes, &rows, &facts(1, 1), false);
        let heaps_at = |window_ms: Millis| {
            candidates
                .iter()
                .filter(|d| d.window_ms == window_ms)
                .filter_map(|d| heap_capacity(&d.config))
                .collect::<BTreeSet<_>>()
        };
        // x = 60 s: a merges 1 (32), b merges 4 (128).
        assert_eq!(heaps_at(60_000), BTreeSet::from([32, 128]));
        // x = 30 s: a merges 2 (64, interpolated), b merges 8 (256).
        assert_eq!(heaps_at(30_000), BTreeSet::from([64, 256]));
        let d = candidates
            .iter()
            .find(|d| d.window_ms == 60_000 && heap_capacity(&d.config) == Some(32))
            .unwrap();
        assert!(is_eligible(&raqes[0], d, &facts(1, 1), &table_accuracy));
        assert!(!is_eligible(&raqes[1], d, &facts(1, 1), &table_accuracy));
    }

    /// A top-10 RAQE needs m · 10: its heaps follow its own k, and a heap
    /// built for k = 10 doesn't serve a k = 32 RAQE at the same merges.
    #[test]
    fn a_heap_candidate_follows_each_raqes_k() {
        let ten = Raqe {
            topk_k: Some(10),
            ..raqe("ten", 240_000, 60_000)
        };
        let raqes = vec![ten.clone(), raqe("b", 240_000, 60_000)];
        let rows = [heap_row(32), heap_row(128), heap_row(512)];
        let candidates = build_all_candidates_unpruned(&raqes, &rows, &facts(1, 1), false);
        let at = |heap| {
            candidates
                .iter()
                .find(|d| d.window_ms == 60_000 && heap_capacity(&d.config) == Some(heap))
                .unwrap()
        };
        // x = 60 s merges 4: k = 10 needs 40, k = 32 needs 128.
        assert_eq!(config_topk_k(&at(40).config), 10);
        assert_eq!(config_topk_k(&at(128).config), TOPK_K);
        assert!(is_eligible(&ten, at(40), &facts(1, 1), &table_accuracy));
        assert!(!is_eligible(
            &raqes[1],
            at(40),
            &facts(1, 1),
            &table_accuracy
        ));
        assert!(is_eligible(
            &raqes[1],
            at(128),
            &facts(1, 1),
            &table_accuracy
        ));
    }

    /// A k below the smallest measured heap prices at that heap, never
    /// extrapolated below it, and the config carries its `topk_k`.
    #[test]
    fn a_small_k_is_floored_at_the_smallest_measured_heap() {
        let rows = [heap_row(32), heap_row(128)];
        let refs: Vec<&AtomicCostEntry> = rows.iter().collect();
        let by_shape = heap_rows_by_shape(&refs);
        let shape = by_shape.values().next().unwrap();
        let entry = super::heap_row(shape, 10, 10).unwrap();
        assert_eq!(heap_capacity(&entry), Some(32));
        assert_eq!(config_topk_k(&entry), 10);
        assert_eq!(
            entry.mem_bytes_per_instance,
            heap_row(32).mem_bytes_per_instance
        );
    }

    /// A floor row measured with its heap implicit at k = 32 keeps that heap
    /// explicit once it serves a smaller k, so its capacity is the priced 32.
    #[test]
    fn the_floored_heap_is_explicit() {
        let mut implicit = heap_row(32);
        implicit.sketch_config = serde_json::json!({"params": {"rows": 3, "cols": 256}});
        let larger = heap_row(128);
        let rows = BTreeMap::from([(32, &implicit), (128, &larger)]);
        let entry = super::heap_row(&rows, 10, 10).unwrap();
        assert_eq!(entry.sketch_config["params"]["heap"], 32);
        assert_eq!(heap_capacity(&entry), Some(32));
        assert_eq!(config_topk_k(&entry), 10);
    }

    /// UnivMon's top-k answers TOPK_K only: a top-10 RAQE can't use it.
    #[test]
    fn univmon_topk_serves_only_topk_k() {
        let univmon = Deployment {
            config: AtomicCostEntry {
                sketch: "univmon-topk".into(),
                ..cost()
            },
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            window_ms: 60_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        let r = raqe("r", 60_000, 60_000);
        let ten = Raqe {
            topk_k: Some(10),
            ..r.clone()
        };
        assert!(is_eligible(&r, &univmon, &facts(1, 1), &table_accuracy));
        assert!(!is_eligible(&ten, &univmon, &facts(1, 1), &table_accuracy));
    }

    #[test]
    fn shared_slide_comes_from_subset_gcd_not_all_divisors() {
        let raqes = vec![raqe("a", 60_000, 20_000), raqe("b", 60_000, 30_000)];
        let candidates =
            build_all_candidates(&raqes, &[cost()], &facts(1, 1), false, &table_accuracy);
        let slides: BTreeSet<_> = candidates
            .iter()
            .filter(|d| d.window_ms == 60_000)
            .map(|d| d.slide_ms)
            .collect();
        assert_eq!(slides, BTreeSet::from([10_000, 20_000, 30_000]));
    }
    #[test]
    fn windows_and_slides_are_multiples_of_the_scrape_interval() {
        let raqes = vec![raqe("a", 60_000, 20_000), raqe("b", 60_000, 30_000)];
        let mut facts = facts(1, 1);
        facts.get_mut(METRIC).unwrap().scrape_interval_ms = 15_000;
        let candidates = build_all_candidates_unpruned(&raqes, &[cost()], &facts, false);
        assert!(!candidates.is_empty());
        // Without the filter, windows 1..60 and slides 10/20 would appear.
        assert!(candidates
            .iter()
            .all(|d| d.window_ms % 15_000 == 0 && d.slide_ms % 15_000 == 0));
    }
    #[test]
    fn different_spatial_filters_never_share_a_deployment() {
        let unfiltered = raqe("a", 60_000, 60_000);
        let filtered = Raqe {
            spatial_filter: r#"{job="api"}"#.into(),
            ..raqe("b", 60_000, 60_000)
        };
        let candidates = build_all_candidates(
            &[unfiltered.clone(), filtered.clone()],
            &[cost()],
            &facts(1, 1),
            false,
            &table_accuracy,
        );
        assert!(candidates.iter().all(|d| is_eligible(
            &unfiltered,
            d,
            &facts(1, 1),
            &table_accuracy
        ) != is_eligible(
            &filtered,
            d,
            &facts(1, 1),
            &table_accuracy
        )));
    }

    #[test]
    fn a_min_query_is_only_offered_the_min_accumulator() {
        let r = Raqe {
            capability: Capability::Min,
            ..raqe("r", 60_000, 60_000)
        };
        let named = |sketch: &str| AtomicCostEntry {
            sketch: sketch.into(),
            accuracy_metric: metric_of(sketch),
            ..cost()
        };
        let candidates = build_all_candidates(
            &[r],
            &[named("exact-min"), named("exact-max")],
            &facts(1, 1),
            false,
            &table_accuracy,
        );
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|d| d.config.sketch == "exact-min"));
    }

    #[test]
    fn undeployable_families_are_candidates_only_when_allowed() {
        let r = raqe("r", 60_000, 60_000);
        let named = |sketch: &str| AtomicCostEntry {
            sketch: sketch.into(),
            accuracy_metric: metric_of(sketch),
            ..cost()
        };
        let costs = [
            named("cms-heap-topk-fastpath-vector2d"),
            named("countsketch-heap-topk-fastpath-vector2d"),
        ];
        let candidates = build_all_candidates(
            std::slice::from_ref(&r),
            &costs,
            &facts(1, 1),
            false,
            &table_accuracy,
        );
        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|d| d.config.sketch == "cms-heap-topk-fastpath-vector2d"));
        let all = build_all_candidates_unpruned(&[r], &costs, &facts(1, 1), true);
        assert!(all
            .iter()
            .any(|d| d.config.sketch == "countsketch-heap-topk-fastpath-vector2d"));
    }

    #[test]
    fn eligibility_requires_exact_non_overlapping_tiling() {
        let r = raqe("r", 600_000, 180_000);
        let d = Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: cost(),
            window_ms: 120_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        assert!(is_eligible(&r, &d, &facts(1, 1), &table_accuracy));
        assert!(!is_eligible(
            &r,
            &Deployment {
                window_ms: 128_000,
                ..d.clone()
            },
            &facts(1, 1),
            &table_accuracy
        ));
        assert!(!is_eligible(
            &r,
            &Deployment {
                slide_ms: 70_000,
                ..d
            },
            &facts(1, 1),
            &table_accuracy
        ));
    }

    #[test]
    fn prunes_finer_slide_when_the_coarser_slide_serves_the_same_raqe() {
        let r = raqe("r", 60_000, 60_000);
        let coarse = Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: cost(),
            window_ms: 60_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        let fine = Deployment {
            slide_ms: 30_000,
            ..coarse.clone()
        };

        let retained = prune_dominated_candidates(
            &[r],
            &facts(1, 1),
            vec![fine, coarse.clone()],
            &table_accuracy,
        );

        assert_eq!(retained, vec![coarse]);
    }

    #[test]
    fn retains_candidate_with_lower_query_latency() {
        let r = raqe("r", 60_000, 60_000);
        let cost = cost();
        let large_window = Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: cost.clone(),
            window_ms: 60_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        let small_window = Deployment {
            window_ms: 30_000,
            slide_ms: 30_000,
            ..large_window.clone()
        };

        let retained = prune_dominated_candidates(
            &[r],
            &facts(1, 1),
            vec![small_window.clone(), large_window.clone()],
            &table_accuracy,
        );

        assert_eq!(retained, vec![large_window]);
    }

    #[test]
    fn retains_candidate_with_lower_stored_memory() {
        let r = raqe("r", 60_000, 60_000);
        // Stores (60 − 60) / 60 + 1 = 1 closed instance of 10 bytes.
        let whole_window = Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 10.0,
                ..cost()
            },
            window_ms: 60_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        // Smaller, faster sketches, but (60 − 20) / 20 + 1 = 3 of them: 18 bytes.
        let panes = Deployment {
            config: AtomicCostEntry {
                mem_bytes_per_instance: 6.0,
                query_cpu_secs: 0.5,
                merge_cpu_secs: 0.1,
                ..cost()
            },
            window_ms: 20_000,
            slide_ms: 20_000,
            ..whole_window.clone()
        };

        let retained = prune_dominated_candidates(
            &[r],
            &facts(1, 1),
            vec![whole_window.clone(), panes.clone()],
            &table_accuracy,
        );

        assert_eq!(retained, vec![whole_window, panes]);
    }

    #[test]
    fn retains_direct_query_candidate_that_never_merges() {
        let r = raqe("r", 60_000, 60_000);
        // x == L: no merge, so no merge memory.
        let direct = Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 10.0,
                ..cost()
            },
            window_ms: 60_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        // Smaller and cheaper on every other cost, but merges two windows,
        // holding a 4-byte accumulator per group.
        let halves = Deployment {
            config: AtomicCostEntry {
                mem_bytes_per_instance: 4.0,
                query_cpu_secs: 0.5,
                merge_cpu_secs: 0.1,
                ..cost()
            },
            window_ms: 30_000,
            slide_ms: 30_000,
            ..direct.clone()
        };

        let retained = prune_dominated_candidates(
            &[r],
            &facts(1, 1),
            vec![direct.clone(), halves.clone()],
            &table_accuracy,
        );

        assert_eq!(retained, vec![direct, halves]);
    }
    /// A whole-sketch hydra-kll row measured at `subpopulations` groups, and
    /// a per-key DeltaSet row.
    fn hydra_and_tracker(subpopulations: f64) -> [AtomicCostEntry; 2] {
        [
            AtomicCostEntry {
                sketch: "hydra-kll".into(),
                mem_bytes_per_instance: 1000.0,
                query_accuracy: BTreeMap::from([
                    ("mean_rank_err".into(), 0.1),
                    ("subpopulations".into(), subpopulations),
                ]),
                ..cost()
            },
            AtomicCostEntry {
                sketch: KEY_TRACKER_FAMILY.into(),
                ..cost()
            },
        ]
    }

    fn quantile_raqe() -> Raqe {
        Raqe {
            capability: Capability::Quantile,
            ..raqe("r", 60_000, 60_000)
        }
    }

    #[test]
    #[should_panic(expected = "exact-delta-set rows in the cost table")]
    fn two_tracker_rows_are_refused() {
        let [hydra, tracker] = hydra_and_tracker(100.0);
        build_all_candidates(
            &[quantile_raqe()],
            &[hydra, tracker.clone(), tracker],
            &facts(10, 10),
            true,
            &table_accuracy,
        );
    }
}
