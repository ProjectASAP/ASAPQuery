//! Candidate generation and eligibility (§3).

use crate::analytical_cost_model;
use crate::{
    AtomicCostEntry, Capability, Deployment, LabelSet, MetricFacts, Millis, Raqe, WorkloadFacts,
    KEY_TRACKER_FAMILY,
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
        for config in costs.iter().filter(|c| {
            capability
                .candidate_families(allow_undeployable_families)
                .any(|family| family == c.sketch)
        }) {
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

/// Configurations come from [`Capability::candidate_families`].
pub fn build_all_candidates(
    raqes: &[Raqe],
    costs: &[AtomicCostEntry],
    facts: &WorkloadFacts,
    allow_undeployable_families: bool,
) -> Vec<Deployment> {
    prune_dominated_candidates(
        raqes,
        facts,
        build_all_candidates_unpruned(raqes, costs, facts, allow_undeployable_families),
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
) -> Vec<Deployment> {
    let costs: Vec<CandidateCosts> = candidates
        .iter()
        .map(|candidate| CandidateCosts::new(candidate, raqes, facts))
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
    fn new(candidate: &Deployment, raqes: &[Raqe], facts: &WorkloadFacts) -> Self {
        let ingest = analytical_cost_model::ingest(candidate, facts);
        let per_raqe = raqes
            .iter()
            .map(|raqe| {
                is_eligible(raqe, candidate, facts).then(|| {
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
) -> Vec<usize> {
    deployments
        .iter()
        .enumerate()
        .filter(|(_, d)| is_eligible(r, d, facts))
        .map(|(i, _)| i)
        .collect()
}

/// Whether `d` can serve `r`. Every rule lives here, so the MILP, enumeration
/// and candidate pruning agree on what is valid.
pub fn is_eligible(r: &Raqe, d: &Deployment, facts: &WorkloadFacts) -> bool {
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
        && r.accuracy_ok_for(&d.config, r.lookback_ms / d.window_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{facts, METRIC};
    use crate::{AccuracyDirection, LabelSet};
    use std::collections::BTreeMap;
    fn raqe(id: &str, lookback: Millis, interval: Millis) -> Raqe {
        Raqe {
            id: id.into(),
            capability: Capability::TopK,
            lookback_ms: lookback,
            interval_ms: interval,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: "err".into(),
            accuracy_sla: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        }
    }
    fn cost() -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "cms-heap-topk-fastpath-vector2d".into(),
            sketch_config: serde_json::json!(null),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 1.0,
            merge_cpu_secs: 1.0,
            query_cpu_secs: 1.0,
            query_accuracy: BTreeMap::from([("err".into(), 0.1)]),
            merge_accuracy: BTreeMap::new(),
            measured_at: None,
        }
    }
    #[test]
    fn eligibility_checks_accuracy_at_the_queries_merge_count() {
        let mut config = cost();
        let err = |v: f64| BTreeMap::from([("err".into(), v)]);
        // Error is not monotone in the count: 16 is worse than 4 and 64.
        config.merge_accuracy = BTreeMap::from([(4, err(0.2)), (16, err(2.0)), (64, err(0.3))]);
        let at_window = |window_ms| Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: config.clone(),
            window_ms,
            slide_ms: window_ms,
            key_tracker: None,
        };
        let r = raqe("a", 60_000, 60_000);
        // One window reads query_accuracy; 2 reads 1 and 4; 4 reads 4 alone.
        assert!(is_eligible(&r, &at_window(60_000), &facts(1, 1)));
        assert!(is_eligible(&r, &at_window(30_000), &facts(1, 1)));
        assert!(is_eligible(&r, &at_window(15_000), &facts(1, 1)));
        // 12 and 20 merges bracket the bad count 16 from either side; 60
        // reads 16 and 64, so it fails too even though 64 alone passes.
        assert!(!is_eligible(&r, &at_window(5_000), &facts(1, 1)));
        assert!(!is_eligible(&r, &at_window(3_000), &facts(1, 1)));
        assert!(!is_eligible(&r, &at_window(1_000), &facts(1, 1)));
        // Past the largest count reads only 64.
        assert!(is_eligible(&r, &at_window(500), &facts(1, 1)));
    }
    #[test]
    fn shared_slide_comes_from_subset_gcd_not_all_divisors() {
        let raqes = vec![raqe("a", 60_000, 20_000), raqe("b", 60_000, 30_000)];
        let candidates = build_all_candidates(&raqes, &[cost()], &facts(1, 1), false);
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
        );
        assert!(candidates
            .iter()
            .all(|d| is_eligible(&unfiltered, d, &facts(1, 1))
                != is_eligible(&filtered, d, &facts(1, 1))));
    }

    #[test]
    fn a_min_query_is_only_offered_the_min_accumulator() {
        let r = Raqe {
            capability: Capability::Min,
            ..raqe("r", 60_000, 60_000)
        };
        let named = |sketch: &str| AtomicCostEntry {
            sketch: sketch.into(),
            ..cost()
        };
        let candidates = build_all_candidates(
            &[r],
            &[named("exact-min"), named("exact-max")],
            &facts(1, 1),
            false,
        );
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|d| d.config.sketch == "exact-min"));
    }

    #[test]
    fn undeployable_families_are_candidates_only_when_allowed() {
        let r = Raqe {
            capability: Capability::Quantile,
            ..raqe("r", 60_000, 60_000)
        };
        let named = |sketch: &str| AtomicCostEntry {
            sketch: sketch.into(),
            ..cost()
        };
        let costs = [named("kll-percall"), named("dd")];
        let candidates =
            build_all_candidates(std::slice::from_ref(&r), &costs, &facts(1, 1), false);
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|d| d.config.sketch == "kll-percall"));
        let all = build_all_candidates_unpruned(&[r], &costs, &facts(1, 1), true);
        assert!(all.iter().any(|d| d.config.sketch == "dd"));
    }

    #[test]
    fn eligibility_requires_exact_non_overlapping_tiling() {
        let r = raqe("r", 600_000, 180_000);
        let d = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: cost(),
            window_ms: 120_000,
            slide_ms: 60_000,
            key_tracker: None,
        };
        assert!(is_eligible(&r, &d, &facts(1, 1)));
        assert!(!is_eligible(
            &r,
            &Deployment {
                window_ms: 128_000,
                ..d.clone()
            },
            &facts(1, 1)
        ));
        assert!(!is_eligible(
            &r,
            &Deployment {
                slide_ms: 70_000,
                ..d
            },
            &facts(1, 1)
        ));
    }

    #[test]
    fn prunes_finer_slide_when_the_coarser_slide_serves_the_same_raqe() {
        let r = raqe("r", 60_000, 60_000);
        let coarse = Deployment {
            capability: Capability::TopK,
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

        let retained = prune_dominated_candidates(&[r], &facts(1, 1), vec![fine, coarse.clone()]);

        assert_eq!(retained, vec![coarse]);
    }

    #[test]
    fn retains_candidate_with_lower_query_latency() {
        let r = raqe("r", 60_000, 60_000);
        let cost = cost();
        let large_window = Deployment {
            capability: Capability::TopK,
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
        );

        assert_eq!(retained, vec![large_window]);
    }

    #[test]
    fn retains_candidate_with_lower_stored_memory() {
        let r = raqe("r", 60_000, 60_000);
        // Stores (60 − 60) / 60 + 1 = 1 closed instance of 10 bytes.
        let whole_window = Deployment {
            capability: Capability::TopK,
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
        );

        assert_eq!(retained, vec![whole_window, panes]);
    }

    #[test]
    fn retains_direct_query_candidate_that_never_merges() {
        let r = raqe("r", 60_000, 60_000);
        // x == L: no merge, so no merge memory.
        let direct = Deployment {
            capability: Capability::TopK,
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

        let retained =
            prune_dominated_candidates(&[r], &facts(1, 1), vec![direct.clone(), halves.clone()]);

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
                    ("err".into(), 0.1),
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
    fn hydra_carries_the_delta_set_tracker_and_needs_one() {
        let costs = hydra_and_tracker(100.0);
        let candidates = build_all_candidates(&[quantile_raqe()], &costs, &facts(10, 10), true);
        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|d| d.key_tracker.as_ref() == Some(&costs[1])));
        let untracked = build_all_candidates(&[quantile_raqe()], &costs[..1], &facts(10, 10), true);
        assert!(untracked.is_empty());
    }

    #[test]
    fn hydra_is_planned_only_up_to_its_measured_group_count() {
        let costs = hydra_and_tracker(100.0);
        let servable = |groups| {
            let facts = facts(groups, groups);
            let candidates =
                build_all_candidates_unpruned(&[quantile_raqe()], &costs, &facts, true);
            !eligible_deployments_for(&quantile_raqe(), &candidates, &facts).is_empty()
        };
        assert!(servable(100));
        assert!(!servable(101));
    }

    #[test]
    fn a_hydra_deployment_without_its_tracker_is_never_eligible() {
        let costs = hydra_and_tracker(100.0);
        let facts = facts(10, 10);
        let candidates = build_all_candidates(&[quantile_raqe()], &costs, &facts, true);
        let untracked = Deployment {
            key_tracker: None,
            ..candidates[0].clone()
        };
        assert!(is_eligible(&quantile_raqe(), &candidates[0], &facts));
        assert!(!is_eligible(&quantile_raqe(), &untracked, &facts));
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
        );
    }

    #[test]
    fn a_shared_sketch_is_not_pruned_by_a_smaller_per_group_one() {
        // Same CPU per op, and 10 bytes per group against Hydra's 1000 for
        // the whole sketch: compared per group, Hydra is dominated. Over 100
        // groups, memory and ingest tie (the tracker is free here), and over
        // 2 merged windows Hydra's latency is 100 queries + 1 merge against
        // KLL's 100 + 100. Latency alone keeps Hydra.
        let r = Raqe {
            lookback_ms: 120_000,
            ..quantile_raqe()
        };
        let [hydra, tracker] = hydra_and_tracker(100.0);
        let kll = AtomicCostEntry {
            sketch: "kll-percall".into(),
            mem_bytes_per_instance: 10.0,
            ..cost()
        };
        let tracker = AtomicCostEntry {
            mem_bytes_per_instance: 0.0,
            insert_cpu_secs: 0.0,
            query_cpu_secs: 0.0,
            merge_cpu_secs: 0.0,
            ..tracker
        };
        let candidates = build_all_candidates(&[r], &[kll, hydra, tracker], &facts(100, 100), true);
        assert!(candidates.iter().any(|d| d.config.sketch == "hydra-kll"));
        assert!(candidates.iter().any(|d| d.config.sketch == "kll-percall"));
    }
}
