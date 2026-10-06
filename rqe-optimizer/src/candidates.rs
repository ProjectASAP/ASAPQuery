//! Candidate generation and eligibility (§3).

use crate::analytical_cost_model::{instance_memory_bytes, merge_memory_per_group};
use crate::{AtomicCostEntry, Capability, Deployment, LabelSet, Millis, Raqe, WorkloadFacts};
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

/// Windows and slides are multiples of the metric's scrape interval: anything
/// finer only splits one scrape's samples.
fn candidate_deployments(
    group: &[&Raqe],
    costs: &[AtomicCostEntry],
    scrape_interval_ms: Millis,
    allow_undeployable_families: bool,
) -> Vec<Deployment> {
    if group.is_empty() {
        return Vec::new();
    }
    let capability = group[0].capability;
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
            candidate_deployments(
                group,
                costs,
                facts[metric].scrape_interval_ms,
                allow_undeployable_families,
            )
        })
        .collect()
}

/// Remove a candidate only when another candidate can replace it in every
/// mapping without making any modeled objective worse.
///
/// This comparison is deliberately local to a (capability, metric,
/// spatial_filter, grouping) group, where `card(G)`, arrival rate and query output size are common
/// multipliers. So it compares ingest CPU and memory per group, then latency
/// (merge and query CPU), merge memory and stored memory for each RAQE the
/// dominated candidate can serve.
pub fn prune_dominated_candidates(
    raqes: &[Raqe],
    facts: &WorkloadFacts,
    candidates: Vec<Deployment>,
) -> Vec<Deployment> {
    let eligibility: Vec<Vec<bool>> = candidates
        .iter()
        .map(|candidate| {
            raqes
                .iter()
                .map(|raqe| is_eligible(raqe, candidate))
                .collect()
        })
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
                    &eligibility[other_index],
                    candidate,
                    &eligibility[candidate_index],
                    raqes,
                    facts,
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

enum Dominance {
    StrictlyBetter,
    Equal,
    NotDominating,
}

/// Whether `replacement` can stand in for `original`: it serves every RAQE
/// `original` serves and is no worse on any cost.
fn dominance(
    replacement: &Deployment,
    replacement_coverage: &[bool],
    original: &Deployment,
    original_coverage: &[bool],
    raqes: &[Raqe],
    facts: &WorkloadFacts,
) -> Dominance {
    let same_group = replacement.capability == original.capability
        && replacement.metric == original.metric
        && replacement.spatial_filter == original.spatial_filter
        && replacement.grouping_labels == original.grouping_labels;
    let covers_original = original_coverage
        .iter()
        .zip(replacement_coverage)
        .all(|(&original_serves, &replacement_serves)| !original_serves || replacement_serves);
    if !same_group || !covers_original {
        return Dominance::NotDominating;
    }

    // Each pair is (replacement's cost, original's cost).
    let mut costs = vec![
        (ingest_cpu(replacement), ingest_cpu(original)),
        (
            ingest_memory(replacement, facts),
            ingest_memory(original, facts),
        ),
    ];
    for (raqe, _) in raqes
        .iter()
        .zip(original_coverage)
        .filter(|(_, &serves)| serves)
    {
        costs.push((
            query_latency(replacement, raqe),
            query_latency(original, raqe),
        ));
        costs.push((
            merge_memory_per_group(raqe, replacement, facts),
            merge_memory_per_group(raqe, original, facts),
        ));
        costs.push((
            stored_memory(replacement, raqe, facts),
            stored_memory(original, raqe, facts),
        ));
    }

    let serves_more = original_coverage
        .iter()
        .zip(replacement_coverage)
        .any(|(&original_serves, &replacement_serves)| !original_serves && replacement_serves);
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

fn open_window_count(deployment: &Deployment) -> f64 {
    deployment.active_instance_count().unwrap_or(u64::MAX) as f64
}

/// Insert CPU per sample. Arrival rate is a common multiplier within a group,
/// so it is left out.
fn ingest_cpu(deployment: &Deployment) -> f64 {
    open_window_count(deployment) * deployment.config.insert_cpu_secs
}

/// Open-window bytes per group. `card(G)` is a common multiplier within a
/// group, so it is left out.
fn ingest_memory(deployment: &Deployment, facts: &WorkloadFacts) -> f64 {
    open_window_count(deployment) * instance_memory_bytes(deployment, facts, deployment.window_ms)
}

fn query_latency(deployment: &Deployment, raqe: &Raqe) -> f64 {
    let instances = deployment
        .query_instance_count(raqe.lookback_ms)
        .expect("candidate coverage only contains exactly tiled RAQEs");
    deployment.config.query_cpu_secs
        + instances.saturating_sub(1) as f64 * deployment.config.merge_cpu_secs
}

/// Closed-window bytes per group when serving `raqe`.
fn stored_memory(deployment: &Deployment, raqe: &Raqe, facts: &WorkloadFacts) -> f64 {
    deployment
        .closed_instance_count(raqe.lookback_ms)
        .expect("candidate coverage only contains exactly tiled RAQEs") as f64
        * instance_memory_bytes(deployment, facts, deployment.window_ms)
}

pub fn eligible_deployments_for(r: &Raqe, deployments: &[Deployment]) -> Vec<usize> {
    deployments
        .iter()
        .enumerate()
        .filter(|(_, d)| is_eligible(r, d))
        .map(|(i, _)| i)
        .collect()
}

pub fn is_eligible(r: &Raqe, d: &Deployment) -> bool {
    r.capability == d.capability
        && r.metric == d.metric
        && r.spatial_filter == d.spatial_filter
        && r.grouping_labels == d.grouping_labels
        && d.window_ms != 0
        && d.slide_ms != 0
        && d.window_ms.is_multiple_of(d.slide_ms)
        && r.lookback_ms.is_multiple_of(d.window_ms)
        && r.interval_ms.is_multiple_of(d.slide_ms)
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
        };
        let r = raqe("a", 60_000, 60_000);
        // One window reads query_accuracy; 2 reads 1 and 4; 4 reads 4 alone.
        assert!(is_eligible(&r, &at_window(60_000)));
        assert!(is_eligible(&r, &at_window(30_000)));
        assert!(is_eligible(&r, &at_window(15_000)));
        // 12 and 20 merges bracket the bad count 16 from either side; 60
        // reads 16 and 64, so it fails too even though 64 alone passes.
        assert!(!is_eligible(&r, &at_window(5_000)));
        assert!(!is_eligible(&r, &at_window(3_000)));
        assert!(!is_eligible(&r, &at_window(1_000)));
        // Past the largest count reads only 64.
        assert!(is_eligible(&r, &at_window(500)));
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
            .all(|d| is_eligible(&unfiltered, d) != is_eligible(&filtered, d)));
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
        };
        assert!(is_eligible(&r, &d));
        assert!(!is_eligible(
            &r,
            &Deployment {
                window_ms: 128_000,
                ..d.clone()
            }
        ));
        assert!(!is_eligible(
            &r,
            &Deployment {
                slide_ms: 70_000,
                ..d
            }
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
}
