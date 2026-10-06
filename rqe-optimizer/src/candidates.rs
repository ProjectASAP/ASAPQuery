//! Candidate generation and eligibility (§3).

use crate::analytical_cost_model::merge_memory_per_group;
use crate::{AtomicCostEntry, Capability, Deployment, LabelSet, Rqe, Seconds, WorkloadFacts};
use std::collections::{BTreeMap, BTreeSet};

pub fn gcd(a: Seconds, b: Seconds) -> Seconds {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

fn divisors(value: Seconds) -> Vec<Seconds> {
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
fn subset_gcds(values: impl IntoIterator<Item = Seconds>) -> BTreeSet<Seconds> {
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
    group: &[&Rqe],
    costs: &[AtomicCostEntry],
    scrape_interval_secs: Seconds,
) -> Vec<Deployment> {
    if group.is_empty() {
        return Vec::new();
    }
    let capability = group[0].capability;
    let windows = group
        .iter()
        .flat_map(|r| divisors(r.lookback_secs))
        .filter(|window_secs| window_secs.is_multiple_of(scrape_interval_secs))
        .collect::<BTreeSet<_>>();
    let mut deployments = Vec::new();
    for window_secs in windows {
        // gcd(x, T) is the largest slide that serves an RQE. For RQEs sharing a
        // deployment, the gcd of theirs is strictly better than any finer
        // slide that serves them all. Any subset might share.
        let slides = subset_gcds(
            group
                .iter()
                .filter(|r| r.lookback_secs % window_secs == 0)
                .map(|r| gcd(window_secs, r.interval_secs)),
        );
        for config in costs
            .iter()
            .filter(|c| capability.families().contains(&c.sketch.as_str()))
        {
            for &slide_secs in &slides {
                if slide_secs.is_multiple_of(scrape_interval_secs) {
                    deployments.push(Deployment {
                        capability,
                        metric: group[0].metric.clone(),
                        grouping_labels: group[0].grouping_labels.clone(),
                        config: config.clone(),
                        window_secs,
                        slide_secs,
                    });
                }
            }
        }
    }
    deployments
}

pub fn build_all_candidates(
    rqes: &[Rqe],
    costs: &[AtomicCostEntry],
    facts: &WorkloadFacts,
) -> Vec<Deployment> {
    prune_dominated_candidates(rqes, build_all_candidates_unpruned(rqes, costs, facts))
}

/// Generate the complete v1 candidate set before dominance pruning.
///
/// The public optimizer entry point is [`build_all_candidates`]. This helper
/// exists so diagnostics can report exactly how much safe pruning removed.
pub fn build_all_candidates_unpruned(
    rqes: &[Rqe],
    costs: &[AtomicCostEntry],
    facts: &WorkloadFacts,
) -> Vec<Deployment> {
    let mut groups: BTreeMap<(Capability, &str, &LabelSet), Vec<&Rqe>> = BTreeMap::new();
    for rqe in rqes {
        groups
            .entry((rqe.capability, &rqe.metric, &rqe.grouping_labels))
            .or_default()
            .push(rqe);
    }
    groups
        .iter()
        .flat_map(|(&(_, metric, _), group)| {
            candidate_deployments(group, costs, facts[metric].scrape_interval_secs)
        })
        .collect()
}

/// Remove a candidate only when another candidate can replace it in every
/// mapping without making any modeled objective worse.
///
/// This comparison is deliberately local to a (capability, metric, grouping)
/// group, where `card(G)`, arrival rate and query output size are common
/// multipliers. So it compares ingest CPU and memory per group, then latency
/// (merge and query CPU), merge memory and stored memory for each RQE the
/// dominated candidate can serve.
pub fn prune_dominated_candidates(rqes: &[Rqe], candidates: Vec<Deployment>) -> Vec<Deployment> {
    let eligibility: Vec<Vec<bool>> = candidates
        .iter()
        .map(|candidate| rqes.iter().map(|rqe| is_eligible(rqe, candidate)).collect())
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
                    rqes,
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

/// Whether `replacement` can stand in for `original`: it serves every RQE
/// `original` serves and is no worse on any cost.
fn dominance(
    replacement: &Deployment,
    replacement_coverage: &[bool],
    original: &Deployment,
    original_coverage: &[bool],
    rqes: &[Rqe],
) -> Dominance {
    let same_group = replacement.capability == original.capability
        && replacement.metric == original.metric
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
        (ingest_memory(replacement), ingest_memory(original)),
    ];
    for (rqe, _) in rqes
        .iter()
        .zip(original_coverage)
        .filter(|(_, &serves)| serves)
    {
        costs.push((
            query_latency(replacement, rqe),
            query_latency(original, rqe),
        ));
        costs.push((
            merge_memory_per_group(rqe, replacement),
            merge_memory_per_group(rqe, original),
        ));
        costs.push((
            stored_memory(replacement, rqe),
            stored_memory(original, rqe),
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
fn ingest_memory(deployment: &Deployment) -> f64 {
    open_window_count(deployment) * deployment.config.mem_bytes_per_instance
}

fn query_latency(deployment: &Deployment, rqe: &Rqe) -> f64 {
    let instances = deployment
        .query_instance_count(rqe.lookback_secs)
        .expect("candidate coverage only contains exactly tiled RQEs");
    deployment.config.query_cpu_secs
        + instances.saturating_sub(1) as f64 * deployment.config.merge_cpu_secs
}

/// Closed-window bytes per group when serving `rqe`.
fn stored_memory(deployment: &Deployment, rqe: &Rqe) -> f64 {
    deployment
        .closed_instance_count(rqe.lookback_secs)
        .expect("candidate coverage only contains exactly tiled RQEs") as f64
        * deployment.config.mem_bytes_per_instance
}

pub fn eligible_deployments_for(r: &Rqe, deployments: &[Deployment]) -> Vec<usize> {
    deployments
        .iter()
        .enumerate()
        .filter(|(_, d)| is_eligible(r, d))
        .map(|(i, _)| i)
        .collect()
}

pub fn is_eligible(r: &Rqe, d: &Deployment) -> bool {
    r.capability == d.capability
        && r.metric == d.metric
        && r.grouping_labels == d.grouping_labels
        && d.window_secs != 0
        && d.slide_secs != 0
        && d.window_secs.is_multiple_of(d.slide_secs)
        && r.lookback_secs.is_multiple_of(d.window_secs)
        && r.interval_secs.is_multiple_of(d.slide_secs)
        && r.accuracy_ok_for(&d.config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{facts, METRIC};
    use crate::{AccuracyDirection, LabelSet};
    use std::collections::BTreeMap;
    fn rqe(id: &str, lookback: Seconds, interval: Seconds) -> Rqe {
        Rqe {
            id: id.into(),
            capability: Capability::TopK,
            lookback_secs: lookback,
            interval_secs: interval,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: "err".into(),
            accuracy_tolerance: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
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
        }
    }
    #[test]
    fn shared_slide_comes_from_subset_gcd_not_all_divisors() {
        let rqes = vec![rqe("a", 60, 20), rqe("b", 60, 30)];
        let candidates = build_all_candidates(&rqes, &[cost()], &facts(1, 1));
        let slides: BTreeSet<_> = candidates
            .iter()
            .filter(|d| d.window_secs == 60)
            .map(|d| d.slide_secs)
            .collect();
        assert_eq!(slides, BTreeSet::from([10, 20, 30]));
    }
    #[test]
    fn windows_and_slides_are_multiples_of_the_scrape_interval() {
        let rqes = vec![rqe("a", 60, 20), rqe("b", 60, 30)];
        let mut facts = facts(1, 1);
        facts.get_mut(METRIC).unwrap().scrape_interval_secs = 15;
        let candidates = build_all_candidates_unpruned(&rqes, &[cost()], &facts);
        assert!(!candidates.is_empty());
        // Without the filter, windows 1..60 and slides 10/20 would appear.
        assert!(candidates
            .iter()
            .all(|d| d.window_secs % 15 == 0 && d.slide_secs % 15 == 0));
    }
    #[test]
    fn a_min_query_is_only_offered_the_min_accumulator() {
        let r = Rqe {
            capability: Capability::Min,
            ..rqe("r", 60, 60)
        };
        let named = |sketch: &str| AtomicCostEntry {
            sketch: sketch.into(),
            ..cost()
        };
        let candidates = build_all_candidates(
            &[r],
            &[named("exact-min"), named("exact-max")],
            &facts(1, 1),
        );
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|d| d.config.sketch == "exact-min"));
    }

    #[test]
    fn eligibility_requires_exact_non_overlapping_tiling() {
        let r = rqe("r", 600, 180);
        let d = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            config: cost(),
            window_secs: 120,
            slide_secs: 60,
        };
        assert!(is_eligible(&r, &d));
        assert!(!is_eligible(
            &r,
            &Deployment {
                window_secs: 128,
                ..d.clone()
            }
        ));
        assert!(!is_eligible(
            &r,
            &Deployment {
                slide_secs: 70,
                ..d
            }
        ));
    }

    #[test]
    fn prunes_finer_slide_when_the_coarser_slide_serves_the_same_rqe() {
        let r = rqe("r", 60, 60);
        let coarse = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            config: cost(),
            window_secs: 60,
            slide_secs: 60,
        };
        let fine = Deployment {
            slide_secs: 30,
            ..coarse.clone()
        };

        let retained = prune_dominated_candidates(&[r], vec![fine, coarse.clone()]);

        assert_eq!(retained, vec![coarse]);
    }

    #[test]
    fn retains_candidate_with_lower_query_latency() {
        let r = rqe("r", 60, 60);
        let cost = cost();
        let large_window = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            config: cost.clone(),
            window_secs: 60,
            slide_secs: 60,
        };
        let small_window = Deployment {
            window_secs: 30,
            slide_secs: 30,
            ..large_window.clone()
        };

        let retained =
            prune_dominated_candidates(&[r], vec![small_window.clone(), large_window.clone()]);

        assert_eq!(retained, vec![large_window]);
    }

    #[test]
    fn retains_candidate_with_lower_stored_memory() {
        let r = rqe("r", 60, 60);
        // Stores (60 − 60) / 60 + 1 = 1 closed instance of 10 bytes.
        let whole_window = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 10.0,
                ..cost()
            },
            window_secs: 60,
            slide_secs: 60,
        };
        // Smaller, faster sketches, but (60 − 20) / 20 + 1 = 3 of them: 18 bytes.
        let panes = Deployment {
            config: AtomicCostEntry {
                mem_bytes_per_instance: 6.0,
                query_cpu_secs: 0.5,
                merge_cpu_secs: 0.1,
                ..cost()
            },
            window_secs: 20,
            slide_secs: 20,
            ..whole_window.clone()
        };

        let retained = prune_dominated_candidates(&[r], vec![whole_window.clone(), panes.clone()]);

        assert_eq!(retained, vec![whole_window, panes]);
    }

    #[test]
    fn retains_direct_query_candidate_that_never_merges() {
        let r = rqe("r", 60, 60);
        // x == L: no merge, so no merge memory.
        let direct = Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 10.0,
                ..cost()
            },
            window_secs: 60,
            slide_secs: 60,
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
            window_secs: 30,
            slide_secs: 30,
            ..direct.clone()
        };

        let retained = prune_dominated_candidates(&[r], vec![direct.clone(), halves.clone()]);

        assert_eq!(retained, vec![direct, halves]);
    }
}
