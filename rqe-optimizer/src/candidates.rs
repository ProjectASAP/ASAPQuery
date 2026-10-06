//! Candidate generation and eligibility (§3).

use crate::{AtomicCostEntry, Capability, Deployment, LabelSet, Rqe, Seconds};
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

/// Every gcd reachable from a non-empty subset, folded incrementally rather
/// than by enumerating the powerset.
fn subset_gcds(values: impl IntoIterator<Item = Seconds>) -> BTreeSet<Seconds> {
    let mut out = BTreeSet::new();
    for value in values {
        let extensions: Vec<_> = out.iter().map(|&previous| gcd(previous, value)).collect();
        out.insert(value);
        out.extend(extensions);
    }
    out
}

fn candidate_deployments(group: &[&Rqe], costs: &[AtomicCostEntry]) -> Vec<Deployment> {
    if group.is_empty() {
        return Vec::new();
    }
    let capability = group[0].capability;
    let labels = group[0].labels.clone();
    let windows = group
        .iter()
        .flat_map(|r| divisors(r.lookback_secs))
        .collect::<BTreeSet<_>>();
    let mut deployments = Vec::new();
    for window_secs in windows {
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
            deployments.extend(slides.iter().map(|&slide_secs| Deployment {
                capability,
                labels: labels.clone(),
                config: config.clone(),
                window_secs,
                slide_secs,
            }));
        }
    }
    deployments
}

pub fn build_all_candidates(rqes: &[Rqe], costs: &[AtomicCostEntry]) -> Vec<Deployment> {
    prune_dominated_candidates(rqes, build_all_candidates_unpruned(rqes, costs))
}

/// Generate the complete v1 candidate set before dominance pruning.
///
/// The public optimizer entry point is [`build_all_candidates`]. This helper
/// exists so diagnostics can report exactly how much safe pruning removed.
pub fn build_all_candidates_unpruned(rqes: &[Rqe], costs: &[AtomicCostEntry]) -> Vec<Deployment> {
    let mut groups: BTreeMap<(Capability, LabelSet), Vec<&Rqe>> = BTreeMap::new();
    for rqe in rqes {
        groups
            .entry((rqe.capability, rqe.labels.clone()))
            .or_default()
            .push(rqe);
    }
    groups
        .values()
        .flat_map(|group| candidate_deployments(group, costs))
        .collect()
}

/// Remove a candidate only when another candidate can replace it in every
/// mapping without making any modeled objective worse.
///
/// This comparison is deliberately local to a capability/label-set group.
/// Within such a group, label cardinality and arrival rate are common
/// multipliers, so comparing per-instance query memory and
/// `active_instances * insert_cost` is sufficient.  Query latency and retained
/// memory are checked for each RQE the dominated candidate can serve.
pub fn prune_dominated_candidates(rqes: &[Rqe], candidates: Vec<Deployment>) -> Vec<Deployment> {
    let eligibility: Vec<Vec<bool>> = candidates
        .iter()
        .map(|candidate| rqes.iter().map(|rqe| is_eligible(rqe, candidate)).collect())
        .collect();

    candidates
        .iter()
        .enumerate()
        .filter_map(|(candidate_index, candidate)| {
            let dominated = eligibility
                .iter()
                .enumerate()
                .any(|(other_index, other_coverage)| {
                    other_index != candidate_index
                    && candidate_dominates(
                        &candidates[other_index],
                        other_coverage,
                        candidate,
                        &eligibility[candidate_index],
                        rqes,
                    )
                    // Identical candidates dominate only in a stable direction,
                    // so a tie never removes both candidates.
                    && (strictly_better(
                        &candidates[other_index],
                        other_coverage,
                        candidate,
                        &eligibility[candidate_index],
                        rqes,
                    ) || other_index < candidate_index)
                });
            (!dominated).then(|| candidate.clone())
        })
        .collect()
}

fn candidate_dominates(
    replacement: &Deployment,
    replacement_coverage: &[bool],
    original: &Deployment,
    original_coverage: &[bool],
    rqes: &[Rqe],
) -> bool {
    if replacement.capability != original.capability || replacement.labels != original.labels {
        return false;
    }

    let replacement_ingest = replacement.active_instance_count().unwrap_or(u64::MAX) as f64
        * replacement.config.insert_cpu_secs;
    let original_ingest = original.active_instance_count().unwrap_or(u64::MAX) as f64
        * original.config.insert_cpu_secs;
    replacement.config.mem_bytes_per_instance <= original.config.mem_bytes_per_instance
        && replacement_ingest <= original_ingest
        && original_coverage
            .iter()
            .zip(replacement_coverage)
            .all(|(&original_serves, &replacement_serves)| !original_serves || replacement_serves)
        && original_coverage
            .iter()
            .enumerate()
            .all(|(rqe_index, &serves)| {
                !serves
                    || (query_latency(replacement, &rqes[rqe_index])
                        <= query_latency(original, &rqes[rqe_index])
                        && retained_memory(replacement, &rqes[rqe_index])
                            <= retained_memory(original, &rqes[rqe_index]))
            })
}

fn strictly_better(
    replacement: &Deployment,
    replacement_coverage: &[bool],
    original: &Deployment,
    original_coverage: &[bool],
    rqes: &[Rqe],
) -> bool {
    let replacement_ingest = replacement.active_instance_count().unwrap_or(u64::MAX) as f64
        * replacement.config.insert_cpu_secs;
    let original_ingest = original.active_instance_count().unwrap_or(u64::MAX) as f64
        * original.config.insert_cpu_secs;
    replacement.config.mem_bytes_per_instance < original.config.mem_bytes_per_instance
        || replacement_ingest < original_ingest
        || original_coverage
            .iter()
            .zip(replacement_coverage)
            .any(|(&original_serves, &replacement_serves)| !original_serves && replacement_serves)
        || original_coverage
            .iter()
            .enumerate()
            .any(|(rqe_index, &serves)| {
                serves
                    && (query_latency(replacement, &rqes[rqe_index])
                        < query_latency(original, &rqes[rqe_index])
                        || retained_memory(replacement, &rqes[rqe_index])
                            < retained_memory(original, &rqes[rqe_index]))
            })
}

fn query_latency(deployment: &Deployment, rqe: &Rqe) -> f64 {
    let instances = deployment
        .query_instance_count(rqe.lookback_secs)
        .expect("candidate coverage only contains exactly tiled RQEs");
    deployment.config.query_cpu_secs
        + instances.saturating_sub(1) as f64 * deployment.config.merge_cpu_secs
}

/// Retained bytes per label group when serving `rqe`. Label cardinality is a
/// common multiplier within a group, so it is left out.
fn retained_memory(deployment: &Deployment, rqe: &Rqe) -> f64 {
    deployment
        .retained_instance_count(rqe.lookback_secs)
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
        && r.labels == d.labels
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
    use crate::{AccuracyDirection, LabelSet};
    use std::collections::BTreeMap;
    fn rqe(id: &str, lookback: Seconds, interval: Seconds) -> Rqe {
        Rqe {
            id: id.into(),
            capability: Capability::TopK,
            lookback_secs: lookback,
            interval_secs: interval,
            labels: LabelSet::new(),
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
        let candidates = build_all_candidates(&rqes, &[cost()]);
        let slides: BTreeSet<_> = candidates
            .iter()
            .filter(|d| d.window_secs == 60)
            .map(|d| d.slide_secs)
            .collect();
        assert_eq!(slides, BTreeSet::from([10, 20, 30]));
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
        let candidates = build_all_candidates(&[r], &[named("exact-min"), named("exact-max")]);
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|d| d.config.sketch == "exact-min"));
    }

    #[test]
    fn eligibility_requires_exact_non_overlapping_tiling() {
        let r = rqe("r", 600, 180);
        let d = Deployment {
            capability: Capability::TopK,
            labels: LabelSet::new(),
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
            labels: LabelSet::new(),
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
            labels: LabelSet::new(),
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
    fn retains_candidate_with_lower_retained_memory() {
        let r = rqe("r", 60, 60);
        // Holds (60 + 60) / 60 = 2 instances of 10 bytes.
        let whole_window = Deployment {
            capability: Capability::TopK,
            labels: LabelSet::new(),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 10.0,
                ..cost()
            },
            window_secs: 60,
            slide_secs: 60,
        };
        // Smaller, faster sketches, but (20 + 60) / 20 = 4 of them: 24 bytes.
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
}
