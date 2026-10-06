//! Brute-force search over full mappings (§4, §7 step 2).
//!
//! A mapping is one deployment choice per RAQE (§4's only constraint is
//! `Σ_D x_{i,D} = 1`; `y_D` is derived). No RAQE is eligible outside its own
//! `(capability, metric, spatial_filter, grouping_labels)` group, so choices are independent and the search
//! space is exactly the cartesian product of the per-RAQE eligible lists --
//! nothing to prune against. Sharing falls out when two choices land on the
//! same index.
//!
//! An ILP would replace this module alone: `Mapping` already encodes
//! `x_{i,D}` and `y_D`.

use crate::{candidates::eligible_deployments_for, Deployment, Mapping, Raqe};

/// RAQEs nothing in `deployments` can serve (§3.4). Surfaced separately from
/// `brute_force`: an unservable RAQE is a modeling gap (tolerance too tight,
/// no such config), not a normal empty result.
pub fn unservable(raqes: &[Raqe], deployments: &[Deployment]) -> Vec<String> {
    raqes
        .iter()
        .filter(|r| eligible_deployments_for(r, deployments).is_empty())
        .map(|r| r.id.clone())
        .collect()
}

/// Every feasible full mapping. Empty if any RAQE is unservable -- check
/// [`unservable`] to tell the two cases apart.
pub fn brute_force(raqes: &[Raqe], deployments: &[Deployment]) -> Vec<Mapping> {
    let mut mappings = Vec::new();
    for_each_mapping(raqes, deployments, |mapping| {
        mappings.push(mapping.to_vec())
    });
    mappings
}

/// Visit each feasible mapping without retaining the complete Cartesian
/// product. The visitor receives a borrowed, index-aligned mapping that is
/// valid only for the duration of the call. Returns the number visited.
pub fn for_each_mapping(
    raqes: &[Raqe],
    deployments: &[Deployment],
    mut visit: impl FnMut(&Mapping),
) -> u64 {
    for_each_mapping_while(raqes, deployments, |mapping| {
        visit(mapping);
        true
    })
    .visited
}

/// Result of a streaming enumeration. `completed` is false when the visitor
/// stopped enumeration early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnumerationResult {
    pub visited: u64,
    pub completed: bool,
}

/// Visit feasible mappings until the visitor returns false. This makes sample
/// and bounded inspection modes safe: they never need to enumerate the rest
/// of the Cartesian product after collecting enough examples.
pub fn for_each_mapping_while(
    raqes: &[Raqe],
    deployments: &[Deployment],
    mut visit: impl FnMut(&Mapping) -> bool,
) -> EnumerationResult {
    let choices: Vec<Vec<usize>> = raqes
        .iter()
        .map(|r| eligible_deployments_for(r, deployments))
        .collect();

    if choices.iter().any(|c| c.is_empty()) {
        return EnumerationResult {
            visited: 0,
            completed: true,
        };
    }

    fn visit_depth_first(
        choices: &[Vec<usize>],
        mapping: &mut Mapping,
        visit: &mut impl FnMut(&Mapping) -> bool,
        visited: &mut u64,
    ) -> bool {
        if mapping.len() == choices.len() {
            *visited += 1;
            return visit(mapping);
        }
        for &deployment in &choices[mapping.len()] {
            mapping.push(deployment);
            let should_continue = visit_depth_first(choices, mapping, visit, visited);
            mapping.pop();
            if !should_continue {
                return false;
            }
        }
        true
    }

    let mut visited = 0;
    let completed = visit_depth_first(
        &choices,
        &mut Vec::with_capacity(raqes.len()),
        &mut visit,
        &mut visited,
    );
    EnumerationResult { visited, completed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidates;
    use crate::test_support::{facts, METRIC};
    use crate::{AtomicCostEntry, Capability, Deployment, LabelSet};
    use std::collections::BTreeMap;

    fn cost(sketch: &str) -> AtomicCostEntry {
        let mut query_accuracy = BTreeMap::new();
        query_accuracy.insert("err".to_string(), 0.01);
        AtomicCostEntry {
            sketch: sketch.to_string(),
            sketch_config: serde_json::json!(null),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 1.0,
            merge_cpu_secs: 1.0,
            query_cpu_secs: 1.0,
            query_accuracy,
        }
    }

    fn build_all_candidates(raqes: &[Raqe], costs: &[AtomicCostEntry]) -> Vec<Deployment> {
        candidates::build_all_candidates(raqes, costs, &facts(1, 1), false)
    }

    fn raqe(id: &str, interval: u64, lookback: u64) -> Raqe {
        Raqe {
            id: id.to_string(),
            capability: Capability::TopK,
            lookback_ms: lookback,
            interval_ms: interval,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: "err".to_string(),
            accuracy_sla: 1.0,
            accuracy_direction: crate::AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        }
    }

    #[test]
    fn two_raqes_can_share_one_deployment() {
        let raqes = vec![raqe("a", 60_000, 3_600_000), raqe("b", 60_000, 3_600_000)];
        let table = vec![cost("cms-heap-topk-fastpath-vector2d")];
        let deployments = build_all_candidates(&raqes, &table);

        assert!(unservable(&raqes, &deployments).is_empty());
        let mappings = brute_force(&raqes, &deployments);
        assert!(
            mappings.iter().any(|m| m[0] == m[1]),
            "expected at least one mapping where both identical RAQEs land on the same deployment"
        );
    }

    #[test]
    fn unservable_raqe_yields_no_mappings() {
        let raqes = vec![raqe("a", 60_000, 3_600_000)];
        let table: Vec<AtomicCostEntry> = Vec::new(); // no configs at all
        let deployments = build_all_candidates(&raqes, &table);

        assert_eq!(unservable(&raqes, &deployments), vec!["a".to_string()]);
        assert!(brute_force(&raqes, &deployments).is_empty());
    }

    #[test]
    fn streaming_visits_the_same_mappings_as_eager_enumeration() {
        let raqes = vec![raqe("a", 60_000, 3_600_000), raqe("b", 60_000, 3_600_000)];
        let deployments = build_all_candidates(&raqes, &[cost("cms-heap-topk-fastpath-vector2d")]);
        let eager = brute_force(&raqes, &deployments);
        let mut streamed = Vec::new();
        let count = for_each_mapping(&raqes, &deployments, |mapping| {
            streamed.push(mapping.clone())
        });
        assert_eq!(count as usize, eager.len());
        assert_eq!(streamed, eager);
    }

    #[test]
    fn streaming_can_stop_after_a_fixed_number_of_mappings() {
        let raqes = vec![raqe("a", 60_000, 3_600_000), raqe("b", 60_000, 3_600_000)];
        let deployments = build_all_candidates(&raqes, &[cost("cms-heap-topk-fastpath-vector2d")]);
        let mut sampled = Vec::new();
        let result = for_each_mapping_while(&raqes, &deployments, |mapping| {
            sampled.push(mapping.clone());
            sampled.len() < 2
        });

        assert_eq!(result.visited, 2);
        assert!(!result.completed);
        assert_eq!(sampled.len(), 2);
    }
}
