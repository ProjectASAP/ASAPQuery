//! Brute-force search over full mappings (§4, §7 step 2).
//!
//! A mapping is one deployment choice per RQE (§4's only constraint is
//! `Σ_D x_{i,D} = 1`; `y_D` is derived). No RQE is eligible outside its own
//! `(capability, labels)` group, so choices are independent and the search
//! space is exactly the cartesian product of the per-RQE eligible lists --
//! nothing to prune against. Sharing falls out when two choices land on the
//! same index.
//!
//! An ILP would replace this module alone: `Mapping` already encodes
//! `x_{i,D}` and `y_D`.

use crate::{candidates::eligible_deployments_for, Deployment, Mapping, Rqe};

/// RQEs nothing in `deployments` can serve (§3.4). Surfaced separately from
/// `brute_force`: an unservable RQE is a modeling gap (tolerance too tight,
/// no such config), not a normal empty result.
pub fn unservable(rqes: &[Rqe], deployments: &[Deployment]) -> Vec<String> {
    rqes.iter()
        .filter(|r| eligible_deployments_for(r, deployments).is_empty())
        .map(|r| r.id.clone())
        .collect()
}

/// Every feasible full mapping. Empty if any RQE is unservable -- check
/// [`unservable`] to tell the two cases apart.
pub fn brute_force(rqes: &[Rqe], deployments: &[Deployment]) -> Vec<Mapping> {
    let mut mappings = Vec::new();
    for_each_mapping(rqes, deployments, |mapping| mappings.push(mapping.to_vec()));
    mappings
}

/// Visit each feasible mapping without retaining the complete Cartesian
/// product. The visitor receives a borrowed, index-aligned mapping that is
/// valid only for the duration of the call. Returns the number visited.
pub fn for_each_mapping(
    rqes: &[Rqe],
    deployments: &[Deployment],
    mut visit: impl FnMut(&Mapping),
) -> u64 {
    for_each_mapping_while(rqes, deployments, |mapping| {
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
    rqes: &[Rqe],
    deployments: &[Deployment],
    mut visit: impl FnMut(&Mapping) -> bool,
) -> EnumerationResult {
    let choices: Vec<Vec<usize>> = rqes
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
        &mut Vec::with_capacity(rqes.len()),
        &mut visit,
        &mut visited,
    );
    EnumerationResult { visited, completed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidates::build_all_candidates;
    use crate::{AtomicCostEntry, Capability, LabelSet};
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

    fn rqe(id: &str, interval: u64, lookback: u64) -> Rqe {
        Rqe {
            id: id.to_string(),
            capability: Capability::TopK,
            lookback_secs: lookback,
            interval_secs: interval,
            labels: LabelSet::new(),
            accuracy_metric: "err".to_string(),
            accuracy_tolerance: 1.0,
            accuracy_direction: crate::AccuracyDirection::LowerIsBetter,
        }
    }

    #[test]
    fn two_rqes_can_share_one_deployment() {
        let rqes = vec![rqe("a", 60, 3_600), rqe("b", 60, 3_600)];
        let table = vec![cost("cms-heap-topk-fastpath-vector2d")];
        let deployments = build_all_candidates(&rqes, &table);

        assert!(unservable(&rqes, &deployments).is_empty());
        let mappings = brute_force(&rqes, &deployments);
        assert!(
            mappings.iter().any(|m| m[0] == m[1]),
            "expected at least one mapping where both identical RQEs land on the same deployment"
        );
    }

    #[test]
    fn unservable_rqe_yields_no_mappings() {
        let rqes = vec![rqe("a", 60, 3_600)];
        let table: Vec<AtomicCostEntry> = Vec::new(); // no configs at all
        let deployments = build_all_candidates(&rqes, &table);

        assert_eq!(unservable(&rqes, &deployments), vec!["a".to_string()]);
        assert!(brute_force(&rqes, &deployments).is_empty());
    }

    #[test]
    fn streaming_visits_the_same_mappings_as_eager_enumeration() {
        let rqes = vec![rqe("a", 60, 3_600), rqe("b", 60, 3_600)];
        let deployments = build_all_candidates(&rqes, &[cost("cms-heap-topk-fastpath-vector2d")]);
        let eager = brute_force(&rqes, &deployments);
        let mut streamed = Vec::new();
        let count = for_each_mapping(&rqes, &deployments, |mapping| {
            streamed.push(mapping.clone())
        });
        assert_eq!(count as usize, eager.len());
        assert_eq!(streamed, eager);
    }

    #[test]
    fn streaming_can_stop_after_a_fixed_number_of_mappings() {
        let rqes = vec![rqe("a", 60, 3_600), rqe("b", 60, 3_600)];
        let deployments = build_all_candidates(&rqes, &[cost("cms-heap-topk-fastpath-vector2d")]);
        let mut sampled = Vec::new();
        let result = for_each_mapping_while(&rqes, &deployments, |mapping| {
            sampled.push(mapping.clone());
            sampled.len() < 2
        });

        assert_eq!(result.visited, 2);
        assert!(!result.completed);
        assert_eq!(sampled.len(), 2);
    }
}
