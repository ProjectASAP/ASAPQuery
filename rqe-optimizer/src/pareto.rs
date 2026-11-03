//! Non-dominated filtering for `(CPU, memory, per-RAQE latency)`.

use crate::{analytical_cost_model::PlanCost, Mapping};

fn dominates(a: &[f64], b: &[f64]) -> bool {
    let mut strict = false;
    for (left, right) in a.iter().zip(b) {
        if left > right {
            return false;
        }
        if left < right {
            strict = true;
        }
    }
    strict
}

fn pareto_vector(plan_cost: &PlanCost) -> Vec<f64> {
    let mut vector = vec![plan_cost.cpu_secs_per_sec(), plan_cost.memory_bytes()];
    vector.extend(&plan_cost.query_latency_ms);
    vector
}

/// A mapping retained on the current incremental Pareto frontier.
#[derive(Debug, Clone)]
pub struct ParetoEntry {
    pub mapping: Mapping,
    pub plan_cost: PlanCost,
}

/// Retains only non-dominated mappings while a streaming enumerator visits
/// them. Peak memory is proportional to the frontier, not all mappings.
#[derive(Debug, Default)]
pub struct ParetoFront {
    entries: Vec<ParetoEntry>,
}

impl ParetoFront {
    pub fn new() -> Self {
        Self::default()
    }

    /// Consider one scored mapping. Returns true when it remains on the
    /// frontier. Equal objective vectors are both retained.
    pub fn consider(&mut self, mapping: &Mapping, plan_cost: PlanCost) -> bool {
        let candidate = pareto_vector(&plan_cost);
        if self
            .entries
            .iter()
            .any(|entry| dominates(&pareto_vector(&entry.plan_cost), &candidate))
        {
            return false;
        }
        self.entries
            .retain(|entry| !dominates(&candidate, &pareto_vector(&entry.plan_cost)));
        self.entries.push(ParetoEntry {
            mapping: mapping.clone(),
            plan_cost,
        });
        true
    }

    pub fn entries(&self) -> &[ParetoEntry] {
        &self.entries
    }
}

pub fn pareto_front(plan_costs: &[PlanCost]) -> Vec<usize> {
    let vectors: Vec<Vec<f64>> = plan_costs.iter().map(pareto_vector).collect();
    (0..plan_costs.len())
        .filter(|&i| !(0..plan_costs.len()).any(|j| j != i && dominates(&vectors[j], &vectors[i])))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytical_cost_model::PhaseCost;

    fn plan_cost(memory: f64, cpu: f64, latency: f64) -> PlanCost {
        let phase = PhaseCost {
            cpu_secs_per_sec: cpu,
            memory_bytes: memory,
        };
        PlanCost {
            ingest: phase,
            merge: PhaseCost::default(),
            query: PhaseCost::default(),
            storage: PhaseCost::default(),
            query_latency_ms: vec![latency],
        }
    }

    #[test]
    fn incremental_front_discards_dominated_mappings() {
        let mut front = ParetoFront::new();
        assert!(front.consider(&vec![0], plan_cost(10.0, 10.0, 10.0)));
        assert!(!front.consider(&vec![1], plan_cost(20.0, 20.0, 20.0)));
        assert!(front.consider(&vec![2], plan_cost(5.0, 5.0, 5.0)));
        assert_eq!(front.entries().len(), 1);
        assert_eq!(front.entries()[0].mapping, vec![2]);
    }
}
