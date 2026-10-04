//! Non-dominated filtering for `(peak memory, TCO CPU, per-RQE latency)`.

use crate::{objectives::Objectives, Mapping};

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

fn objective_vector(objectives: &Objectives) -> Vec<f64> {
    let mut vector = vec![
        objectives.peak_query_memory_bytes,
        objectives.tco_cpu_secs_per_sec,
    ];
    vector.extend(&objectives.query_latency_secs);
    vector
}

/// A mapping retained on the current incremental Pareto frontier.
#[derive(Debug, Clone)]
pub struct ParetoEntry {
    pub mapping: Mapping,
    pub objectives: Objectives,
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
    pub fn consider(&mut self, mapping: &Mapping, objectives: Objectives) -> bool {
        let candidate = objective_vector(&objectives);
        if self
            .entries
            .iter()
            .any(|entry| dominates(&objective_vector(&entry.objectives), &candidate))
        {
            return false;
        }
        self.entries
            .retain(|entry| !dominates(&candidate, &objective_vector(&entry.objectives)));
        self.entries.push(ParetoEntry {
            mapping: mapping.clone(),
            objectives,
        });
        true
    }

    pub fn entries(&self) -> &[ParetoEntry] {
        &self.entries
    }
}

pub fn pareto_front(objs: &[Objectives]) -> Vec<usize> {
    let vectors: Vec<Vec<f64>> = objs.iter().map(objective_vector).collect();
    (0..objs.len())
        .filter(|&i| !(0..objs.len()).any(|j| j != i && dominates(&vectors[j], &vectors[i])))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn objectives(memory: f64, cpu: f64, latency: f64) -> Objectives {
        Objectives {
            peak_query_memory_bytes: memory,
            ingest_cpu_secs_per_sec: 0.0,
            merge_cpu_secs_per_sec: 0.0,
            query_cpu_secs_per_sec: 0.0,
            tco_cpu_secs_per_sec: cpu,
            query_latency_secs: vec![latency],
        }
    }

    #[test]
    fn incremental_front_discards_dominated_mappings() {
        let mut front = ParetoFront::new();
        assert!(front.consider(&vec![0], objectives(10.0, 10.0, 10.0)));
        assert!(!front.consider(&vec![1], objectives(20.0, 20.0, 20.0)));
        assert!(front.consider(&vec![2], objectives(5.0, 5.0, 5.0)));
        assert_eq!(front.entries().len(), 1);
        assert_eq!(front.entries()[0].mapping, vec![2]);
    }
}
