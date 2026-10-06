//! MILP solver for one scalarized RQE deployment optimization (§4).
//!
//! The model is documented in `docs/rqe_sketch_deployment_v1.md`. It avoids
//! enumerating the Cartesian product of eligible deployment choices.

use crate::analytical_cost_model::{self, score, PhaseCost, PlanCost, BYTES_PER_GIB};
use crate::candidates::eligible_deployments_for;
use crate::{Deployment, Mapping, Rqe, WorkloadFacts};
use good_lp::{
    default_solver, variable, Expression, ProblemVariables, ResolutionError, Solution, SolverModel,
    Variable,
};

/// What a [`minimize`] solve minimizes. Eligibility, assignment and activation
/// rows, and bounds are the same for every objective.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Objective {
    /// `w_cpu · CPU + w_mem · memory`. CPU is mean CPU-sec/sec, the area under
    /// the load curve, so bursts are not priced. Memory is GiB summed over
    /// every phase, as if every query evaluates at once.
    AUCCost { w_cpu: f64, w_mem: f64 },
}

impl Default for Objective {
    /// CPU only, until memory gets a price.
    fn default() -> Self {
        Objective::AUCCost {
            w_cpu: 1.0,
            w_mem: 0.0,
        }
    }
}

impl Objective {
    pub fn value(&self, plan_cost: &PlanCost) -> f64 {
        self.weigh(PhaseCost {
            cpu_secs_per_sec: plan_cost.cpu_secs_per_sec(),
            memory_bytes: plan_cost.memory_bytes(),
        })
    }

    fn weigh(&self, cost: PhaseCost) -> f64 {
        let Objective::AUCCost { w_cpu, w_mem } = *self;
        w_cpu * cost.cpu_secs_per_sec + w_mem * cost.memory_bytes / BYTES_PER_GIB
    }
}

/// Optional hard bounds for a solve. An empty latency vector means no RQE has
/// a latency bound; otherwise it is index-aligned with `rqes`.
#[derive(Debug, Clone, Default)]
pub struct MilpBounds {
    pub max_query_latency_secs: Vec<Option<f64>>,
}

#[derive(Debug, Clone)]
pub struct MilpSolution {
    pub mapping: Mapping,
    pub plan_cost: PlanCost,
}

/// Minimize `objective` subject to optional latency bounds. `facts` must pass
/// [`crate::validate_facts`]. Missing eligible deployments are a caller input
/// error; use `enumerate::unservable` to report them before calling this
/// function.
pub fn minimize(
    rqes: &[Rqe],
    deployments: &[Deployment],
    facts: &WorkloadFacts,
    bounds: &MilpBounds,
    objective: Objective,
) -> Result<MilpSolution, ResolutionError> {
    assert!(
        bounds.max_query_latency_secs.is_empty()
            || bounds.max_query_latency_secs.len() == rqes.len(),
        "latency bounds must be empty or index-aligned with rqes"
    );

    let eligible: Vec<Vec<usize>> = rqes
        .iter()
        .map(|rqe| eligible_deployments_for(rqe, deployments))
        .collect();
    assert!(
        eligible.iter().all(|choices| !choices.is_empty()),
        "cannot solve a workload with an unservable RQE"
    );

    let mut variables = ProblemVariables::new();
    let active: Vec<Variable> = deployments
        .iter()
        .map(|_| variables.add(variable().binary()))
        .collect();
    let assignments: Vec<Vec<(usize, Variable)>> = eligible
        .iter()
        .map(|choices| {
            choices
                .iter()
                .map(|&deployment_index| (deployment_index, variables.add(variable().binary())))
                .collect()
        })
        .collect();
    // Weighted storage cost per deployment: the max over the RQEs it serves.
    let deployment_storage_cost: Vec<Variable> = deployments
        .iter()
        .map(|_| variables.add(variable().min(0)))
        .collect();

    // Ingest is paid once per active deployment, merge and query once per
    // assignment, and storage once per deployment for its longest lookback.
    let ingest_cost: Vec<f64> = deployments
        .iter()
        .map(|deployment| objective.weigh(analytical_cost_model::ingest(deployment, facts)))
        .collect();
    let merge_and_query_cost = |rqe: &Rqe, deployment: &Deployment| {
        objective.weigh(analytical_cost_model::merge(rqe, deployment, facts))
            + objective.weigh(analytical_cost_model::query(rqe, deployment, facts))
    };
    let storage_cost_for_rqe_and_deployment = |rqe: &Rqe, deployment: &Deployment| {
        objective.weigh(PhaseCost {
            cpu_secs_per_sec: 0.0,
            memory_bytes: analytical_cost_model::storage_bytes(rqe, deployment, facts),
        })
    };

    // Real plans cost ~1e-6 CPU-sec/sec and read in microseconds, below
    // HiGHS's absolute gap (1e-6) and feasibility tolerance (1e-7). So costs
    // are divided by `reference`, a plan's cost without sharing (each RQE on
    // its cheapest deployment), and per-RQE bounds become exclusions. The
    // result is re-scored on real costs.
    let reference: f64 = rqes
        .iter()
        .zip(&eligible)
        .map(|(rqe, choices)| {
            choices
                .iter()
                .map(|&deployment_index| {
                    let deployment = &deployments[deployment_index];
                    ingest_cost[deployment_index]
                        + merge_and_query_cost(rqe, deployment)
                        + storage_cost_for_rqe_and_deployment(rqe, deployment)
                })
                .fold(f64::INFINITY, f64::min)
        })
        .sum();
    let reference = if reference.is_normal() {
        reference
    } else {
        1.0
    };

    let mut goal: Expression = deployment_storage_cost.iter().sum();
    for (&cost, &active_variable) in ingest_cost.iter().zip(&active) {
        goal.add_mul(cost / reference, active_variable);
    }
    for (rqe_index, choices) in assignments.iter().enumerate() {
        for &(deployment_index, assignment) in choices {
            goal.add_mul(
                merge_and_query_cost(&rqes[rqe_index], &deployments[deployment_index]) / reference,
                assignment,
            );
        }
    }

    let mut model = variables.minimise(goal).using(default_solver);
    for (rqe_index, choices) in assignments.iter().enumerate() {
        let assignment_sum: Expression = choices.iter().map(|(_, variable)| *variable).sum();
        model.add_constraint(assignment_sum.eq(1));

        for &(deployment_index, assignment) in choices {
            let rqe = &rqes[rqe_index];
            let deployment = &deployments[deployment_index];
            model.add_constraint((assignment - active[deployment_index]).leq(0));
            // Zero when `w_mem` is 0; such a row can never bind.
            let scaled_storage_cost =
                storage_cost_for_rqe_and_deployment(rqe, deployment) / reference;
            if scaled_storage_cost > 0.0 {
                model.add_constraint(
                    (scaled_storage_cost * assignment - deployment_storage_cost[deployment_index])
                        .leq(0),
                );
            }
            // Each RQE takes exactly one deployment, so a per-RQE bound just
            // forbids the choices over it. Comparing in f64 here, not in the
            // solver, keeps µs latencies clear of its feasibility tolerance.
            let latency = analytical_cost_model::query_latency_secs(rqe, deployment, facts);
            if bounds
                .max_query_latency_secs
                .get(rqe_index)
                .copied()
                .flatten()
                .is_some_and(|limit| latency > limit)
            {
                model.add_constraint(Expression::from(assignment).leq(0));
            }
        }
    }

    for (deployment_index, active_variable) in active.iter().enumerate() {
        let assignments_using_deployment: Expression = assignments
            .iter()
            .flat_map(|choices| choices.iter())
            .filter(|(index, _)| *index == deployment_index)
            .map(|(_, assignment)| *assignment)
            .sum();
        model.add_constraint((*active_variable - assignments_using_deployment).leq(0));
    }

    let solved = model.solve()?;
    let mapping = assignments
        .iter()
        .map(|choices| {
            choices
                .iter()
                .find_map(|&(deployment_index, assignment)| {
                    (solved.value(assignment) > 0.5).then_some(deployment_index)
                })
                .expect("MILP assigns exactly one deployment to every RQE")
        })
        .collect();
    let plan_cost = score(rqes, deployments, &mapping, facts);
    Ok(MilpSolution { mapping, plan_cost })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enumerate::brute_force;
    use crate::test_support::{self, facts};

    /// Window and slide 60 s, merge 1 CPU-sec.
    fn deployment(insert_cpu_secs: f64, memory: f64, query_cpu_secs: f64) -> Deployment {
        test_support::deployment(memory, insert_cpu_secs, 1.0, query_cpu_secs, 60, 60)
    }

    fn rqe(id: &str, lookback_secs: u64) -> Rqe {
        Rqe {
            id: id.into(),
            ..test_support::rqe(lookback_secs, 60)
        }
    }

    fn weights(w_cpu: f64, w_mem: f64) -> Objective {
        Objective::AUCCost { w_cpu, w_mem }
    }

    /// The MILP's plan scores as well as the best of every mapping.
    fn assert_matches_brute_force(rqes: &[Rqe], deployments: &[Deployment], objective: Objective) {
        let facts = facts(1, 1);
        let best = brute_force(rqes, deployments)
            .iter()
            .map(|mapping| objective.value(&score(rqes, deployments, mapping, &facts)))
            .min_by(f64::total_cmp)
            .expect("test workload is servable");
        let milp = minimize(rqes, deployments, &facts, &MilpBounds::default(), objective)
            .expect("feasible MILP");
        let got = objective.value(&milp.plan_cost);
        assert!(
            (got - best).abs() <= 1e-9 * best,
            "{objective:?}: {got} vs {best}"
        );
    }

    #[test]
    fn minimizes_cpu_and_respects_a_latency_bound() {
        let rqes = vec![rqe("r", 60)];
        // CPU 1 + 3/60 with latency 3, or 2 + 1/60 with latency 1.
        let deployments = vec![deployment(1.0, 20.0, 3.0), deployment(2.0, 10.0, 1.0)];
        let solve = |bounds: &MilpBounds| {
            minimize(
                &rqes,
                &deployments,
                &facts(1, 1),
                bounds,
                Objective::default(),
            )
            .expect("feasible MILP")
            .mapping
        };

        assert_eq!(solve(&MilpBounds::default()), vec![0]);
        let bounded = MilpBounds {
            max_query_latency_secs: vec![Some(2.0)],
        };
        assert_eq!(solve(&bounded), vec![1]);
    }

    #[test]
    fn matches_brute_force_with_shared_deployment_costs() {
        let rqes = vec![rqe("frequent", 60), rqe("long", 600)];
        let deployments = vec![
            deployment(1.0, 0.5 * BYTES_PER_GIB, 400.0),
            deployment(3.0, 0.1 * BYTES_PER_GIB, 1.0),
            deployment(0.2, 2.0 * BYTES_PER_GIB, 1.0),
        ];
        for objective in [weights(1.0, 0.0), weights(0.0, 1.0), weights(1.0, 4.0)] {
            assert_matches_brute_force(&rqes, &deployments, objective);
        }
    }

    #[test]
    fn memory_weight_trades_cpu_for_memory() {
        let rqes = vec![rqe("r", 60)];
        // CPU-heavy and small, or CPU-light and twice the size.
        let deployments = vec![
            deployment(1.0, 1.0 * BYTES_PER_GIB, 0.0),
            deployment(0.05, 2.0 * BYTES_PER_GIB, 0.0),
        ];
        let solve = |objective| {
            minimize(
                &rqes,
                &deployments,
                &facts(1, 1),
                &MilpBounds::default(),
                objective,
            )
            .expect("feasible MILP")
            .mapping
        };

        assert_eq!(solve(weights(1.0, 0.0)), vec![1]);
        assert_eq!(solve(weights(0.0, 1.0)), vec![0]);
    }

    #[test]
    fn tiny_magnitudes_match_brute_force_and_respect_latency_bounds() {
        // Real plans cost ~1e-6 CPU-sec/sec with µs latencies: below HiGHS's
        // absolute gap and feasibility tolerances unless the model is scaled.
        let rqes = vec![rqe("frequent", 60), rqe("long", 600)];
        let tiny = 1e-9;
        let deployments = vec![
            deployment(1.0 * tiny, 0.5 * tiny * BYTES_PER_GIB, 400.0 * tiny),
            deployment(3.0 * tiny, 0.1 * tiny * BYTES_PER_GIB, 1.0 * tiny),
            deployment(0.2 * tiny, 2.0 * tiny * BYTES_PER_GIB, 1.0 * tiny),
        ]
        .into_iter()
        .map(|mut candidate| {
            candidate.config.merge_cpu_secs = tiny;
            candidate
        })
        .collect::<Vec<_>>();
        for objective in [weights(1.0, 0.0), weights(1.0, 4.0)] {
            assert_matches_brute_force(&rqes, &deployments, objective);
        }

        // The cheap deployment's latency is 1.5e-7 s, over a 1e-7 s bound by
        // less than HiGHS's absolute feasibility tolerance.
        let rqes = vec![rqe("r", 60)];
        let deployments = vec![
            deployment(tiny, tiny, 1.5e-7),
            deployment(10.0 * tiny, tiny, 0.5e-7),
        ];
        let milp = minimize(
            &rqes,
            &deployments,
            &facts(1, 1),
            &MilpBounds {
                max_query_latency_secs: vec![Some(1e-7)],
            },
            Objective::default(),
        )
        .expect("feasible MILP");
        assert_eq!(milp.mapping, vec![1]);
        assert!(milp.plan_cost.query_latency_secs[0] <= 1e-7);
    }
}
