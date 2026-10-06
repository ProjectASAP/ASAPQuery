//! AutoSketch-Adapted: the per-query configuration search of AutoSketch
//! (Sun et al., NSDI '24, Algorithm 4), used as a baseline against the
//! batch MILP. Ported from ASAPQuery-backend
//! `data_plane/examples/autosketch_comparison.rs` and generalized from its
//! fixed CMS width/depth grid to each variant's measured parameter axes.
//!
//! AutoSketch plans one query at a time, never shares state across queries,
//! and constrains accuracy only. So every RQE is searched independently,
//! gets its own deployment, and latency is not considered.
//!
//! One extension over the paper: AutoSketch's compiler maps an operator to one
//! sketch algorithm and tunes its parameters, while this search covers every
//! variant serving the RQE's capability and keeps the cheapest, so it chooses
//! from the same sketches as the MILP.

use crate::candidates::gcd;
use crate::{AtomicCostEntry, Deployment, Mapping, Rqe, Seconds};
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

/// One RQE's search. `selected` and `probes` index into the cost table.
#[derive(Debug, Clone)]
pub struct RqeSearch {
    pub rqe_id: String,
    /// Smallest feasible configuration found; `None` means unservable.
    pub selected: Option<usize>,
    /// Distinct configurations whose accuracy was evaluated, in order.
    pub probes: Vec<usize>,
    pub search_wall_secs: f64,
}

/// A whole-workload plan: one dedicated deployment per RQE, so identical
/// choices are still paid for once per RQE, and the identity mapping.
#[derive(Debug, Clone)]
pub struct AutoSketchPlan {
    pub deployments: Vec<Deployment>,
    pub mapping: Mapping,
    pub searches: Vec<RqeSearch>,
}

/// One sliding sketch per query window: `x = S`, `y = T`, or `gcd(S, T)`
/// when `T` does not divide `S`, so the window still tiles into slides.
pub fn window_adapter(rqe: &Rqe) -> (Seconds, Seconds) {
    (rqe.lookback_secs, gcd(rqe.lookback_secs, rqe.interval_secs))
}

/// Search every RQE independently. `accuracy` is the benchmark oracle: the
/// measured metric named by `rqe.accuracy_metric` for a configuration, or
/// `None` when it was not measured. Returns the IDs of unservable RQEs.
pub fn plan(
    rqes: &[Rqe],
    costs: &[AtomicCostEntry],
    seed: u64,
    mut accuracy: impl FnMut(&Rqe, &AtomicCostEntry) -> Option<f64>,
) -> Result<AutoSketchPlan, Vec<String>> {
    let searches: Vec<_> = rqes
        .iter()
        .map(|rqe| search(rqe, costs, seed, &mut accuracy))
        .collect();
    let unservable: Vec<_> = searches
        .iter()
        .filter(|s| s.selected.is_none())
        .map(|s| s.rqe_id.clone())
        .collect();
    if !unservable.is_empty() {
        return Err(unservable);
    }
    let deployments = rqes
        .iter()
        .zip(&searches)
        .map(|(rqe, s)| {
            let (window_secs, slide_secs) = window_adapter(rqe);
            Deployment {
                capability: rqe.capability,
                metric: rqe.metric.clone(),
                grouping_labels: rqe.grouping_labels.clone(),
                config: costs[s.selected.expect("unservable RQEs returned above")].clone(),
                window_secs,
                slide_secs,
            }
        })
        .collect();
    Ok(AutoSketchPlan {
        deployments,
        mapping: (0..rqes.len()).collect(),
        searches,
    })
}

/// Algorithm 4 for one RQE: LHS seeds per sketch variant, then
/// feasibility-directed neighbor search with the paper's pruning and
/// stopping rules. Minimizes memory per instance, then insert CPU.
pub fn search(
    rqe: &Rqe,
    costs: &[AtomicCostEntry],
    seed: u64,
    mut accuracy: impl FnMut(&Rqe, &AtomicCostEntry) -> Option<f64>,
) -> RqeSearch {
    let started = Instant::now();
    let grids: Vec<Grid> = rqe
        .capability
        .families()
        .iter()
        .filter_map(|variant| Grid::new(variant, costs))
        .collect();

    let mut pending = VecDeque::new();
    for grid in &grids {
        let seeds = grid.lhs(seed);
        if seeds.is_empty() {
            // A sparse table can leave no LHS point measured. Seed the
            // cheapest configuration so the variant is still searched.
            if let Some(&cheapest) = grid
                .points
                .values()
                .min_by(|&&a, &&b| compare_resources(&costs[a], &costs[b]))
            {
                pending.push_back((cheapest, None));
            }
        }
        pending.extend(seeds.into_iter().map(|index| (index, None)));
    }

    let mut evaluated: BTreeMap<usize, bool> = BTreeMap::new();
    let mut probes = Vec::new();
    let mut best: Option<usize> = None;
    let cheaper = |a: usize, b: usize| compare_resources(&costs[a], &costs[b]).is_lt();

    while let Some((index, initial_feasible)) = pending.pop_front() {
        // EXAMINE rule (1): a configuration is evaluated, and expanded, once.
        if evaluated.contains_key(&index) {
            continue;
        }
        probes.push(index);
        let feasible = accuracy(rqe, &costs[index])
            .is_some_and(|value| value.is_finite() && rqe.accuracy_ok(value));
        evaluated.insert(index, feasible);
        if feasible && best.is_none_or(|b| cheaper(index, b)) {
            best = Some(index);
        }
        // Stopping rule: a path that started feasible stops shrinking at the
        // first failure; one that started infeasible stops at the first success.
        if initial_feasible.unwrap_or(feasible) != feasible {
            continue;
        }
        let grid = grids
            .iter()
            .find(|g| g.variant == costs[index].sketch)
            .expect("every pending index comes from a grid");
        for neighbor in grid.neighbors(index) {
            // Shrink when feasible, grow when not.
            let toward = if feasible {
                cheaper(neighbor, index)
            } else {
                cheaper(index, neighbor)
            };
            // Prune anything no cheaper than a configuration already known to
            // satisfy the intent.
            let useful = best.is_none_or(|b| cheaper(neighbor, b));
            if toward && useful && !evaluated.contains_key(&neighbor) {
                pending.push_back((neighbor, Some(feasible)));
            }
        }
    }

    RqeSearch {
        rqe_id: rqe.id.clone(),
        selected: best,
        probes,
        search_wall_secs: started.elapsed().as_secs_f64(),
    }
}

/// AutoSketch's resource score with no ALUs: memory, with insert CPU only
/// breaking ties.
fn compare_resources(a: &AtomicCostEntry, b: &AtomicCostEntry) -> std::cmp::Ordering {
    a.mem_bytes_per_instance
        .total_cmp(&b.mem_bytes_per_instance)
        .then(a.insert_cpu_secs.total_cmp(&b.insert_cpu_secs))
}

/// One sketch variant's measured configurations, indexed by their numeric
/// parameter values (`sketch_config.params`).
struct Grid {
    variant: String,
    /// Sorted distinct measured values per parameter name.
    axes: BTreeMap<String, Vec<f64>>,
    /// Parameter vector (in `axes` order) -> first matching cost-table row.
    points: BTreeMap<Vec<u64>, usize>,
}

impl Grid {
    fn new(variant: &str, costs: &[AtomicCostEntry]) -> Option<Grid> {
        let rows: Vec<(usize, BTreeMap<String, f64>)> = costs
            .iter()
            .enumerate()
            .filter(|(_, c)| c.sketch == variant)
            .map(|(i, c)| (i, numeric_params(&c.sketch_config)))
            .collect();
        if rows.is_empty() {
            return None;
        }
        let mut axes: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        for (_, params) in &rows {
            for (name, &value) in params {
                axes.entry(name.clone()).or_default().push(value);
            }
        }
        for values in axes.values_mut() {
            values.sort_by(f64::total_cmp);
            values.dedup();
        }
        let mut points = BTreeMap::new();
        for (index, params) in rows {
            let key: Option<Vec<u64>> = axes
                .keys()
                .map(|name| params.get(name).map(|v| v.to_bits()))
                .collect();
            if let Some(key) = key {
                points.entry(key).or_insert(index);
            }
        }
        Some(Grid {
            variant: variant.to_string(),
            axes,
            points,
        })
    }

    fn key_of(&self, index: usize) -> &Vec<u64> {
        self.points
            .iter()
            .find(|(_, &i)| i == index)
            .map(|(key, _)| key)
            .expect("index belongs to this grid")
    }

    /// Latin hypercube seeds: each sample takes a distinct value on every
    /// axis. With several axes, the sample count is the shortest axis length
    /// (the paper's Figure 4); a single axis gets one random seed, since one
    /// sample per value would make the search exhaustive.
    fn lhs(&self, seed: u64) -> Vec<usize> {
        let mut rng = Rng(seed);
        let count = if self.axes.len() > 1 {
            self.axes.values().map(Vec::len).min().unwrap_or(0)
        } else {
            1
        };
        let columns: Vec<Vec<u64>> = self
            .axes
            .values()
            .map(|values| {
                let mut shuffled: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
                rng.shuffle(&mut shuffled);
                shuffled
            })
            .collect();
        (0..count)
            .filter_map(|sample| {
                let key: Vec<u64> = columns.iter().map(|column| column[sample]).collect();
                self.points.get(&key).copied()
            })
            .collect()
    }

    /// Measured configurations that move one parameter to its adjacent
    /// measured value.
    fn neighbors(&self, index: usize) -> Vec<usize> {
        let key = self.key_of(index);
        let mut out = Vec::new();
        for (axis, values) in self.axes.values().enumerate() {
            let position = values
                .iter()
                .position(|v| v.to_bits() == key[axis])
                .expect("key values come from the axes");
            let adjacent = [position.checked_sub(1), Some(position + 1)];
            for next in adjacent.into_iter().flatten().filter(|&p| p < values.len()) {
                let mut moved = key.clone();
                moved[axis] = values[next].to_bits();
                if let Some(&neighbor) = self.points.get(&moved) {
                    out.push(neighbor);
                }
            }
        }
        out
    }
}

fn numeric_params(sketch_config: &serde_json::Value) -> BTreeMap<String, f64> {
    sketch_config
        .get("params")
        .and_then(serde_json::Value::as_object)
        .map(|params| {
            params
                .iter()
                .filter_map(|(name, value)| value.as_f64().map(|v| (name.clone(), v)))
                .collect()
        })
        .unwrap_or_default()
}

/// SplitMix64, as in the backend port: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            values.swap(i, (self.next() % (i as u64 + 1)) as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytical_cost_model::{score, PhaseCost};
    use crate::candidates::is_eligible;
    use crate::test_support::{facts, METRIC};
    use crate::{AccuracyDirection, Capability, LabelSet};
    use std::collections::BTreeSet;

    const ERR: &str = "err";

    fn cms(rows: u64, cols: u64) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "cms-heap-topk-fastpath-vector2d".into(),
            sketch_config: serde_json::json!({
                "algorithm": "cms-heap-topk-fastpath-vector2d",
                "params": {"rows": rows, "cols": cols},
            }),
            mem_bytes_per_instance: (rows * cols * 4) as f64,
            insert_cpu_secs: 1e-8 * rows as f64,
            merge_cpu_secs: 1e-6,
            query_cpu_secs: 1e-6,
            // Error falls with width and depth, so feasibility is monotone.
            query_accuracy: BTreeMap::from([(ERR.into(), 1.0 / (rows * cols) as f64)]),
        }
    }

    fn grid(rows: &[u64], cols: &[u64]) -> Vec<AtomicCostEntry> {
        rows.iter()
            .flat_map(|&r| cols.iter().map(move |&c| cms(r, c)))
            .collect()
    }

    fn rqe(id: &str, lookback: Seconds, interval: Seconds, tolerance: f64) -> Rqe {
        Rqe {
            id: id.into(),
            capability: Capability::TopK,
            lookback_secs: lookback,
            interval_secs: interval,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: ERR.into(),
            accuracy_tolerance: tolerance,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
        }
    }

    fn table_accuracy(rqe: &Rqe, config: &AtomicCostEntry) -> Option<f64> {
        config.query_accuracy.get(&rqe.accuracy_metric).copied()
    }

    fn cheapest_feasible(rqe: &Rqe, costs: &[AtomicCostEntry]) -> usize {
        (0..costs.len())
            .filter(|&i| rqe.accuracy_ok_for(&costs[i]))
            .min_by(|&a, &b| compare_resources(&costs[a], &costs[b]))
            .unwrap()
    }

    #[test]
    fn picks_smallest_feasible_config() {
        let costs = grid(&[1, 2, 3], &[100, 200, 400, 800]);
        // Needs rows * cols >= 1000: rows=3 cols=400 (1200 counters) is the
        // smallest; rows=2 cols=800 and rows=3 cols=800 also pass.
        let r = rqe("r", 3_600, 60, 1.0 / 1_000.0);
        let found = search(&r, &costs, 7, table_accuracy);
        assert_eq!(found.selected, Some(cheapest_feasible(&r, &costs)));
    }

    #[test]
    fn evaluates_fewer_configs_than_exhaustive_search() {
        let rows: Vec<u64> = (1..=8).collect();
        let cols: Vec<u64> = (0..8).map(|i| 64 << i).collect();
        let costs = grid(&rows, &cols);
        let r = rqe("r", 3_600, 60, 1.0 / 2_000.0);
        let found = search(&r, &costs, 42, table_accuracy);
        assert!(found.selected.is_some());
        assert!(
            found.probes.len() < costs.len(),
            "{} probes",
            found.probes.len()
        );
        let unique: BTreeSet<_> = found.probes.iter().collect();
        assert_eq!(unique.len(), found.probes.len());
    }

    #[test]
    fn never_shares_identical_choices() {
        let costs = grid(&[2, 3], &[256, 512]);
        let rqes = vec![rqe("a", 3_600, 60, 0.01), rqe("b", 3_600, 60, 0.01)];
        let plan = plan(&rqes, &costs, 1, table_accuracy).unwrap();
        assert_eq!(plan.deployments.len(), 2);
        assert_eq!(plan.deployments[0], plan.deployments[1]);
        assert_eq!(plan.mapping, vec![0, 1]);

        let facts = facts(1, 10);
        let both = score(&rqes, &plan.deployments, &plan.mapping, &facts);
        let one = score(&rqes[..1], &plan.deployments[..1], &vec![0], &facts);
        assert_eq!(
            both.ingest,
            PhaseCost {
                cpu_secs_per_sec: 2.0 * one.ingest.cpu_secs_per_sec,
                memory_bytes: 2.0 * one.ingest.memory_bytes,
            }
        );
    }

    #[test]
    fn adapted_window_is_eligible() {
        let costs = grid(&[2, 3], &[256, 512]);
        // T divides S, and T does not divide S (gcd slide).
        let rqes = vec![rqe("tiled", 3_600, 60, 0.01), rqe("gcd", 3_600, 280, 0.01)];
        let plan = plan(&rqes, &costs, 1, table_accuracy).unwrap();
        assert_eq!(window_adapter(&rqes[0]), (3_600, 60));
        assert_eq!(window_adapter(&rqes[1]), (3_600, 40));
        for (r, d) in rqes.iter().zip(&plan.deployments) {
            assert!(is_eligible(r, d), "{} not eligible", r.id);
        }
    }

    #[test]
    fn reports_unservable_rqe() {
        let costs = grid(&[2, 3], &[256, 512]);
        let rqes = vec![
            rqe("ok", 3_600, 60, 0.01),
            rqe("too_strict", 3_600, 60, 1e-9),
        ];
        assert_eq!(
            plan(&rqes, &costs, 1, table_accuracy).unwrap_err(),
            vec!["too_strict".to_string()]
        );
    }

    #[test]
    fn missing_measurement_is_infeasible() {
        let costs = grid(&[2, 3], &[256, 512]);
        let r = rqe("r", 3_600, 60, 0.01);
        let found = search(&r, &costs, 1, |_, _| None);
        assert_eq!(found.selected, None);
        assert_eq!(found.probes.len(), costs.len());
    }

    fn kll(k: u64) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "kll-percall".into(),
            sketch_config: serde_json::json!({"algorithm": "kll-percall", "params": {"k": k}}),
            mem_bytes_per_instance: (k * 8) as f64,
            insert_cpu_secs: 1e-8,
            merge_cpu_secs: 1e-6,
            query_cpu_secs: 1e-6,
            query_accuracy: BTreeMap::from([(ERR.into(), 1.0 / k as f64)]),
        }
    }

    fn quantile_rqe(tolerance: f64) -> Rqe {
        Rqe {
            capability: Capability::Quantile,
            ..rqe("q", 3_600, 60, tolerance)
        }
    }

    #[test]
    fn single_axis_path_stops_at_the_feasibility_boundary() {
        // Feasible iff k >= 6. One LHS seed s, and no extra cheapest seed
        // because the seed is measured. A feasible seed shrinks to k = 5 and
        // stops; an infeasible one grows to k = 6 and stops.
        let costs: Vec<_> = (1..=10).map(kll).collect();
        let r = quantile_rqe(1.0 / 6.0);
        for seed in 0..20 {
            let found = search(&r, &costs, seed, table_accuracy);
            let ks: Vec<u64> = found.probes.iter().map(|&i| i as u64 + 1).collect();
            assert_eq!(found.selected, Some(5), "seed {seed}: {ks:?}");
            let s = ks[0];
            let (lo, hi) = (s.min(5), s.max(6));
            assert_eq!(ks.len() as u64, hi - lo + 1, "seed {seed}: {ks:?}");
            assert!(
                ks.iter().all(|&k| (lo..=hi).contains(&k)),
                "seed {seed}: {ks:?}"
            );
        }
    }

    #[test]
    fn chooses_the_cheaper_variant() {
        let mut costs = grid(&[2, 3], &[256, 512]);
        let mut cs = cms(2, 256);
        cs.sketch = "countsketch-heap-topk-fastpath-vector2d".into();
        cs.mem_bytes_per_instance = 100.0;
        costs.push(cs);
        let r = rqe("r", 3_600, 60, 0.01);
        let found = search(&r, &costs, 3, table_accuracy);
        assert_eq!(found.selected, Some(costs.len() - 1));
    }

    #[test]
    fn higher_is_better_metric_is_a_floor() {
        // Precision rises with k; the target is a floor of 0.95.
        let costs: Vec<_> = (1..=10)
            .map(|k| {
                let mut c = kll(k);
                c.query_accuracy = BTreeMap::from([(ERR.into(), 0.9 + 0.01 * k as f64)]);
                c
            })
            .collect();
        let r = Rqe {
            accuracy_direction: AccuracyDirection::HigherIsBetter,
            ..quantile_rqe(0.95)
        };
        let found = search(&r, &costs, 11, table_accuracy);
        assert_eq!(found.selected, Some(4)); // k = 5: 0.95
    }

    #[test]
    fn lhs_samples_take_distinct_values_on_every_axis() {
        let costs = grid(&[1, 2, 3, 4], &[64, 128, 256, 512, 1024]);
        let g = Grid::new("cms-heap-topk-fastpath-vector2d", &costs).unwrap();
        for seed in 0..10 {
            let samples = g.lhs(seed);
            assert_eq!(samples.len(), 4);
            for axis in 0..2 {
                let values: BTreeSet<u64> = samples.iter().map(|&i| g.key_of(i)[axis]).collect();
                assert_eq!(values.len(), samples.len(), "seed {seed} axis {axis}");
            }
        }
    }
}
