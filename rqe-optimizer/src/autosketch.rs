//! AutoSketch-Adapted: the per-query configuration search of AutoSketch
//! (Sun et al., NSDI '24, Algorithm 4), used as a baseline against the
//! batch MILP. Ported from ASAPQuery-backend
//! `data_plane/examples/autosketch_comparison.rs` and generalized from its
//! fixed CMS width/depth grid to each variant's measured parameter axes.
//!
//! AutoSketch plans one query at a time, never shares state across queries,
//! and constrains accuracy only. So every RAQE is searched independently,
//! gets its own deployment, and latency is not considered.
//!
//! One extension over the paper: AutoSketch's compiler maps an operator to one
//! sketch algorithm and tunes its parameters, while this search covers every
//! deployable variant serving the RAQE's capability and keeps the cheapest,
//! so it chooses from the same sketches as the MILP, except those shared by
//! all groups (sketch-bench#159).

use crate::candidates::gcd;
use crate::{AtomicCostEntry, Deployment, Mapping, Millis, Raqe};
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

/// One RAQE's search. `selected` and `probes` index into the cost table.
#[derive(Debug, Clone)]
pub struct RaqeSearch {
    pub raqe_id: String,
    /// Smallest feasible configuration found; `None` means unservable.
    pub selected: Option<usize>,
    /// Distinct configurations whose accuracy was evaluated, in order.
    pub probes: Vec<usize>,
    pub search_wall_secs: f64,
}

/// A whole-workload plan: one dedicated deployment per RAQE, so identical
/// choices are still paid for once per RAQE, and the identity mapping.
#[derive(Debug, Clone)]
pub struct AutoSketchPlan {
    pub deployments: Vec<Deployment>,
    pub mapping: Mapping,
    pub searches: Vec<RaqeSearch>,
}

/// One sliding sketch per query window: `x = S`, `y = T`, or `gcd(S, T)`
/// when `T` does not divide `S`, so the window still tiles into slides.
pub fn window_adapter(raqe: &Raqe) -> (Millis, Millis) {
    (raqe.lookback_ms, gcd(raqe.lookback_ms, raqe.interval_ms))
}

/// Search every RAQE independently. `accuracy` is the benchmark oracle: a
/// configuration's accuracy in its family's metric ([`crate::accuracy_key`]),
/// or `None` when it was not measured. Returns the IDs of unservable RAQEs.
/// `allow_undeployable_families` as in
/// [`crate::Capability::candidate_families`].
pub fn plan(
    raqes: &[Raqe],
    costs: &[AtomicCostEntry],
    seed: u64,
    allow_undeployable_families: bool,
    mut accuracy: impl FnMut(&Raqe, &AtomicCostEntry) -> Option<f64>,
) -> Result<AutoSketchPlan, Vec<String>> {
    // One resolution per distinct (k, top-k?), not per RAQE.
    let keys: Vec<(u64, bool)> = raqes
        .iter()
        .map(|r| (r.topk_k(), crate::candidates::is_topk(r.capability)))
        .collect();
    let mut resolutions: BTreeMap<(u64, bool), Resolved> = BTreeMap::new();
    for (raqe, &key) in raqes.iter().zip(&keys) {
        resolutions
            .entry(key)
            .or_insert_with(|| resolve_for(raqe, costs));
    }
    let searches: Vec<_> = raqes
        .iter()
        .zip(&keys)
        .map(|(raqe, key)| {
            search_resolved(
                raqe,
                &resolutions[key],
                seed,
                allow_undeployable_families,
                &mut accuracy,
            )
        })
        .collect();
    let unservable: Vec<_> = searches
        .iter()
        .filter(|s| s.selected.is_none())
        .map(|s| s.raqe_id.clone())
        .collect();
    if !unservable.is_empty() {
        return Err(unservable);
    }
    let deployments = raqes
        .iter()
        .zip(&searches)
        .zip(&keys)
        .map(|((raqe, s), key)| {
            let (window_ms, slide_ms) = window_adapter(raqe);
            Deployment {
                capability: raqe.capability,
                metric: raqe.metric.clone(),
                spatial_filter: raqe.spatial_filter.clone(),
                grouping_labels: raqe.grouping_labels.clone(),
                config: resolutions[key].costs
                    [s.selected.expect("unservable RAQEs returned above")]
                .clone(),
                window_ms,
                slide_ms,
                key_tracker: None,
            }
        })
        .collect();
    Ok(AutoSketchPlan {
        deployments,
        mapping: (0..raqes.len()).collect(),
        searches,
    })
}

/// `costs` as a top-`k` RAQE sees them, index for index: each heap top-k
/// config's smallest-heap row priced at the heap a one-window answer needs
/// (`k`, floored at the smallest measured heap) by the MILP's own
/// [`crate::candidates::heap_row`], and its other heap rows masked out of
/// the search. AutoSketch never merges (x = S), so heap is no search axis.
/// For a top-k RAQE (`topk`), rows that can't answer `k` are masked too.
fn resolve_heaps(costs: &[AtomicCostEntry], k: u64, topk: bool) -> Resolved {
    let mut resolved = costs.to_vec();
    let mut usable = vec![true; costs.len()];
    // Heap rows by family and counter shape, then by heap, with their index.
    let mut groups: BTreeMap<(String, String), BTreeMap<u64, usize>> = BTreeMap::new();
    for (i, row) in costs.iter().enumerate() {
        if let Some(heap) = crate::heap_capacity(row) {
            let mut shape = row.sketch_config["params"].clone();
            if let Some(params) = shape.as_object_mut() {
                params.remove("heap");
            }
            groups
                .entry((row.sketch.clone(), shape.to_string()))
                .or_default()
                .insert(heap, i);
        }
    }
    for by_heap in groups.values() {
        let rows: BTreeMap<u64, &AtomicCostEntry> = by_heap
            .iter()
            .map(|(&heap, &i)| (heap, &costs[i]))
            .collect();
        by_heap.values().for_each(|&i| usable[i] = false);
        let (Some((_, &smallest)), Some(entry)) = (
            by_heap.first_key_value(),
            crate::candidates::heap_row(&rows, k, k),
        ) else {
            continue;
        };
        resolved[smallest] = entry;
        usable[smallest] = true;
    }
    // A fixed-k top-k family answers TOPK_K only.
    if topk {
        for (i, row) in resolved.iter().enumerate() {
            usable[i] &= crate::serves_topk_k(row, k);
        }
    }
    Resolved {
        costs: resolved,
        usable,
    }
}

/// `costs` as a RAQE sees them ([`resolve_heaps`]) and which rows the
/// search may use.
struct Resolved {
    costs: Vec<AtomicCostEntry>,
    usable: Vec<bool>,
}

/// Algorithm 4 for one RAQE: LHS seeds per sketch variant, then
/// feasibility-directed neighbor search with the paper's pruning and
/// stopping rules. Minimizes memory per instance, then insert CPU.
pub fn search(
    raqe: &Raqe,
    costs: &[AtomicCostEntry],
    seed: u64,
    allow_undeployable_families: bool,
    accuracy: impl FnMut(&Raqe, &AtomicCostEntry) -> Option<f64>,
) -> RaqeSearch {
    let resolved = resolve_for(raqe, costs);
    search_resolved(raqe, &resolved, seed, allow_undeployable_families, accuracy)
}

fn resolve_for(raqe: &Raqe, costs: &[AtomicCostEntry]) -> Resolved {
    resolve_heaps(
        costs,
        raqe.topk_k(),
        crate::candidates::is_topk(raqe.capability),
    )
}

/// [`search`] over costs already resolved for `raqe`.
fn search_resolved(
    raqe: &Raqe,
    resolved: &Resolved,
    seed: u64,
    allow_undeployable_families: bool,
    mut accuracy: impl FnMut(&Raqe, &AtomicCostEntry) -> Option<f64>,
) -> RaqeSearch {
    let started = Instant::now();
    let (costs, usable) = (&resolved.costs[..], &resolved.usable);
    let grids: Vec<Grid> = raqe
        .capability
        .candidate_families(allow_undeployable_families)
        // ponytail: ranking compares per-instance memory, which a sketch
        // shared by all groups isn't comparable on; sketch-bench#159.
        .filter(|variant| !crate::family_properties(variant).one_fixed_size_sketch_for_all_groups)
        .filter_map(|variant| Grid::new(variant, costs, usable))
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
        let feasible = raqe.meets_sla(&costs[index].sketch, accuracy(raqe, &costs[index]));
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

    RaqeSearch {
        raqe_id: raqe.id.clone(),
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
    /// `k`: a heap top-k variant's grid holds the smallest measured heap
    /// that answers a top-`k` query.
    /// `usable` masks the rows [`resolve_heaps`] set aside.
    fn new(variant: &str, costs: &[AtomicCostEntry], usable: &[bool]) -> Option<Grid> {
        let rows: Vec<(usize, BTreeMap<String, f64>)> = costs
            .iter()
            .enumerate()
            .filter(|&(i, c)| c.sketch == variant && usable[i])
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

/// Search axes: numeric params but the heap and k, which
/// [`resolve_heaps`] fixes per RAQE.
fn numeric_params(sketch_config: &serde_json::Value) -> BTreeMap<String, f64> {
    sketch_config
        .get("params")
        .and_then(serde_json::Value::as_object)
        .map(|params| {
            params
                .iter()
                .filter(|(name, _)| !matches!(name.as_str(), "heap" | "topk_k"))
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
    use crate::test_support::{facts, measured_at, METRIC};
    use crate::{row_accuracy, Capability, LabelSet};
    use std::collections::BTreeSet;

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
            // Precision rises with width and depth, so feasibility is monotone.
            query_accuracy: BTreeMap::from([(
                "precision_at_k".into(),
                1.0 - 1.0 / (rows * cols) as f64,
            )]),
            accuracy_metric: "precision_at_k".into(),
            measured_at: measured_at(),
        }
    }

    fn grid(rows: &[u64], cols: &[u64]) -> Vec<AtomicCostEntry> {
        rows.iter()
            .flat_map(|&r| cols.iter().map(move |&c| cms(r, c)))
            .collect()
    }

    /// A top-k RAQE whose precision floor is `1 - error_budget`.
    fn raqe(id: &str, lookback: Millis, interval: Millis, error_budget: f64) -> Raqe {
        Raqe {
            id: id.into(),
            capability: Capability::TopKByValue,
            lookback_ms: lookback,
            interval_ms: interval,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_sla: 1.0 - error_budget,
            latency_sla_ms: None,
            topk_k: None,
            accuracy_covers_share: None,
        }
    }

    fn table_accuracy(_: &Raqe, config: &AtomicCostEntry) -> Option<f64> {
        Some(row_accuracy(config))
    }

    fn cheapest_feasible(raqe: &Raqe, costs: &[AtomicCostEntry]) -> usize {
        (0..costs.len())
            .filter(|&i| raqe.meets_sla(&costs[i].sketch, table_accuracy(raqe, &costs[i])))
            .min_by(|&a, &b| compare_resources(&costs[a], &costs[b]))
            .unwrap()
    }

    #[test]
    fn picks_smallest_feasible_config() {
        let costs = grid(&[1, 2, 3], &[100, 200, 400, 800]);
        // Needs rows * cols >= 1000: rows=3 cols=400 (1200 counters) is the
        // smallest; rows=2 cols=800 and rows=3 cols=800 also pass.
        let r = raqe("r", 3_600_000, 60_000, 1.0 / 1_000.0);
        let found = search(&r, &costs, 7, false, table_accuracy);
        assert_eq!(found.selected, Some(cheapest_feasible(&r, &costs)));
    }

    #[test]
    fn evaluates_fewer_configs_than_exhaustive_search() {
        let rows: Vec<u64> = (1..=8).collect();
        let cols: Vec<u64> = (0..8).map(|i| 64 << i).collect();
        let costs = grid(&rows, &cols);
        let r = raqe("r", 3_600_000, 60_000, 1.0 / 2_000.0);
        let found = search(&r, &costs, 42, false, table_accuracy);
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
        let raqes = vec![
            raqe("a", 3_600_000, 60_000, 0.01),
            raqe("b", 3_600_000, 60_000, 0.01),
        ];
        let plan = plan(&raqes, &costs, 1, false, table_accuracy).unwrap();
        assert_eq!(plan.deployments.len(), 2);
        assert_eq!(plan.deployments[0], plan.deployments[1]);
        assert_eq!(plan.mapping, vec![0, 1]);

        let facts = facts(1, 10);
        let both = score(&raqes, &plan.deployments, &plan.mapping, &facts);
        let one = score(&raqes[..1], &plan.deployments[..1], &vec![0], &facts);
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
        let raqes = vec![
            raqe("tiled", 3_600_000, 60_000, 0.01),
            raqe("gcd", 3_600_000, 280_000, 0.01),
        ];
        let plan = plan(&raqes, &costs, 1, false, table_accuracy).unwrap();
        assert_eq!(window_adapter(&raqes[0]), (3_600_000, 60_000));
        assert_eq!(window_adapter(&raqes[1]), (3_600_000, 40_000));
        for (r, d) in raqes.iter().zip(&plan.deployments) {
            assert!(
                is_eligible(r, d, &facts(1, 1), &crate::table_accuracy),
                "{} not eligible",
                r.id
            );
        }
    }

    #[test]
    fn reports_unservable_raqe() {
        let costs = grid(&[2, 3], &[256, 512]);
        let raqes = vec![
            raqe("ok", 3_600_000, 60_000, 0.01),
            raqe("too_strict", 3_600_000, 60_000, 1e-9),
        ];
        assert_eq!(
            plan(&raqes, &costs, 1, false, table_accuracy).unwrap_err(),
            vec!["too_strict".to_string()]
        );
    }

    #[test]
    fn missing_measurement_is_infeasible() {
        let costs = grid(&[2, 3], &[256, 512]);
        let r = raqe("r", 3_600_000, 60_000, 0.01);
        let found = search(&r, &costs, 1, false, |_, _| None);
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
            query_accuracy: BTreeMap::from([("mean_rank_err".into(), 1.0 / k as f64)]),
            accuracy_metric: "mean_rank_err".into(),
            measured_at: measured_at(),
        }
    }

    fn quantile_raqe(max_rank_err: f64) -> Raqe {
        Raqe {
            capability: Capability::Quantile,
            accuracy_sla: max_rank_err,
            ..raqe("q", 3_600_000, 60_000, 0.0)
        }
    }

    #[test]
    fn single_axis_path_stops_at_the_feasibility_boundary() {
        // Feasible iff k >= 6. One LHS seed s, and no extra cheapest seed
        // because the seed is measured. A feasible seed shrinks to k = 5 and
        // stops; an infeasible one grows to k = 6 and stops.
        let costs: Vec<_> = (1..=10).map(kll).collect();
        let r = quantile_raqe(1.0 / 6.0);
        for seed in 0..20 {
            let found = search(&r, &costs, seed, false, table_accuracy);
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
        // countsketch-heap isn't deployable, so this needs the study flag.
        let mut costs = grid(&[2, 3], &[256, 512]);
        let mut cs = cms(2, 256);
        cs.sketch = "countsketch-heap-topk-fastpath-vector2d".into();
        cs.mem_bytes_per_instance = 100.0;
        costs.push(cs);
        let r = raqe("r", 3_600_000, 60_000, 0.01);
        let found = search(&r, &costs, 3, true, table_accuracy);
        assert_eq!(found.selected, Some(costs.len() - 1));
    }

    #[test]
    fn higher_is_better_metric_is_a_floor() {
        // Precision rises with k; the target is a floor of 0.95.
        let costs: Vec<_> = (1..=10)
            .map(|k| {
                let mut c = cms(1, k);
                c.query_accuracy =
                    BTreeMap::from([("precision_at_k".into(), 0.9 + 0.01 * k as f64)]);
                c
            })
            .collect();
        let r = Raqe {
            accuracy_sla: 0.95,
            ..raqe("r", 3_600_000, 60_000, 0.0)
        };
        let found = search(&r, &costs, 11, false, table_accuracy);
        assert_eq!(found.selected, Some(4)); // k = 5: 0.95
    }

    #[test]
    fn lhs_samples_take_distinct_values_on_every_axis() {
        let costs = grid(&[1, 2, 3, 4], &[64, 128, 256, 512, 1024]);
        let resolved = resolve_heaps(&costs, crate::TOPK_K, true);
        let g = Grid::new(
            "cms-heap-topk-fastpath-vector2d",
            &resolved.costs,
            &resolved.usable,
        )
        .unwrap();
        for seed in 0..10 {
            let samples = g.lhs(seed);
            assert_eq!(samples.len(), 4);
            for axis in 0..2 {
                let values: BTreeSet<u64> = samples.iter().map(|&i| g.key_of(i)[axis]).collect();
                assert_eq!(values.len(), samples.len(), "seed {seed} axis {axis}");
            }
        }
    }

    /// AutoSketch prices a top-k heap as the MILP does: the smallest heap
    /// row resolved at k (floored at the smallest measured heap, extended
    /// past the largest), the other heap rows masked out.
    #[test]
    fn heaps_resolve_at_k_like_the_milp() {
        let at = |heap: u64| {
            let mut row = cms(3, 256);
            row.sketch_config["params"]["heap"] = heap.into();
            row.mem_bytes_per_instance = 1000.0 + heap as f64;
            row
        };
        let costs = [at(32), at(128)];
        for (k, heap, mem) in [(10, 32, 1032.0), (100, 100, 1100.0), (512, 512, 1512.0)] {
            let Resolved {
                costs: resolved,
                usable,
            } = resolve_heaps(&costs, k, true);
            assert_eq!(usable, vec![true, false], "k = {k}");
            assert_eq!(crate::heap_capacity(&resolved[0]), Some(heap), "k = {k}");
            assert_eq!(crate::config_topk_k(&resolved[0]), k, "k = {k}");
            assert!(
                (resolved[0].mem_bytes_per_instance - mem).abs() < 1e-9,
                "k = {k}"
            );
        }
    }

    /// UnivMon answers TOPK_K only, so a top-10 search can't pick it.
    #[test]
    fn a_fixed_k_family_is_masked_off_its_k() {
        let costs = [AtomicCostEntry {
            sketch: "univmon-topk".into(),
            ..cms(3, 256)
        }];
        assert_eq!(resolve_heaps(&costs, crate::TOPK_K, true).usable, [true]);
        assert_eq!(resolve_heaps(&costs, 10, true).usable, [false]);
        // A non-top-k RAQE never asks for a k.
        assert_eq!(resolve_heaps(&costs, 10, false).usable, [true]);
        let ten = Raqe {
            topk_k: Some(10),
            ..raqe("r", 60_000, 60_000, 0.1)
        };
        assert_eq!(search(&ten, &costs, 0, true, table_accuracy).selected, None);
    }
}
