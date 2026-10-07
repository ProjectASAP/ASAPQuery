//! Each sketch algorithm's published accuracy guarantee, at 95% confidence,
//! in the metric the optimizer checks it by. [`crate::saturation`] falls back
//! to it where the study measured nothing: past an unsaturated curve, past
//! the measured merge counts or N, or with no merge curve for a point.
//!
//! The bounds hold for any stream length, so they don't depend on N. They are
//! looser than the measured curves.

use std::collections::BTreeMap;

use aqpbm_core::MeasuredShape;

use crate::analytical_cost_model::TOPK_ENTRIES;

/// Confidence the bounds are stated at. Rank and relative errors are
/// absolute, so their bounds are two-sided; CMS only overestimates, so the
/// top-k bound is one-sided.
pub const CONFIDENCE: f64 = 0.95;
/// The standard normal quantile at [`CONFIDENCE`], two-sided.
const Z: f64 = 1.96;

/// The bound on `sketch`'s error with `params` on data shaped `shape`,
/// answering from `merges` merged instances, or `None` when the algorithm
/// has none at [`CONFIDENCE`]:
///
/// - `kll-percall`: normalized rank error of one quantile. DataSketches
///   publishes `2.296 / k^0.9723` at 99%; KLL's error scales with
///   `sqrt(ln(2/δ))` (two-sided), which gives the 95% value. KLL merges without loss of
///   this guarantee.
/// - `dd`: relative value error `α` for an unbounded store (sketch-bench's
///   DDSketch collapses no buckets); merges exactly. The measured metric can
///   exceed `α` (zero-bucket values, rank conventions), which the caller's
///   floor at the last measurement covers.
/// - `hll`: relative error `z · 1.04 / sqrt(2^lg_k)`, `z` the two-sided
///   normal quantile; merges exactly.
/// - `cms-heap-topk-fastpath-vector2d`: precision@k on Zipf(θ) over `K`
///   keys, for every key at once. A CMS row overshoots a key by more than
///   `t` with probability at most `N / (w·t)` (Markov), and the `d` rows are
///   independent, so a non-top key overtakes top key `i` with probability at
///   most `(w·(p_i − p_{k+1}))^−d`. A union bound over the `K − k` non-top
///   keys, then over the counted top keys, keeps the total failure within
///   `1 − CONFIDENCE`: precision is the share of the top `k` counted,
///   taken from the most frequent down. It assumes the heap ranks keys by
///   their final CMS estimates. A merged answer has none: each shard's heap
///   keeps only its own top `k`.
pub fn bound(
    sketch: &str,
    params: &BTreeMap<String, f64>,
    shape: MeasuredShape,
    merges: u64,
) -> Option<f64> {
    match sketch {
        "kll-percall" => {
            let k = params.get("k")?;
            let at_99 = 2.296 / k.powf(0.9723);
            // Two-sided tails: ln(2/δ).
            Some(at_99 * ((2.0 / (1.0 - CONFIDENCE)).ln() / 200f64.ln()).sqrt())
        }
        "dd" => params.get("alpha").copied(),
        "hll" => {
            let registers = 2f64.powf(*params.get("lg_k")?);
            Some(Z * 1.04 / registers.sqrt())
        }
        "cms-heap-topk-fastpath-vector2d" if merges <= 1 => {
            let (rows, cols) = (*params.get("rows")?, *params.get("cols")?);
            let MeasuredShape::Zipf { skew, keys } = shape else {
                return None;
            };
            Some(topk_precision(skew, keys, rows, cols))
        }
        _ => None,
    }
}

/// The share of the top [`TOPK_ENTRIES`] keys of Zipf(`skew`) over `keys`
/// keys that a CMS with `rows` × `cols` keeps ranked, all at once with
/// probability [`CONFIDENCE`] (see [`bound`]).
fn topk_precision(skew: f64, keys: f64, rows: f64, cols: f64) -> f64 {
    let k = TOPK_ENTRIES as usize;
    if keys <= k as f64 {
        return 1.0;
    }
    let total = zipf_normalizer(skew, keys);
    let share = |rank: usize| (rank as f64).powf(-skew) / total;
    let next = share(k + 1);
    let mut failure = 0.0;
    let mut ranked = 0;
    for i in 1..=k {
        failure += (keys - k as f64) * (cols * (share(i) - next)).powf(-rows);
        if failure > 1.0 - CONFIDENCE {
            break;
        }
        ranked += 1;
    }
    ranked as f64 / k as f64
}

/// `Σ_{n=1}^{K} n^−θ`: the first terms exactly, the rest by its integral
/// with the endpoint correction (Euler–Maclaurin), so large `K` costs O(1).
fn zipf_normalizer(skew: f64, keys: f64) -> f64 {
    const EXACT: f64 = 1000.0;
    let head = keys.min(EXACT) as usize;
    let mut total: f64 = (1..=head).map(|n| (n as f64).powf(-skew)).sum();
    if keys > EXACT {
        let integral = |x: f64| {
            if (skew - 1.0).abs() < 1e-12 {
                x.ln()
            } else {
                x.powf(1.0 - skew) / (1.0 - skew)
            }
        };
        total += integral(keys) - integral(EXACT) + (keys.powf(-skew) - EXACT.powf(-skew)) / 2.0;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
        pairs.iter().map(|&(k, v)| (k.to_string(), v)).collect()
    }

    const PARETO: MeasuredShape = MeasuredShape::Pareto { tail_index: 2.0 };

    #[test]
    fn kll_hll_and_dd_bounds_ignore_n_and_merges() {
        let kll = bound("kll-percall", &params(&[("k", 200.0)]), PARETO, 64).unwrap();
        // 1.32% at 99%, scaled by sqrt(ln 40 / ln 200).
        assert!((kll - 0.01109).abs() < 1e-4, "{kll}");
        assert_eq!(
            bound("dd", &params(&[("alpha", 0.01)]), PARETO, 1000),
            Some(0.01)
        );
        let hll = bound("hll", &params(&[("lg_k", 12.0)]), PARETO, 4).unwrap();
        assert!((hll - 1.96 * 1.04 / 64.0).abs() < 1e-12);
    }

    #[test]
    fn topk_precision_holds_for_every_key_at_once() {
        let zipf = MeasuredShape::Zipf {
            skew: 1.1,
            keys: 1e4,
        };
        let topk = |rows: f64, cols: f64, merges| {
            bound(
                "cms-heap-topk-fastpath-vector2d",
                &params(&[("rows", rows), ("cols", cols)]),
                zipf,
                merges,
            )
        };
        let wide = topk(5.0, 16384.0, 1).unwrap();
        let narrow = topk(3.0, 256.0, 1).unwrap();
        // More rows and columns keep more of the top 32 ranked; the 32nd and
        // 33rd keys of Zipf 1.1 are too close for any of them.
        assert!(wide > narrow, "{wide} {narrow}");
        assert!(wide < 1.0 && wide > 0.0, "{wide}");
        // Merged heaps: no guarantee.
        assert_eq!(topk(5.0, 16384.0, 4), None);
    }

    #[test]
    fn the_normalizer_matches_the_exact_sum() {
        for (skew, keys) in [(1.1, 1e4), (0.5, 5e4), (1.0, 2e3), (2.0, 1e5)] {
            let exact: f64 = (1..=keys as usize).map(|n| (n as f64).powf(-skew)).sum();
            let approx = zipf_normalizer(skew, keys);
            assert!(
                (approx - exact).abs() / exact < 1e-6,
                "{skew} {keys}: {approx} {exact}"
            );
        }
    }

    #[test]
    fn sketches_without_a_published_bound_have_none() {
        assert_eq!(bound("univmon-topk", &BTreeMap::new(), PARETO, 1), None);
    }
}
