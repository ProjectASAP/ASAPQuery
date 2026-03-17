use std::collections::HashMap;

use clap::Parser;
use sketch_core::config::{self, ImplMode};
use sketch_core::count_min::CountMinSketch;
use sketch_core::count_min_with_heap::CountMinSketchWithHeap;
use sketch_core::hydra_kll::HydraKllSketch;
use sketch_core::kll::KllSketch;

#[derive(Clone)]
struct Lcg64 {
    state: u64,
}

impl Lcg64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_f64_0_1(&mut self) -> f64 {
        let x = self.next_u64() >> 11;
        (x as f64) / ((1u64 << 53) as f64)
    }
}

fn pearson_corr(exact: &[f64], est: &[f64]) -> f64 {
    let n = exact.len().min(est.len());
    if n == 0 {
        return f64::NAN;
    }
    let (mut sum_x, mut sum_y) = (0.0, 0.0);
    for i in 0..n {
        sum_x += exact[i];
        sum_y += est[i];
    }
    let mean_x = sum_x / (n as f64);
    let mean_y = sum_y / (n as f64);
    let (mut num, mut den_x, mut den_y) = (0.0, 0.0, 0.0);
    for i in 0..n {
        let dx = exact[i] - mean_x;
        let dy = est[i] - mean_y;
        num += dx * dy;
        den_x += dx * dx;
        den_y += dy * dy;
    }
    if den_x == 0.0 || den_y == 0.0 {
        return f64::NAN;
    }
    num / (den_x.sqrt() * den_y.sqrt())
}

fn mape(exact: &[f64], est: &[f64]) -> f64 {
    let n = exact.len().min(est.len());
    let mut num = 0.0;
    let mut denom = 0.0;
    for i in 0..n {
        if exact[i] == 0.0 {
            continue;
        }
        num += ((exact[i] - est[i]) / exact[i]).abs();
        denom += 1.0;
    }
    if denom == 0.0 {
        return if exact == est { 0.0 } else { f64::INFINITY };
    }
    (num / denom) * 100.0
}

fn rmse_percentage(exact: &[f64], est: &[f64]) -> f64 {
    let n = exact.len().min(est.len());
    let mut sum_sq = 0.0;
    let mut denom = 0.0;
    for i in 0..n {
        if exact[i] == 0.0 {
            continue;
        }
        let rel = (exact[i] - est[i]) / exact[i];
        sum_sq += rel * rel;
        denom += 1.0;
    }
    if denom == 0.0 {
        return if exact == est { 0.0 } else { f64::INFINITY };
    }
    (sum_sq / denom).sqrt() * 100.0
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value = "sketchlib")]
    cms_impl: ImplMode,
    #[arg(long, value_enum, default_value = "sketchlib")]
    kll_impl: ImplMode,
    #[arg(long, value_enum, default_value = "sketchlib")]
    cmwh_impl: ImplMode,
}

fn rank_fraction(sorted: &[f64], x: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = sorted.partition_point(|v| *v <= x);
    (idx as f64) / (sorted.len() as f64)
}

// --- CountMinSketch parameter sets and runner ---

struct CmsParams {
    depth: usize,
    width: usize,
    n: usize,
    domain: usize,
}

struct CmsResult {
    pearson: f64,
    mape: f64,
    rmse: f64,
}

fn run_countmin_once(seed: u64, p: &CmsParams) -> CmsResult {
    let mut rng = Lcg64::new(seed);
    let mut exact: Vec<f64> = vec![0.0; p.domain];
    let mut cms = CountMinSketch::new(p.depth, p.width);

    for _ in 0..p.n {
        let r = rng.next_u64();
        let key_id = if (r & 0xFF) < 200 {
            (r as usize) % 20
        } else {
            (r as usize) % p.domain
        };
        let key = format!("k{key_id}");
        cms.update(&key, 1.0);
        exact[key_id] += 1.0;
    }

    let mut est: Vec<f64> = Vec::with_capacity(p.domain);
    for key_id in 0..p.domain {
        let key = format!("k{key_id}");
        est.push(cms.query_key(&key));
    }

    CmsResult {
        pearson: pearson_corr(&exact, &est),
        mape: mape(&exact, &est),
        rmse: rmse_percentage(&exact, &est),
    }
}

// --- CountMinSketchWithHeap ---

struct CmwhParams {
    depth: usize,
    width: usize,
    n: usize,
    domain: usize,
    heap_size: usize,
}

struct CmwhResult {
    topk_recall: f64,
    pearson: f64,
    mape: f64,
    rmse: f64,
}

fn run_countmin_with_heap_once(seed: u64, p: &CmwhParams) -> CmwhResult {
    let mut rng = Lcg64::new(seed ^ 0xA5A5_A5A5);
    let mut exact: Vec<f64> = vec![0.0; p.domain];
    let mut cms = CountMinSketchWithHeap::new(p.depth, p.width, p.heap_size);

    for _ in 0..p.n {
        let r = rng.next_u64();
        let key_id = if (r & 0xFF) < 200 {
            (r as usize) % 20
        } else {
            (r as usize) % p.domain
        };
        let key = format!("k{key_id}");
        cms.update(&key, 1.0);
        exact[key_id] += 1.0;
    }

    let mut exact_pairs: Vec<(usize, f64)> = exact.iter().copied().enumerate().collect();
    exact_pairs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    exact_pairs.truncate(p.heap_size);

    let exact_top: HashMap<String, f64> = exact_pairs
        .into_iter()
        .map(|(k, v)| (format!("k{k}"), v))
        .collect();

    let mut est_vals = Vec::with_capacity(exact_top.len());
    let mut exact_vals = Vec::with_capacity(exact_top.len());
    let mut hit = 0usize;
    for item in cms.topk_heap_items() {
        if exact_top.contains_key(&item.key) {
            hit += 1;
        }
    }
    for (k, v) in &exact_top {
        exact_vals.push(*v);
        est_vals.push(cms.query_key(k));
    }

    CmwhResult {
        topk_recall: (hit as f64) / (p.heap_size as f64),
        pearson: pearson_corr(&exact_vals, &est_vals),
        mape: mape(&exact_vals, &est_vals),
        rmse: rmse_percentage(&exact_vals, &est_vals),
    }
}

// --- KllSketch ---

struct KllParams {
    k: u16,
    n: usize,
}

struct KllResult {
    rank_err_50: f64,
    rank_err_90: f64,
    rank_err_99: f64,
}

fn run_kll_once(seed: u64, p: &KllParams) -> KllResult {
    let mut rng = Lcg64::new(seed ^ 0x1234_5678);
    let mut values: Vec<f64> = Vec::with_capacity(p.n);
    let mut sk = KllSketch::new(p.k);

    for _ in 0..p.n {
        let v = rng.next_f64_0_1() * 1_000_000.0;
        values.push(v);
        sk.update(v);
    }

    values.sort_by(f64::total_cmp);
    let qs = [0.5, 0.9, 0.99];
    let rank_err = |q: f64| (rank_fraction(&values, sk.get_quantile(q)) - q).abs();

    KllResult {
        rank_err_50: rank_err(qs[0]),
        rank_err_90: rank_err(qs[1]),
        rank_err_99: rank_err(qs[2]),
    }
}

// --- HydraKllSketch ---

struct HydraKllParams {
    rows: usize,
    cols: usize,
    k: u16,
    n: usize,
    domain: usize,
    eval_keys: usize,
}

struct HydraKllResult {
    mean_50: f64,
    max_50: f64,
    mean_90: f64,
    max_90: f64,
}

fn run_hydra_kll_once(seed: u64, p: &HydraKllParams) -> HydraKllResult {
    let mut rng = Lcg64::new(seed ^ 0xDEAD_BEEF);
    let mut hydra = HydraKllSketch::new(p.rows, p.cols, p.k);
    let mut exact: HashMap<String, Vec<f64>> = HashMap::new();

    for _ in 0..p.n {
        let r = rng.next_u64();
        let key_id = if (r & 0xFF) < 200 {
            (r as usize) % 20
        } else {
            (r as usize) % p.domain
        };
        let key = format!("k{key_id}");
        let v = rng.next_f64_0_1() * 1_000_000.0;
        hydra.update(&key, v);
        exact.entry(key).or_default().push(v);
    }

    let _qs = [0.5, 0.9];
    let mut keys: Vec<String> = exact.keys().cloned().collect();
    keys.sort();
    keys.truncate(p.eval_keys);

    let mut mean_50 = 0.0f64;
    let mut max_50 = 0.0f64;
    let mut mean_90 = 0.0f64;
    let mut max_90 = 0.0f64;
    let nk = keys.len() as f64;
    for key in &keys {
        let mut vals = exact.get(key).cloned().unwrap_or_default();
        vals.sort_by(f64::total_cmp);
        for (q, mean_ref, max_ref) in [
            (0.5, &mut mean_50, &mut max_50),
            (0.9, &mut mean_90, &mut max_90),
        ] {
            let est = hydra.query(key, q);
            let err = (rank_fraction(&vals, est) - q).abs();
            *mean_ref += err;
            if err > *max_ref {
                *max_ref = err;
            }
        }
    }
    mean_50 /= nk;
    mean_90 /= nk;

    HydraKllResult {
        mean_50,
        max_50,
        mean_90,
        max_90,
    }
}

fn main() {
    let args = Args::parse();
    config::configure(args.cms_impl, args.kll_impl, args.cmwh_impl)
        .expect("sketch backend already initialised");

    let seed = 0xC0FFEE_u64;
    let mode = if matches!(args.cms_impl, ImplMode::Legacy)
        || matches!(args.kll_impl, ImplMode::Legacy)
        || matches!(args.cmwh_impl, ImplMode::Legacy)
    {
        "Legacy"
    } else {
        "sketchlib-rust"
    };

    // CountMinSketch: multiple (depth, width, n, domain)
    let cms_param_sets: Vec<CmsParams> = vec![
        CmsParams {
            depth: 3,
            width: 1024,
            n: 100_000,
            domain: 1000,
        },
        CmsParams {
            depth: 5,
            width: 2048,
            n: 200_000,
            domain: 2000,
        },
        CmsParams {
            depth: 7,
            width: 4096,
            n: 200_000,
            domain: 2000,
        },
        CmsParams {
            depth: 5,
            width: 2048,
            n: 50_000,
            domain: 500,
        },
    ];

    println!("## CountMinSketch ({mode})");
    println!("| depth | width | n_updates | domain | Pearson corr | MAPE (%) | RMSE (%) |");
    println!("|-------|-------|------------|--------|--------------|----------|----------|");
    for p in &cms_param_sets {
        let r = run_countmin_once(seed, p);
        println!(
            "| {} | {} | {} | {} | {:.10} | {:.6} | {:.6} |",
            p.depth, p.width, p.n, p.domain, r.pearson, r.mape, r.rmse
        );
    }

    // CountMinSketchWithHeap
    let cmwh_param_sets: Vec<CmwhParams> = vec![
        CmwhParams {
            depth: 3,
            width: 1024,
            n: 100_000,
            domain: 1000,
            heap_size: 10,
        },
        CmwhParams {
            depth: 5,
            width: 2048,
            n: 200_000,
            domain: 2000,
            heap_size: 20,
        },
        CmwhParams {
            depth: 5,
            width: 2048,
            n: 200_000,
            domain: 2000,
            heap_size: 50,
        },
    ];

    println!("\n## CountMinSketchWithHeap ({mode})");
    println!("| depth | width | n | domain | heap_size | Top-k recall | Pearson (top-k) | MAPE (%) | RMSE (%) |");
    println!("|-------|-------|-----|--------|-----------|--------------|-----------------|----------|----------|");
    for p in &cmwh_param_sets {
        let r = run_countmin_with_heap_once(seed, p);
        println!(
            "| {} | {} | {} | {} | {} | {:.4} | {:.10} | {:.6} | {:.6} |",
            p.depth, p.width, p.n, p.domain, p.heap_size, r.topk_recall, r.pearson, r.mape, r.rmse
        );
    }

    // KllSketch
    let kll_param_sets: Vec<KllParams> = vec![
        KllParams { k: 20, n: 200_000 },
        KllParams { k: 50, n: 200_000 },
        KllParams { k: 200, n: 200_000 },
        KllParams { k: 20, n: 50_000 },
    ];

    println!("\n## KllSketch ({mode})");
    println!(
        "| k | n_updates | q=0.5 abs_rank_error | q=0.9 abs_rank_error | q=0.99 abs_rank_error |"
    );
    println!(
        "|---|-----------|----------------------|----------------------|-----------------------|"
    );
    for p in &kll_param_sets {
        let r = run_kll_once(seed, p);
        println!(
            "| {} | {} | {:.6} | {:.6} | {:.6} |",
            p.k, p.n, r.rank_err_50, r.rank_err_90, r.rank_err_99
        );
    }

    // HydraKllSketch
    let hydra_param_sets: Vec<HydraKllParams> = vec![
        HydraKllParams {
            rows: 2,
            cols: 64,
            k: 20,
            n: 200_000,
            domain: 200,
            eval_keys: 50,
        },
        HydraKllParams {
            rows: 3,
            cols: 128,
            k: 20,
            n: 200_000,
            domain: 200,
            eval_keys: 50,
        },
        HydraKllParams {
            rows: 3,
            cols: 128,
            k: 50,
            n: 200_000,
            domain: 200,
            eval_keys: 50,
        },
        HydraKllParams {
            rows: 3,
            cols: 128,
            k: 20,
            n: 100_000,
            domain: 100,
            eval_keys: 50,
        },
    ];

    println!("\n## HydraKllSketch ({mode})");
    println!("| rows | cols | k | n | domain | q=0.5 mean/max | q=0.9 mean/max |");
    println!("|------|------|---|-----|--------|----------------|----------------|");
    for p in &hydra_param_sets {
        let r = run_hydra_kll_once(seed, p);
        println!(
            "| {} | {} | {} | {} | {} | {:.5} / {:.5} | {:.5} / {:.5} |",
            p.rows, p.cols, p.k, p.n, p.domain, r.mean_50, r.max_50, r.mean_90, r.max_90
        );
    }
}
