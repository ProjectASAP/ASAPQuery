// Fidelity benchmarks comparing legacy vs sketchlib implementations across sketch types.
#![allow(dead_code)]

use clap::Parser;
use sketch_core::config::{self, ImplMode};
use sketch_core::count_min::CountMinSketch;

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

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_CMS_IMPL)]
    cms_impl: ImplMode,
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_KLL_IMPL)]
    kll_impl: ImplMode,
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_CMWH_IMPL)]
    cmwh_impl: ImplMode,
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
}
