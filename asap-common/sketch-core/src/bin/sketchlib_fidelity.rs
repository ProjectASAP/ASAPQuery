// Scaffold for fidelity benchmarks; helpers used in later PRs when sketch types are integrated.
#![allow(dead_code)]

use clap::Parser;
use sketch_core::config::{self, ImplMode};

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

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_IMPL_MODE)]
    cms_impl: ImplMode,
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_IMPL_MODE)]
    kll_impl: ImplMode,
    #[arg(long, value_enum, default_value_t = sketch_core::config::DEFAULT_IMPL_MODE)]
    cmwh_impl: ImplMode,
}

fn main() {
    let args = Args::parse();
    config::configure(args.cms_impl, args.kll_impl, args.cmwh_impl)
        .expect("sketch backend already initialised");

    let mode = if matches!(args.cms_impl, ImplMode::Legacy)
        || matches!(args.kll_impl, ImplMode::Legacy)
        || matches!(args.cmwh_impl, ImplMode::Legacy)
    {
        "Legacy"
    } else {
        "sketchlib-rust"
    };

    println!("# Sketchlib Fidelity Report ({})", mode);
    println!();
    println!("Fidelity tests will be added as sketch implementations are integrated.");
}
