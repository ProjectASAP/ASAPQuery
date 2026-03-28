# Sketchlib Fidelity Report

Compares the **legacy** Count-Min Sketch implementation in `sketch-core` vs the new **sketchlib-rust** backend.

## Fidelity harness

The fidelity binary selects backends via CLI flags.

| Goal        | Command                                                       |
|-------------|---------------------------------------------------------------|
| CMS sketchlib | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl sketchlib` |
| CMS legacy   | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl legacy` |

## Unit tests

Unit tests always run with **legacy** backends enabled (the test ctor calls
`force_legacy_mode_for_tests()`), so you only need:

```bash
cargo test -p sketch-core
```

## Results

### CountMinSketch (accuracy vs exact counts)

#### depth=3

| width | n      | domain | Mode           | Pearson corr   | MAPE (%) | RMSE (%) |
|-------|--------|--------|----------------|----------------|----------|----------|
| 1024  | 100000 | 1000   | Legacy         | 0.9998451189   | 24.48    | 52.76    |
| 1024  | 100000 | 1000   | sketchlib-rust | 0.9998387103   | 24.36    | 54.11    |

#### depth=5

| width | n      | domain | Mode           | Pearson corr   | MAPE (%) | RMSE (%) |
|-------|--------|--------|----------------|----------------|----------|----------|
| 2048  | 200000 | 2000   | Legacy         | 0.9999733814   | 8.75     | 29.94    |
| 2048  | 200000 | 2000   | sketchlib-rust | 0.9999744627   | 8.37     | 28.84    |
| 2048  | 50000  | 500    | Legacy         | 1.0000000000   | 0.00     | 0.00     |
| 2048  | 50000  | 500    | sketchlib-rust | 1.0000000000   | 0.00     | 0.00     |

#### depth=7

| width | n      | domain | Mode           | Pearson corr   | MAPE (%) | RMSE (%) |
|-------|--------|--------|----------------|----------------|----------|----------|
| 4096  | 200000 | 2000   | Legacy         | 0.9999993694   | 0.20     | 3.69     |
| 4096  | 200000 | 2000   | sketchlib-rust | 0.9999993499   | 0.21     | 4.27     |
