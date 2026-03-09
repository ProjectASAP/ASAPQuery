# Report

Compares the **legacy** sketch implementations in `sketch-core` vs the new **sketchlib-rust** backends for:

- `CountMinSketch`
- `CountMinSketchWithHeap` (Count-Min portion)
- `KllSketch`
- `HydraKllSketch` (via `KllSketch`)




### Fidelity harness

The fidelity binary now selects backends via CLI flags instead of environment variables.

| Goal                     | Command                                                                                                      |
|--------------------------|--------------------------------------------------------------------------------------------------------------|
| Default (all sketchlib)  | `cargo run -p sketch-core --bin sketchlib_fidelity`                                                          |
| All legacy               | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl legacy --kll-impl legacy --cmwh-impl legacy` |
| Legacy KLL only          | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl sketchlib --kll-impl legacy --cmwh-impl sketchlib` |

### Unit tests

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

---

### CountMinSketchWithHeap (top-k + CMS accuracy on exact top-k)

The heap is maintained by local updates; recall is measured against the **true** top-k at the end of the stream.

#### depth=3

| width | n      | domain | heap_size | Mode           | Top-k recall | Pearson (top-k) | MAPE (%) | RMSE (%) |
|-------|--------|--------|-----------|----------------|--------------|-----------------|----------|----------|
| 1024  | 100000 | 1000   | 10        | Legacy         | 0.40         | 0.9571          | 0.174    | 0.319    |
| 1024  | 100000 | 1000   | 10        | sketchlib-rust | 0.40         | 1.0000          | 0.000    | 0.000    |

#### depth=5

| width | n      | domain | heap_size | Mode           | Top-k recall | Pearson (top-k) | MAPE (%) | RMSE (%) |
|-------|--------|--------|-----------|----------------|--------------|-----------------|----------|----------|
| 2048  | 200000 | 2000   | 20        | Legacy         | 0.60         | 0.9964          | 0.045    | 0.101    |
| 2048  | 200000 | 2000   | 20        | sketchlib-rust | 0.60         | 0.9982          | 0.021    | 0.067    |
| 2048  | 200000 | 2000   | 50        | Legacy         | 0.40         | 0.9999983       | 5.60     | 16.49    |
| 2048  | 200000 | 2000   | 50        | sketchlib-rust | 0.40         | 0.9999990       | 3.90     | 12.95    |

---

### KllSketch (quantiles, absolute rank error)

For each quantile \(q\), we compute the sketch estimate `est_value`, then:
`abs_rank_error = |rank_fraction(exact_sorted_values, est_value) - q|`.

#### k=20

| n_updates | Mode           | q=0.5   | q=0.9   | q=0.99  |
|-----------|----------------|---------|---------|---------|
| 200000    | Legacy         | 0.0104  | 0.0145  | 0.0028  |
| 200000    | sketchlib-rust | 0.0275  | 0.0470  | 0.0061  |
| 50000     | Legacy         | 0.0131  | 0.0091  | 0.0054  |
| 50000     | sketchlib-rust | 0.0110  | 0.0116  | 0.0031  |

#### k=50

| n_updates | Mode           | q=0.5   | q=0.9   | q=0.99  |
|-----------|----------------|---------|---------|---------|
| 200000    | Legacy         | 0.0013  | 0.0021  | 0.0012  |
| 200000    | sketchlib-rust | 0.0101  | 0.0044  | 0.0074  |

#### k=200

| n_updates | Mode           | q=0.5   | q=0.9   | q=0.99  |
|-----------|----------------|---------|---------|---------|
| 200000    | Legacy         | 0.0021  | 0.0036  | 0.0000  |
| 200000    | sketchlib-rust | 0.0015  | 0.0001  | 0.0002  |

---

### HydraKllSketch (per-key quantiles, mean/max absolute rank error across 50 keys)

#### rows=2, cols=64

| k   | n      | domain | Mode           | q=0.5 (mean / max) | q=0.9 (mean / max) |
|-----|--------|--------|----------------|--------------------|--------------------|
| 20  | 200000 | 200    | Legacy         | 0.0170 / 0.0546    | 0.0165 / 0.0452    |
| 20  | 200000 | 200    | sketchlib-rust | 0.0254 / 0.0629    | 0.0546 / 0.0942    |

#### rows=3, cols=128

| k   | n      | domain | Mode           | q=0.5 (mean / max) | q=0.9 (mean / max) |
|-----|--------|--------|----------------|--------------------|--------------------|
| 20  | 200000 | 200    | Legacy         | 0.0166 / 0.0591    | 0.0114 / 0.0304    |
| 20  | 200000 | 200    | sketchlib-rust | 0.0216 / 0.0534    | 0.0238 / 0.1087    |
| 50  | 200000 | 200    | Legacy         | 0.0099 / 0.0352    | 0.0087 / 0.0330    |
| 50  | 200000 | 200    | sketchlib-rust | 0.0119 / 0.0458    | 0.0119 / 0.0296    |
| 20  | 100000 | 100    | Legacy         | 0.0141 / 0.0574    | 0.0149 / 0.0471    |
| 20  | 100000 | 100    | sketchlib-rust | 0.0202 / 0.0621    | 0.0287 / 0.0779    |
