# Sketchlib Fidelity Report

Compares the **legacy** sketch implementations in `sketch-core` vs the new **sketchlib-rust** backends for:

- `CountMinSketch`
- `CountMinSketchWithHeap` (Count-Min portion)

## Fidelity harness

The fidelity binary selects backends via CLI flags.

| Goal                     | Command                                                                                                      |
|--------------------------|--------------------------------------------------------------------------------------------------------------|
| CMS + CMWH sketchlib     | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl sketchlib --cmwh-impl sketchlib`             |
| CMS + CMWH legacy        | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl legacy --cmwh-impl legacy`                    |
| CMS sketchlib, CMWH legacy | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl sketchlib --cmwh-impl legacy`               |

Section titles in the binary reflect **per-sketch** mode (`--cms-impl` for CountMinSketch, `--cmwh-impl` for CountMinSketchWithHeap); `kll_impl` does not affect these tables.

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

---

### CountMinSketchWithHeap (top-k + CMS accuracy on exact top-k)

The heap is maintained by local updates; recall is measured against the **true** top-k at the end of the stream.

#### depth=3

| width | n      | domain | heap_size | Mode           | Top-k recall | Pearson (top-k) | MAPE (%) | RMSE (%) |
|-------|--------|--------|-----------|----------------|--------------|-----------------|----------|----------|
| 1024  | 100000 | 1000   | 10        | Legacy         | 0.40         | 0.9571          | 0.174    | 0.319    |
| 1024  | 100000 | 1000   | 10        | sketchlib-rust | 0.80         | 1.0000          | 0.000    | 0.000    |

#### depth=5

| width | n      | domain | heap_size | Mode           | Top-k recall | Pearson (top-k) | MAPE (%) | RMSE (%) |
|-------|--------|--------|-----------|----------------|--------------|-----------------|----------|----------|
| 2048  | 200000 | 2000   | 20        | Legacy         | 0.60         | 0.9964          | 0.045    | 0.101    |
| 2048  | 200000 | 2000   | 20        | sketchlib-rust | 1.00         | 0.9982          | 0.021    | 0.067    |
| 2048  | 200000 | 2000   | 50        | Legacy         | 0.40         | 0.9999983       | 5.60     | 16.49    |
| 2048  | 200000 | 2000   | 50        | sketchlib-rust | 0.48         | 0.9999990       | 3.90     | 12.95    |
