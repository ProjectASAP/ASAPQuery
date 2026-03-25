# Sketchlib Fidelity Report

Compares the **legacy** sketch implementations in `sketch-core` vs the new **sketchlib-rust** backends for:

- `CountMinSketch`
- `CountMinSketchWithHeap` (Count-Min portion)
- `KllSketch`
- `HydraKllSketch` (via `KllSketch`)

## Running Fidelity Tests

The fidelity binary selects backends via CLI flags instead of environment variables.

| Goal                     | Command                                                                                                      |
|--------------------------|--------------------------------------------------------------------------------------------------------------|
| Default (all sketchlib)  | `cargo run -p sketch-core --bin sketchlib_fidelity`                                                          |
| All legacy               | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl legacy --kll-impl legacy --cmwh-impl legacy` |
| Legacy KLL only          | `cargo run -p sketch-core --bin sketchlib_fidelity -- --cms-impl sketchlib --kll-impl legacy --cmwh-impl sketchlib` |

## Unit Tests

Unit tests always run with **legacy** backends enabled (the test ctor calls
`force_legacy_mode_for_tests()`), so you only need:

```bash
cargo test -p sketch-core
```

## Results

Fidelity results will be added as sketch implementations are integrated in subsequent PRs.
