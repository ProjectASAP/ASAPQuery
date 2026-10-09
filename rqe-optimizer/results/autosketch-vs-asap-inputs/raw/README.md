# Raw approxbench records of the study

These are the raw approxbench records (gzipped JSONL, one record per
benchmark run) behind `../saturation/`, from the sketch-bench study on
asap_sketchlib 0.3.0 run on 2026-10-07 with `scripts/study_saturation.py`
(sketch-bench main at the time, with #186's full-cross grid). The curves and
the cost table in `../saturation/` are reduced from them; the figures read
only those.

Rerunning them takes **0.77 h of machine time in all, run in parallel on 5
CloudLab machines** (Intel Xeon E5-2683 v3, 56 cores each), so about 20–30
minutes of wall time; the cost table alone, measured serially on an idle
machine, takes 17 minutes.

Every accuracy run is `study_saturation.py --phase accuracy --seeds 3` plus:

| File | Machine | Arguments | Records | Run time | Feeds |
|---|---|---|---|---|---|
| `grid_topk.saturation_accuracy.jsonl.gz` | clnode155 | `--n-max 1e7 --jobs 48 --families topk` (then the K = 1e4 column refilled for #186) | 13,056 | 98 s + 42 s | `out_grid_1e7_cost/` |
| `grid_card.saturation_accuracy.jsonl.gz` | clnode178 | `--n-max 1e7 --jobs 48 --families cardinality` (then the K = 1e4 column refilled) | 4,896 | 100 s + 43 s | `out_grid_1e7_cost/` |
| `grid_quantile.saturation_accuracy.jsonl.gz` | clnode178 | `--n-max 1e7 --jobs 48 --families quantile --merge-shards-list 1,4,16,64` | 5,712 | 32 s | `out_grid_1e7_cost/` |
| `kll_a2.saturation_accuracy.jsonl.gz` | clnode178 | `--no-cost-shape --families quantile --alphas 2 --n-max 3.16e7 --jobs 24`, KLL k ∈ {50, 200, 800}, `--merge-shards-list 1,4,16,64,256,1024,4096,16384,65536,86400` | 1,710 | 47 s | `out_1e9/` |
| `kll_a3.saturation_accuracy.jsonl.gz` | clnode178 | as above with `--alphas 3 --n-max 3.16e8`, `--merge-shards-list 1,4,16,64,256,1024,3600,4096` | 1,656 | 362 s | `out_1e9/` |
| `dd_a2.saturation_accuracy.jsonl.gz` | clnode178 | `--no-cost-shape --families quantile --alphas 2 --n-max 3.16e7 --jobs 12`, DDSketch α ∈ {0.005, 0.01, 0.02, 0.05} | 228 | 12 s | `out_1e9/` |
| `dd_a3.saturation_accuracy.jsonl.gz` | clnode178 | as above with `--alphas 3 --n-max 3.16e8` | 276 | 146 s | `out_1e9/` |
| `topk_1e9_3.saturation_accuracy.jsonl.gz` | clnode138 | `--no-cost-shape --families topk --thetas 1.1 --cardinalities 10000 --n-max 1e9 --jobs 24`, rows = 3 | 300 | 389 s | `out_1e9/` |
| `topk_1e9_5.saturation_accuracy.jsonl.gz` | clnode167 | as above, rows = 5 | 300 | 437 s | `out_1e9/` |
| `optimizer_cost.rqe_atomic_costs_raw.jsonl.gz`, `optimizer_cost.rqe_atomic_costs_grid.jsonl.gz` | clnode109 (alone) | `--phase optimizer-cost --seeds 3` (cost and accuracy passes, sketches and exact aggregations, serially) | 188 | 1047 s | `optimizer_cost/` |

Run times are each file's first-to-last record timestamps; the refills ran
about 40 minutes after their grids and are counted separately.
