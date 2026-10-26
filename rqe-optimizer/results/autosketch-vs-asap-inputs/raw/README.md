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

## The denser grid (#213, measured 2026-10-13/14 UTC)

Only the new configs, with the same arguments as the rows above plus
`--sketches`/`--configs`; accuracy runs on clnode130 (the same Xeon
E5-2683 v3 node type), cost rows serially on idle clnode138, where the
existing cost rows were measured.

| File | Arguments | Feeds |
|---|---|---|
| `dense_grid_q.saturation_accuracy.jsonl.gz` | as `grid_quantile`, `--sketches kll-percall,dd --configs k=25,k=100,k=400,alpha=0.1` | `out_grid_1e7_cost/` |
| `dense_kll_a2…`, `dense_kll_a3…` | as `kll_a2`, `kll_a3`, `--sketches kll-percall --configs k=25,k=100,k=400` | `out_1e9/` |
| `dense_dd_a2…`, `dense_dd_a3…` | as `dd_a2`, `dd_a3`, `--sketches dd --configs alpha=0.1` | `out_1e9/` |
| `dense_optimizer_cost_q.rqe_atomic_costs_raw.jsonl.gz` | `--phase optimizer-cost --seeds 3 --families quantile --sketches kll-percall,dd --configs k=25,k=100,k=400,alpha=0.1` | the cost table's 4 new rows |
| `dense_optimizer_cost_h.rqe_atomic_costs_raw.jsonl.gz` | `--phase optimizer-cost --seeds 3 --families hydra --sketches hydra-hll,hydra-kll --hydra-variants hydra-hll,hydra-kll --hydra-ws 65536` (4-shard merges) | the cost table's 3 new Hydra rows |

The Hydra W = 65536 accuracy rows (999 runs, `--phase hydra --hydra-sweeps eval
--hydra-ws 65536 --hydra-variants hydra-hll,hydra-kll --hydra-shard i/4` on
clnode109/155/167/178) are appended to
`../../autosketch-vs-asap-hydra/inputs/saturation/hydra_saturation.csv`.
