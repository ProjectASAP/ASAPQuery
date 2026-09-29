# Dataset skew analysis

Measures how skewed real observability traces are, so sketch benchmarks can
be run over a realistic range of skew rather than a single guessed value.

Two tasks:

1. **Label query sets per dataset.** `queries/*.yaml` lists PromQL-style
   queries over each trace (`sum by (user) (cpu_rate)`,
   `count by (um, dm) (rt)`, `quantile(0.99, cpu_rate)`, ...), each with
   the table, group-by labels and value column it reads.
2. **Lower / MLE / upper skew per distribution.** `fit_skew.py` fits
   - a discrete Zipf exponent θ to the rank-frequency of every key query's
     group-by key (weighted by row count and by value sum), and
   - a continuous power-law tail exponent α to every value query's column,

   once on all data and once per time window, at several window lengths.

## Running

```bash
pip install -r requirements.txt
./fetch_data.sh /path/to/trace-data        # ~5 GB; re-run to resume
python fit_skew.py --data-root /path/to/trace-data
```

This writes `results/skew_summary.csv` (committed) and plots to `out/`
(gitignored). Useful flags:

- `--queries queries/google_2011.yaml`: run a subset of datasets.
- `--max-rows 300000`: rows read per file (BOOM: time steps per series)
  for a quick smoke run.
- `--no-plots`, `--out DIR`, `--summary PATH`.
- `--min-window-rows` (default 200) and `--min-window-keys` (default 2):
  windows below either threshold are skipped when computing the bounds.
- `--workers N`: process pool size (default: all cores). Files and fits run
  in parallel.

A full run over the fetched data takes about 5 minutes with 48 workers on a
56-core machine (peak RSS of the main process about 3.6 GB). Alibaba archives are streamed with `tarfile`, not extracted.

Tests: `python -m unittest discover -s tests -p 'test_*.py'`.

## Datasets

| Dataset | Files | Tables | Window lengths |
|---|---|---|---|
| Google ClusterData 2011-2 | `task_usage` and `task_events` part 0 of 500, all 500 `job_events` parts, `schema.csv` | `task_usage` joined with task and job attributes | 5, 15, 60 min |
| Alibaba microservices v2022 | `NodeMetricsUpdate_0`, `MSMetricsUpdate_0`, `CallGraph_0..9`, `MCRRTUpdate_0..9` (first 30 minutes) | `MSRTMCR`, `CallGraph`, `MSMetrics`, `NodeMetrics` | 1, 5, 30 min |
| Datadog BOOM | `dataset_taxonomy.json` and 20 multivariate series | per-series `target` | 20 equal chunks per series |

Citations:

```bibtex
@online{google-traces-2011,
  title={{Google ClusterData 2011 traces}},
  url={{https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md}},
  year={2025}
}

@inproceedings{luo2022Prediction,
  title={The Power of Prediction: Microservice Auto Scaling via Workload Learning},
  author={Luo, Shutian and Xu, Huanle and Ye, Kejiang and Xu, Guoyao and Zhang, Liping and Yang, Guodong and Xu, Chengzhong},
  booktitle={Proceedings of the ACM Symposium on Cloud Computing},
  year={2022}
}

@misc{cohen2025timedifferentobservabilityperspective,
  title={This Time is Different: An Observability Perspective on Time Series Foundation Models},
  author={Ben Cohen and Emaad Khwaja and Youssef Doubli and Salahidine Lemaachi and Chris Lettieri and Charles Masson and Hugo Miccinilli and Elise Ramé and Qiqi Ren and Afshin Rostamizadeh and Jean Ogier du Terrail and Anna-Monica Toon and Kan Wang and Stephan Xie and Zongzhe Xu and Viktoriya Zhukova and David Asker and Ameet Talwalkar and Othmane Abou-Amal},
  year={2025},
  eprint={2505.14766},
  archivePrefix={arXiv},
  primaryClass={cs.LG},
  url={https://arxiv.org/abs/2505.14766}
}
```

## Definitions

- **Key θ**: sort the per-key weights descending and fit the exponent `s` of
  a discrete Zipf over ranks `1..K` by maximum likelihood
  (`scipy.optimize.minimize_scalar`, bounded to [0, 5]). The weight is
  either the row count per key (`weight=count`) or the sum of the value per
  key with negatives clipped to 0 (`weight=value`).
- **Value α**: continuous power law fitted with `powerlaw.Fit` (pinned to
  1.5), with α the exponent of the pdf `p(x) ∝ x^-α` for `x ≥ xmin`.
  Non-positive values are dropped first, and each fit uses a uniform
  subsample of at most 100,000 values. `xmin` minimizes the KS distance over
  a grid of 50 quantiles from p50 to p99.9, restricted to candidates that
  keep at least 100 values in the tail. α therefore describes a tail between
  the top half and roughly the top 0.1% of the values; `tail_frac` says
  which. A window needs at least 200 positive values to be fittable.
- **mle**: the estimate on all data pooled.
- **lower / upper**: min / max over the set {every per-window estimate that
  passes the thresholds, mle}, so `lower <= mle <= upper` always. The pooled
  and per-window fits have no fixed order: pooling adds rare keys to the tail
  and accumulates mass on persistent heavy keys, which usually steepens the
  pooled rank-frequency (most CallGraph queries), but it can also flatten it.
- **Window lengths**: each dataset YAML lists `window_lengths_s`, finest
  first; every length must be a multiple of the finest. Data are read once
  and aggregated per finest window; coarser windows sum the finest per-key
  aggregates (key queries) or concatenate the finest windows' values before
  subsampling (value queries). mle does not depend on the window length.
  BOOM has no timestamps in this analysis and keeps its 20 equal chunks per
  series (`window_len_s` is empty).
- **power_law_ok / best_alt**: the power law is compared with a lognormal
  and an exponential (`distribution_compare`, likelihood ratio `R` and
  p-value `p`). `power_law_ok` is true only if the power law significantly
  beats both (`R > 0` and `p < 0.1` for each). Otherwise `best_alt` names the
  alternative that is significantly better with the most negative `R`, or
  `inconclusive` if neither is; α is still reported. `best_alt` is empty
  when `power_law_ok` is true. A lognormal with large σ mimics a power-law
  tail, so this rule rarely passes: even an exact Pareto(α=2) sample comes out
  `inconclusive` (see the tests).

## Output: `results/skew_summary.csv`

One row per (query, kind, weight, window length). The sketch-bench saturation study reads
`dataset, query_id, kind, weight, window_len_s, lower, mle, upper` to pick the θ and α
range it sweeps.

| Column | Meaning |
|---|---|
| `dataset`, `query_id`, `promql` | query identity (a query with both kinds gets a `keys` and a `values` row) |
| `kind` | `keys` (θ) or `values` (α) |
| `weight` | `count` or `value` for key rows, empty for value rows |
| `window_len_s` | window length the bounds were computed at (empty for BOOM) |
| `K` | distinct keys with positive weight (BOOM: variates) |
| `rows` | rows with non-null keys (values: finite values) |
| `n_windows` | windows used for the bounds (BOOM: variate-chunk fits) |
| `lower`, `mle`, `upper` | θ or α as defined above |
| `top1_share`, `theta_ls` | key rows: share of the largest key, negated log-log least-squares slope |
| `dropped_frac` | value rows: fraction of finite values that were ≤ 0 |
| `xmin`, `ks_d`, `tail_frac` | value rows: fitted `xmin`, KS distance of the tail, and fraction of the fitted sample at or above `xmin` |
| `R_lognormal`, `p_lognormal`, `R_exponential`, `p_exponential` | log-likelihood ratio (power law vs alternative) and its p-value |
| `power_law_ok`, `best_alt` | see Definitions |
| `ok_frac` | BOOM rows: share of variates with `power_law_ok` |

Plots in `out/`: `<dataset>__<query>__rank_<weight>__<window_len>s.png` (log-log
rank-frequency with the lower/mle/upper θ lines, one per window length) and
`<dataset>__<query>__ccdf.png` (empirical CCDF with the fitted α).

## Caveats

- **θ depends on time scale.** A sketch sees the keys of one window of
  length x, while a query aggregates over its lookback S (often many
  windows). Short windows see fewer keys with noisier counts; long ones pool
  more keys. Pick the row whose `window_len_s` matches the sketch window, and
  compare it with mle (all data) for long lookbacks.

- **BOOM** strips tags and z-scores each variate, so there is no key θ and
  the raw value scale is lost. α is fitted per variate on `x - min(x)`
  (zeros dropped). `ok_frac` is the share of compared variates with
  `power_law_ok`. If `ok_frac >= 0.5`, the series row reports the medians
  over the passing variates of each variate's lower, mle and upper, and the
  diagnostics (`xmin`, `R`, `p`, ...) are medians over the same variates. If
  `ok_frac < 0.5`, lower/mle/upper are empty, `power_law_ok` is false,
  `best_alt` is the most common `best_alt` among the failing variates, and the
  diagnostics are medians over the failing variates.
- **Google join rule**: `task_usage` rows get `user`, `priority` and
  `scheduling_class` from the last non-null `task_events` value for the same
  `(job_id, task_index)`, and `logical_job_name` from the last non-null
  `job_events` value for the same `job_id` (both ordered by event time).
- **Small counts bias θ upward**: θ is fitted to the *sorted observed*
  counts, so the long tail of keys seen once or twice is flatter than the
  true law and the order statistics exaggerate the head. Windows with few
  rows per key (short windows, high-cardinality keys) are most affected, which
  widens `upper`.
- Alibaba tables are clipped to the first 30 minutes (`max_time_secs`), the
  span of the 10 CallGraph / MCRRTUpdate shards. CallGraph has malformed rows
  (extra fields), which are skipped and counted in the log, and `rt` values
  of `None`, which are read as missing.
