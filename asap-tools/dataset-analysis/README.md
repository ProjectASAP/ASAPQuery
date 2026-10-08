# Dataset skew analysis

Measures how skewed real observability traces are, so sketch benchmarks can
be run over a realistic range of skew rather than a single guessed value.

Two tasks:

1. **Label query sets per dataset.** `queries/*.yaml` lists PromQL queries
   over each trace, each in an instant form (`sum by (user) (cpu_rate)`)
   and/or a range form (`sum by (user) (sum_over_time(cpu_rate[5m]))`),
   with the table, group-by labels and value column it reads.
2. **Lower / MLE / upper skew per distribution.** `fit_skew.py` fits
   - a discrete Zipf exponent θ to the rank-frequency of every key query's
     group-by key (weighted by row count and by value sum), and
   - a continuous power-law tail exponent α to every value query's column,

   once on all data and once per evaluation time, the way Prometheus would
   evaluate the query every `step_s` seconds.

## Running

```bash
pip install -r requirements.txt
./fetch_data.sh /path/to/trace-data        # ~207 GB; re-run to resume
python fit_skew.py --data-root /path/to/trace-data
```

This writes `results/skew_summary.csv` (committed) and plots to `out/`
(gitignored). Useful flags:

- `--queries queries/google_2011.yaml`: run a subset of datasets.
- `--max-rows 300000`: rows read per file (BOOM: time steps per series)
  for a quick smoke run.
- `--no-plots`, `--out DIR`, `--summary PATH`.
- `--min-eval-rows` (default 200) and `--min-eval-keys` (default 2):
  evaluations below either threshold are skipped when computing the bounds.
- `--workers N`: process pool size (default: all cores). Files and fits run
  in parallel.

With 48 workers on a 56-core, 251 GB machine, Google takes about 1 hour
(peak RSS of the main process 45 GB) and Alibaba about 10.5 hours (98 GB;
CallGraph's 6h windows over high-cardinality keys dominate). Run the two
datasets on separate machines with `--queries` to overlap them. Alibaba archives are streamed with `tarfile`, not extracted.

Tests: `python -m unittest discover -s tests -p 'test_*.py'`.

## Datasets

| Dataset | Files | Tables | Step | Ranges |
|---|---|---|---|---|
| Google ClusterData 2011-2 | `task_usage` and `task_events` parts 0..119 of 500 (about 7 days), all 500 `job_events` parts, `schema.csv` | `task_usage` joined with task and job attributes | 5 min | instant, 5m, 1h, 6h, 24h |
| Alibaba microservices v2022 | `MCRRTUpdate_0..479` (first day) | `MSRTMCR` | 1 min | instant, 5m, 1h, 6h, 24h |
| | `CallGraph_0..479` (first day) | `CallGraph` | 1 min | 1m, 5m, 1h, 6h, 24h (events, no instant) |
| | `MSMetricsUpdate_0..47`, `NodeMetricsUpdate_0..1` (first day) | `MSMetrics`, `NodeMetrics` | 1 min | instant, 5m, 1h, 6h, 24h |
| Datadog BOOM | `dataset_taxonomy.json` and 20 multivariate series | per-series `target` | none | 20 equal chunks per series |

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
  which. An evaluation needs at least 200 positive values to be fittable.
- **mle**: the estimate on all data pooled.
- **lower / upper**: min / max over the set {every per-evaluation estimate that
  passes the thresholds, mle}, so `lower <= mle <= upper` always. The pooled
  and per-evaluation fits have no fixed order: pooling adds rare keys to the tail
  and accumulates mass on persistent heavy keys, which usually steepens the
  pooled rank-frequency (most CallGraph queries), but it can also flatten it.
- **Evaluation**: each table has a `step_s` (its sampling period). A row
  at time `t` belongs to step `ceil(t / step_s)`, so the evaluation at step
  `w` (time `w * step_s`) sees rows in `((w - 1) * step_s, w * step_s]`.
  Every query is evaluated at every step, as in a Prometheus range query
  with that step:
  - **range** (`5m`, `1h`, ...; a multiple of `step_s`): the rows of the
    last `range / step_s` steps, i.e. a sliding window, not disjoint
    windows. Only evaluations whose whole range lies in the data are used.
    Key queries sum the per-step per-key aggregates; value queries merge
    per-step uniform samples (at most 100,000 each) into a uniform sample
    of the range.
  - **instant**: one row per series (the table's `series_key`), its latest
    sample at or before the evaluation time and at most 5 minutes old (the
    Prometheus lookback; one step if `step_s` is longer). A series that
    stops is still seen for up to 5 minutes. Needs files that are
    consecutive time chunks; the run fails if a series' samples interleave
    across files. Event tables (CallGraph) have no series and only range
    forms.

  mle pools every row the query form sees: all rows for range forms, and
  every (series, evaluation) pair for the instant form, so `rows_total` of
  an instant row counts a series once per evaluation it is visible in.
  BOOM has no timestamps in this analysis and keeps its 20 equal chunks per
  series (`range` is empty).
- **tail_class**: the power law is compared with an exponential and a
  lognormal (`distribution_compare`, likelihood ratio `R` and p-value `p`;
  significant means `p < 0.1`). `light` if the power law does not
  significantly beat the exponential (`R > 0`); otherwise `power_law` if it
  also significantly beats the lognormal, `lognormal` if it is significantly
  worse than the lognormal (`R < 0`), else `heavy_inconclusive`. α is always
  reported. Power law and lognormal are often indistinguishable: a lognormal
  with large σ is nearly straight on a log-log plot over several decades, so
  the likelihood-ratio test usually cannot separate them without far more
  tail data than a trace provides (Clauset, Shalizi and Newman, "Power-law
  distributions in empirical data", SIAM Review 2009). Even an exact
  Pareto(α=2) sample of 20,000 values comes out `heavy_inconclusive` here.

## Output: `results/skew_summary.csv`

One row per (query, kind, weight, range). The sketch-bench saturation study
reads the worst case per query: `worst_theta_cms` (key rows: the lowest θ of
the weight a per-key counter sketch sees, `cms_weight`), `worst_K`,
`min_N` and `max_N` for the key cardinality and stream size an evaluation
sees, and `worst_alpha_rank` / `worst_alpha_memory` for value rows, plus the
`target_*` accuracy it has to reach. For
value rows it should use only rows whose `tail_class` is not `light`: a light tail decays at least exponentially, so its α is just the
slope of whatever sliver of the tail the fit picked and does not describe a
power-law regime.

| Column | Meaning |
|---|---|
| `dataset`, `query_id`, `promql` | query identity (a query with both kinds gets a `keys` and a `values` row) |
| `kind` | `keys` (θ) or `values` (α) |
| `weight` | `count` or `value` for key rows, empty for value rows |
| `range`, `range_s` | `instant` or the range duration (`range_s` empty for instant and BOOM) |
| `step_s` | evaluation step of the table |
| `K_total` | distinct keys with positive weight over the whole sample (BOOM: variates) |
| `rows_total` | rows with non-null keys over the whole sample (values: finite values; instant: series-evaluations) |
| `K_win_min`, `K_win_median`, `K_win_max` | key rows: distinct keys per evaluation |
| `rows_win_min`, `rows_win_median`, `rows_win_max` | rows per evaluation (value rows: positive values; empty for BOOM) |
| `n_evals` | evaluations used for the bounds (BOOM: variate-chunk fits) |
| `lower`, `mle`, `upper` | θ or α as defined above |
| `worst_theta_cms` | key rows whose weight is the query's `cms_weight`: `lower` |
| `worst_K`, `min_N`, `max_N` | `K_win_max`, `rows_win_min`, `rows_win_max` |
| `worst_alpha_rank`, `worst_alpha_memory` | value rows: `upper` (steepest tail, hardest for rank error) and `lower` (heaviest tail, largest value range) |
| `target_are_top100`, `target_precision_at_k`, `target_hll_rel_err`, `target_rank_err` | accuracy targets (defaults in `fit_skew.py`, overridden by a query's `targets`) |
| `top1_share`, `theta_ls` | key rows: share of the largest key, negated log-log least-squares slope |
| `dropped_frac` | value rows: fraction of finite values that were ≤ 0 |
| `xmin`, `ks_d`, `tail_frac` | value rows: fitted `xmin`, KS distance of the tail, and fraction of the fitted sample at or above `xmin` |
| `R_lognormal`, `p_lognormal`, `R_exponential`, `p_exponential` | log-likelihood ratio (power law vs alternative) and its p-value |
| `tail_class` | see Definitions (BOOM: majority class over variates) |
| `ok_frac` | BOOM rows: share of variates whose `tail_class` is not `light` |

Plots in `out/`: `<dataset>__<query>__rank_<weight>__<range>.png` (log-log
rank-frequency with the lower/mle/upper θ lines) and
`<dataset>__<query>__ccdf__<range>.png` (empirical CCDF with the fitted α).

## Caveats

- **θ depends on the query form.** An instant query sees one sample per
  series, so its count-weighted θ measures how series spread over keys; a
  range query sees every row in its range. Short ranges see fewer keys with
  noisier counts; long ones pool more keys. Use the row whose `range`
  matches the query.

- **BOOM** strips tags and z-scores each variate, so there is no key θ and
  the raw value scale is lost. α is fitted per variate on `x - min(x)`
  (zeros dropped), and each variate gets its own `tail_class`. The series
  row reports the majority class and `ok_frac`, the share of variates that
  are not `light`. lower/mle/upper are medians over the non-light variates of
  each variate's lower, mle and upper, and the diagnostics (`xmin`, `R`,
  `p`, ...) are medians over the same variates; all are empty if every
  variate is light.
- **Google join rule**: `task_usage` rows get `user`, `priority` and
  `scheduling_class` from the last non-null value for the same
  `(job_id, task_index)` over all 120 fetched `task_events` parts, and `logical_job_name` from the last non-null
  `job_events` value for the same `job_id` (both ordered by event time).
- **Small counts bias θ upward**: θ is fitted to the *sorted observed*
  counts, so the long tail of keys seen once or twice is flatter than the
  true law and the order statistics exaggerate the head. Evaluations with few
  rows per key (short ranges, high-cardinality keys) are most affected, which
  widens `upper`.
- Google `task_usage` rows measure 5-minute intervals and are stamped at
  their `end_time`. CallGraph has malformed rows
  (extra fields), which are skipped and counted in the log, and `rt` values
  of `None`, which are read as missing.
