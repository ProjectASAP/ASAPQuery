"""Select an Alibaba-style subset (200 jobs x P tasks) from the deduped Google
2011 steps over a day window and measure value spread, Zipf skew and top-k
separability under two region/service groupings.

usage: analyze_subset.py SCRATCH DAY0 NDAYS THRESH P [--seed 862]
"""

import argparse
import json
import random
import duckdb
import numpy as np
from scipy.optimize import minimize_scalar

ZIPF_THETA_BOUNDS = (0.0, 5.0)


def zipf_mle(weights):  # copied from asap-tools/dataset-analysis/fit_skew.py
    w = np.sort(np.asarray(weights, dtype=float))[::-1]
    w = w[w > 0]
    if len(w) < 2:
        return float("nan")
    log_rank = np.log(np.arange(1, len(w) + 1))
    total, weighted_log_rank = w.sum(), (w * log_rank).sum()

    def nll(s):
        return s * weighted_log_rank + total * np.log(np.exp(-s * log_rank).sum())

    return float(minimize_scalar(nll, bounds=ZIPF_THETA_BOUNDS, method="bounded").x)


def topk_stats(totals, k):
    v = np.sort(totals)[::-1]
    return v[:k].sum() / v.sum(), (v[k - 1] - v[k]) / v[k - 1]


def cyclic_window_sums(m, w):
    """m: [groups, T]; sums over every cyclic window of w steps -> [groups, T]."""
    T = m.shape[1]
    reps, rem = divmod(w, T)
    ext = np.concatenate([m, m[:, :rem]], axis=1) if rem else m
    cs = np.concatenate([np.zeros((m.shape[0], 1)), np.cumsum(ext, axis=1)], axis=1)
    part = cs[:, rem : rem + T] - cs[:, :T] if rem else 0.0
    return reps * m.sum(axis=1, keepdims=True) + part


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("scratch")
    ap.add_argument("day0", type=int)
    ap.add_argument("ndays", type=int)
    ap.add_argument("thresh", type=float)
    ap.add_argument("P", type=int)
    ap.add_argument("--seed", type=int, default=862)
    ap.add_argument("--out")
    a = ap.parse_args()
    D = a.scratch
    s0, T = 2 + 288 * a.day0, 288 * a.ndays
    need = T if a.thresh >= 1 else int(a.thresh * T)
    c = duckdb.connect()
    c.execute("SET memory_limit='5GB'; SET threads=12")
    c.execute(
        f"""CREATE TABLE cov AS SELECT job, task, count(*) n FROM '{D}/steps.parquet'
                  WHERE step >= {s0} AND step < {s0 + T} GROUP BY ALL HAVING count(*) >= {need}"""
    )
    elig = [
        r[0]
        for r in c.sql(
            f"SELECT job FROM cov GROUP BY job HAVING count(*) >= {a.P} ORDER BY job"
        ).fetchall()
    ]
    chosen = random.Random(a.seed).sample(elig, 200)
    c.execute(
        "CREATE TABLE ch AS SELECT * FROM (SELECT unnest($1::BIGINT[]) job, generate_subscripts($1::BIGINT[], 1) - 1 AS idx)",
        [chosen],
    )
    c.execute(
        f"""CREATE TABLE sel AS SELECT job, task, idx, k FROM (SELECT cov.job, task, idx,
                  row_number() OVER (PARTITION BY cov.job ORDER BY task) - 1 AS k FROM cov JOIN ch USING (job)) WHERE k < {a.P}"""
    )
    users = dict(
        c.sql(
            f"SELECT job, user FROM '{D}/jobs.parquet' JOIN ch USING (job)"
        ).fetchall()
    )
    sclass = dict(
        c.sql(
            f"SELECT job, sclass FROM '{D}/jobs.parquet' JOIN ch USING (job)"
        ).fetchall()
    )
    rows = c.sql(
        f"""SELECT idx, k, step - {s0} AS t, cpu, mem FROM '{D}/steps.parquet' JOIN sel USING (job, task)
                     WHERE step >= {s0} AND step < {s0 + T}"""
    ).fetchnumpy()
    N = 200 * a.P
    leaf = rows["idx"].astype(np.int64) * a.P + rows["k"]
    out = {
        "window": f"days {a.day0}..{a.day0 + a.ndays - 1} (steps {s0}..{s0 + T - 1})",
        "T": T,
        "thresh": a.thresh,
        "P": a.P,
        "eligible_jobs": len(elig),
        "users_among_chosen": len(set(users.values())),
        "max_chosen_jobs_per_user": max(
            np.unique(list(users.values()), return_counts=True)[1].tolist()
        ),
        "users_with_ge10_chosen": int(
            (np.unique(list(users.values()), return_counts=True)[1] >= 10).sum()
        ),
        "sclass_counts": {
            str(k): v
            for k, v in zip(*np.unique(list(sclass.values()), return_counts=True))
        },
    }
    for metric in ("cpu", "mem"):
        m = np.full((N, T), np.nan)
        m[leaf, rows["t"]] = rows[metric]
        missing = np.isnan(m).mean()
        # forward-fill then back-fill missing steps (only when thresh < 1)
        for i in np.where(np.isnan(m).any(axis=1))[0]:
            r = m[i]
            idx = np.where(~np.isnan(r), np.arange(T), 0)
            np.maximum.accumulate(idx, out=idx)
            r[:] = r[idx]
            first = np.argmax(~np.isnan(r))
            r[np.isnan(r)] = r[first]
        v = m.ravel()
        res = {
            "missing_frac": missing,
            "min": v.min(),
            "p50": np.median(v),
            "p95": np.quantile(v, 0.95),
            "p99": np.quantile(v, 0.99),
            "max": v.max(),
            "zero_frac": (v == 0).mean(),
            "lt_1e-4_frac": (v < 1e-4).mean(),
            "lt_1e-6_frac": (v < 1e-6).mean(),
            "distinct_values": int(np.unique(v.astype(np.float32)).size),
            "series_constant_frac": (m.max(axis=1) == m.min(axis=1)).mean(),
            "median_series_cv": float(
                np.median(m.std(axis=1) / np.maximum(m.mean(axis=1), 1e-12))
            ),
        }
        leaf_tot = m.sum(axis=1)
        job_m = m.reshape(200, a.P, T).sum(axis=1)
        within = m.reshape(200, a.P, T).sum(axis=2)
        with np.errstate(invalid="ignore", divide="ignore"):
            res["within_job_cv_median"] = float(
                np.nanmedian(within.std(axis=1) / within.mean(axis=1))
            )
        res["zero_jobs"] = int((within.sum(axis=1) == 0).sum())
        res["zero_tasks"] = int((leaf_tot == 0).sum())
        res["min_nonzero"] = float(v[v > 0].min())
        res["frac_series_with_any_zero"] = float((m == 0).any(axis=1).mean())
        srt = -np.sort(-within, axis=1)  # per-job task totals, desc
        res["within_job_top1_vs_2_gap_median"] = float(
            np.nanmedian(
                (srt[:, 0] - srt[:, 1]) / np.where(srt[:, 0] > 0, srt[:, 0], np.nan)
            )
        )
        res["within_job_max_over_median_median"] = float(
            np.nanmedian(
                srt[:, 0]
                / np.where(np.median(srt, axis=1) > 0, np.median(srt, axis=1), np.nan)
            )
        )
        ej = c.sql(
            f"""SELECT sum({metric}) FROM '{D}/steps.parquet' JOIN (SELECT job, task FROM cov WHERE job IN (SELECT unnest($1::BIGINT[]))) USING (job, task)
                      WHERE step >= {s0} AND step < {s0 + T} GROUP BY job""",
            params=[elig],
        ).fetchnumpy()
        res["zipf_job_eligible_pool_all_full_tasks"] = zipf_mle(list(ej.values())[0])
        res["zipf_leaf"] = zipf_mle(leaf_tot)
        res["zipf_job"] = zipf_mle(job_m.sum(axis=1))
        groupings = {"random": np.arange(200)}
        order = sorted(range(200), key=lambda i: (users[chosen[i]], chosen[i]))
        pos = np.empty(200, dtype=int)
        pos[order] = np.arange(200)
        groupings["user_sorted"] = pos
        for gname, slot in groupings.items():
            # slot[i] = position 0..199 of chosen job i; region = pos//50, service = pos//10
            jm = np.zeros_like(job_m)
            jm[slot] = job_m
            sm = jm.reshape(20, 10, T).sum(axis=1)
            rm = jm.reshape(4, 50, T).sum(axis=1)
            g = {
                "zipf_service": zipf_mle(sm.sum(axis=1)),
                "zipf_region": zipf_mle(rm.sum(axis=1)),
            }
            for lvl, mat in (("rs", sm), ("rsj", jm)):
                for wname, w in (
                    ("full", None),
                    ("1m=600", 600),
                    ("5m=3000", 3000),
                    ("20m=12000", 12000),
                ):
                    sums = (
                        mat.sum(axis=1, keepdims=True)
                        if w is None
                        else cyclic_window_sums(mat, w)
                    )
                    full_top = {
                        k: set(np.argsort(-mat.sum(axis=1))[:k]) for k in (3, 5, 8)
                    }
                    for k in (3, 5, 8):
                        st = np.array(
                            [topk_stats(sums[:, t], k) for t in range(sums.shape[1])]
                        )
                        same = np.mean(
                            [
                                set(np.argsort(-sums[:, t])[:k]) == full_top[k]
                                for t in range(sums.shape[1])
                            ]
                        )
                        g[f"{lvl}/{wname}/k{k}"] = {
                            "share_med": float(np.median(st[:, 0])),
                            "gap_med": float(np.median(st[:, 1])),
                            "gap_min": float(st[:, 1].min()),
                            "gap_p10": float(np.quantile(st[:, 1], 0.1)),
                            "set_eq_full_frac": float(same),
                        }
            res[gname] = g
        out[metric] = res
    js = json.dumps(out, indent=1, default=float)
    print(js)
    if a.out:
        open(a.out, "w").write(js)


if __name__ == "__main__":
    main()
