#!/usr/bin/env python3
"""A replay table for paced-feeder from Google ClusterData 2011-2 task_usage:
task CPU rate in the random workloads' label shape (4 region x 5 service x 10
job x 50 label_0), so their plans apply unchanged.

The Google counterpart of asap-tools/experiments/datasets/alibaba/make_replay.py,
with the same outputs and label convention (a child's value is its parent's, a
`.`, and its index), and these differences:
- input is the headerless task_usage CSV.gz parts (read with DuckDB) plus
  job_events for each job's user; a step is `start_time // 300 s`;
- a series is a Google task (job ID, task index), not a machine; rows of one
  task in one step (restarts, migrations) are merged by a duration-weighted
  mean of CPU rate;
- job = Google job (drawn by `--seed` among jobs with >= 50 tasks reporting at
  every step, sorted as integers; tasks are their first 50 by index);
- region/service are blocks of 50/10 of the chosen jobs ordered by (user, job),
  so one user's jobs share services;
- values are CPU rate clipped at 1.0, times `--scale` (1e6), rounded to 2
  decimals: Count-Min top-k counters round to integers, and x1e4 would zero
  about 6% of samples.

Writes `<out>/replay_table_google.json` (the workload table with `data`'s
values replaced by `{"replay": {"steps", "values"}}`, series-major in
paced-feeder's series order: mixed radix over region, service, job, label_0),
`<out>/replay_mapping.csv`, and `<out>/replay.csv`
(`offset_ms,region,service,job,label_0,value`, sorted by offset).

Usage (DuckDB from baselines/PilotDB/.venv):
  python -I make_google_replay.py --trace .../clusterdata-2011-2 \
      --table .../seed-862-dashboard/workload-01-pilot/eval_table.json --out DIR
"""

import argparse
import csv
import json
import os
import random
import tempfile

import duckdb
import numpy as np

REGIONS, SERVICES, JOBS, PODS = 4, 5, 10, 50
STEP_US = 300_000_000
FIRST_STEP, STEPS = 7490, 864  # trace days 26..28
FIRST_PART, LAST_PART = 448, 499  # task_usage parts covering those steps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", required=True, help="clusterdata-2011-2 directory")
    parser.add_argument(
        "--table", required=True, help="workload table whose `data` gets the replay"
    )
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=862)
    parser.add_argument("--scale", type=float, default=1e6)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)

    con = duckdb.connect()
    tmp = tempfile.mkdtemp(dir=args.out, prefix="duckdb-tmp-")
    con.execute(
        f"SET memory_limit='{args.memory_limit}'; SET threads={args.threads}; SET temp_directory='{tmp}'; "
        "SET preserve_insertion_order=false; SET enable_progress_bar=false"
    )
    parts = [
        f"{args.trace}/task_usage/part-{p:05d}-of-00500.csv.gz"
        for p in range(FIRST_PART, LAST_PART + 1)
    ]
    lo, hi = FIRST_STEP * STEP_US, (FIRST_STEP + STEPS) * STEP_US
    con.execute(
        f"""
        CREATE TABLE cells AS
        SELECT job, task, (start_us // {STEP_US} - {FIRST_STEP})::SMALLINT AS t,
               -- exact DECIMAL sums keep the mean independent of thread order
               sum(cpu::DECIMAL(18, 12) * w)::DOUBLE / sum(w)::DOUBLE AS cpu
        FROM (SELECT column0 AS start_us, column2 AS job, column3 AS task, column5 AS cpu,
                     greatest(column1 - column0, 1000000) AS w
              FROM read_csv({parts!r}, header=false, auto_detect=false,
                            columns={{'column0':'BIGINT','column1':'BIGINT','column2':'BIGINT','column3':'INTEGER',
                                     'column4':'BIGINT','column5':'DOUBLE','column6':'VARCHAR','column7':'VARCHAR',
                                     'column8':'VARCHAR','column9':'VARCHAR','column10':'VARCHAR','column11':'VARCHAR',
                                     'column12':'VARCHAR','column13':'VARCHAR','column14':'VARCHAR','column15':'VARCHAR',
                                     'column16':'VARCHAR','column17':'VARCHAR','column18':'VARCHAR','column19':'VARCHAR'}}))
        WHERE start_us >= {lo} AND start_us < {hi}
        GROUP BY ALL"""
    )
    con.execute(
        f"CREATE TABLE full_tasks AS SELECT job, task FROM cells GROUP BY ALL HAVING count(*) = {STEPS}"
    )
    eligible = [
        j
        for (j,) in con.execute(
            f"SELECT job FROM full_tasks GROUP BY job HAVING count(*) >= {PODS} ORDER BY job"
        ).fetchall()
    ]
    jobs = REGIONS * SERVICES * JOBS
    print(f"{STEPS} steps; {len(eligible)} jobs eligible")
    chosen = random.Random(args.seed).sample(eligible, jobs)

    users = dict(
        con.execute(
            f"""
        SELECT column2, arg_max(column4, column0) FILTER (WHERE column4 IS NOT NULL)
        FROM read_csv('{args.trace}/job_events/part-*-of-00500.csv.gz', header=false, auto_detect=false,
                      columns={{'column0':'BIGINT','column1':'VARCHAR','column2':'BIGINT','column3':'INTEGER',
                               'column4':'VARCHAR','column5':'VARCHAR','column6':'VARCHAR','column7':'VARCHAR'}})
        WHERE column2 IN (SELECT unnest($chosen)) GROUP BY 1""",
            {"chosen": chosen},
        ).fetchall()
    )
    ordered = sorted(chosen, key=lambda j: (users[j], j))

    # series index in paced-feeder's order: ((i_job * PODS) + k), i_job = r*50 + s*10 + j
    mapping = []  # (region, service, job, label_0, job_id, task, user, series)
    for i, job_id in enumerate(ordered):
        region = str(i // (SERVICES * JOBS))
        service = f"{region}.{i // JOBS % SERVICES}"
        job = f"{service}.{i % JOBS}"
        tasks = [
            t
            for (t,) in con.execute(
                f"SELECT task FROM full_tasks WHERE job = ? ORDER BY task LIMIT {PODS}",
                [job_id],
            ).fetchall()
        ]
        for k, task in enumerate(tasks):
            mapping.append(
                (
                    region,
                    service,
                    job,
                    f"{job}.{k}",
                    job_id,
                    task,
                    users[job_id],
                    i * PODS + k,
                )
            )
    with open(f"{args.out}/replay_mapping.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "region",
                "service",
                "job",
                "label_0",
                "google_job_id",
                "task_index",
                "user",
            ]
        )
        w.writerows(m[:7] for m in mapping)

    con.execute("CREATE TABLE sel (job BIGINT, task INTEGER, series INTEGER)")
    con.executemany(
        "INSERT INTO sel VALUES (?, ?, ?)", [(m[4], m[5], m[7]) for m in mapping]
    )
    got = con.execute(
        f"""SELECT series, t, round(least(cpu, 1.0) * {args.scale}, 2) AS v
                          FROM cells JOIN sel USING (job, task)"""
    ).fetchnumpy()
    n = len(mapping)
    values = np.full((n, STEPS), np.nan)
    values[got["series"], got["t"]] = got["v"]
    assert n == REGIONS * SERVICES * JOBS * PODS and not np.isnan(values).any()

    labels = {m[7]: m[:4] for m in mapping}
    with open(f"{args.out}/replay.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["offset_ms", "region", "service", "job", "label_0", "value"])
        for t in range(STEPS):
            for s in range(n):
                w.writerow([t * STEP_US // 1000, *labels[s], values[s, t]])

    table = json.load(open(args.table))
    data = table["workloads"][0]["e2e"]["metrics"]["data"]
    data["values"] = {"replay": {"steps": STEPS, "values": values.ravel().tolist()}}
    with open(f"{args.out}/replay_table_google.json", "w") as f:
        json.dump(table, f)
    con.close()
    os.rmdir(tmp) if not os.listdir(tmp) else None
    print(f"{n} series x {STEPS} steps -> {args.out}/replay_table_google.json")


if __name__ == "__main__":
    main()
