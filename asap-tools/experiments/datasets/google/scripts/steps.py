"""task_usage.parquet -> steps.parquet: one row per (job, task, 5-min step),
value = duration-weighted mean over the step's measurement rows (dedups
restarts / split windows). Also per-task coverage stats."""

import sys
import time
import duckdb

D = sys.argv[1]
con = duckdb.connect()
con.execute(
    f"SET memory_limit='5GB'; SET threads=12; SET preserve_insertion_order=false; SET temp_directory='{D}/tmp'"
)
t = time.time()
con.execute(
    f"""
COPY (SELECT job, task, (start_s // 300)::INTEGER AS step,
             (sum(cpu * greatest(end_s - start_s, 1)) / sum(greatest(end_s - start_s, 1)))::FLOAT AS cpu,
             (sum(mem * greatest(end_s - start_s, 1)) / sum(greatest(end_s - start_s, 1)))::FLOAT AS mem,
             count(*)::SMALLINT AS nrows
      FROM '{D}/task_usage.parquet' GROUP BY ALL)
TO '{D}/steps.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)"""
)
print("steps done", time.time() - t, flush=True)
print(
    con.sql(
        f"SELECT count(*), min(step), max(step), count(DISTINCT step) FROM '{D}/steps.parquet'"
    ).fetchall(),
    flush=True,
)
print(
    con.sql(
        f"SELECT nrows, count(*) FROM '{D}/steps.parquet' GROUP BY 1 ORDER BY 1 LIMIT 8"
    ).fetchall(),
    flush=True,
)
