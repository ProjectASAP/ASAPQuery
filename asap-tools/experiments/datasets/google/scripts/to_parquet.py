"""task_usage CSV.gz -> parquet (needed columns only), plus job_events -> jobs.parquet.

Usage: python3 to_parquet.py .../clusterdata-2011-2 OUT_DIR"""

import sys
import time
import duckdb

G, OUT = sys.argv[1], sys.argv[2]
con = duckdb.connect()
con.execute(
    "SET memory_limit='5GB'; SET threads=12; SET preserve_insertion_order=false"
)
con.execute(f"SET temp_directory='{OUT}/tmp'")
cols = ", ".join(
    f"'c{i}': '{t}'"
    for i, t in enumerate(
        [
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "INTEGER",
            "BIGINT",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "INTEGER",
            "DOUBLE",
        ],
        1,
    )
)
t = time.time()
con.execute(
    f"""
COPY (SELECT CAST(c1/1000000 AS INTEGER) AS start_s, CAST(c2/1000000 AS INTEGER) AS end_s,
             c3 AS job, c4 AS task, c5 AS machine, c6 AS cpu, c7 AS mem, c8 AS amem, c14 AS max_cpu, c19 AS agg
      FROM read_csv('{G}/task_usage/part-*-of-00500.csv.gz', header=false, columns={{{cols}}}, filename=true))
TO '{OUT}/task_usage.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)"""
)
print("task_usage done", time.time() - t, flush=True)
jcols = "{'time':'BIGINT','missing':'INTEGER','job':'BIGINT','etype':'INTEGER','user':'VARCHAR','sclass':'INTEGER','jname':'VARCHAR','lname':'VARCHAR'}"
con.execute(
    f"""COPY (SELECT job, arg_max(user, time) FILTER (WHERE user IS NOT NULL) AS user,
   arg_max(lname, time) FILTER (WHERE lname IS NOT NULL) AS lname, arg_max(sclass, time) AS sclass
   FROM read_csv('{G}/job_events/part-*.csv.gz', header=false, columns={jcols}) GROUP BY job)
   TO '{OUT}/jobs.parquet' (FORMAT parquet)"""
)
print("jobs done", time.time() - t, flush=True)
