import sys
import duckdb

D = sys.argv[1]
con = duckdb.connect()
con.execute(f"SET memory_limit='5GB'; SET threads=12; SET temp_directory='{D}/tmp'")
W = {"h1": (2, 14), "d1": (2, 290), "d7": (2, 2018), "d29": (2, 8354)}
aggs = ", ".join(
    f"count(*) FILTER (WHERE step >= {a} AND step < {b}) AS n_{k}"
    for k, (a, b) in W.items()
)
con.execute(
    f"COPY (SELECT job, task, {aggs} FROM '{D}/steps.parquet' GROUP BY ALL) TO '{D}/task_cov.parquet' (FORMAT parquet)"
)
full = ", ".join(
    f"count(*) FILTER (WHERE n_{k} = {b - a}) AS full_{k}" for k, (a, b) in W.items()
)
con.execute(
    f"COPY (SELECT job, count(*) AS ntasks, {full} FROM '{D}/task_cov.parquet' GROUP BY job) TO '{D}/job_cov.parquet' (FORMAT parquet)"
)
print(
    con.sql(
        f"SELECT count(*) tasks, count(DISTINCT job) jobs FROM '{D}/task_cov.parquet'"
    ).fetchall()
)
for k in W:
    print(
        k,
        con.sql(
            f"""SELECT count(*) FILTER (WHERE full_{k} >= 1) AS jobs_any, count(*) FILTER (WHERE full_{k} >= 50) AS jobs50,
        count(DISTINCT j.user) FILTER (WHERE full_{k} >= 50) users50, sum(full_{k}) full_tasks
        FROM '{D}/job_cov.parquet' c JOIN '{D}/jobs.parquet' j USING (job)"""
        ).fetchall(),
    )
    print(
        "  users by #jobs50:",
        con.sql(
            f"""SELECT n, count(*) FROM (SELECT user, count(*) n FROM '{D}/job_cov.parquet' c JOIN '{D}/jobs.parquet' j USING (job)
        WHERE full_{k} >= 50 GROUP BY user) GROUP BY n ORDER BY n"""
        ).fetchall(),
    )
