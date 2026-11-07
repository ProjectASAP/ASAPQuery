import sys
import duckdb

D = sys.argv[1]
c = duckdb.connect()
c.execute(f"SET memory_limit='5GB'; SET threads=12; SET temp_directory='{D}/tmp'")
# day d covers steps [2+288d, 2+288(d+1)); 29 days exactly = 8352 steps
c.execute(
    f"COPY (SELECT job, task, ((step-2)//288)::TINYINT AS dy, count(*)::SMALLINT n FROM '{D}/steps.parquet' GROUP BY ALL) TO '{D}/task_day.parquet' (FORMAT parquet)"
)
c.execute(f"CREATE TABLE td AS SELECT * FROM '{D}/task_day.parquet'")


def elig(d0, L, th):
    need = int(th * 288 * L + 1e-9) if th < 1 else 288 * L
    return c.sql(
        f"""SELECT count(*) FILTER (WHERE f>=25), count(*) FILTER (WHERE f>=50), count(*) FILTER (WHERE f>=100) FROM
      (SELECT job, count(*) f FROM (SELECT job, task, sum(n) s FROM td WHERE dy >= {d0} AND dy < {d0+L} GROUP BY ALL) WHERE s >= {need} GROUP BY job)"""
    ).fetchone()


print("single days, strict: day -> jobs with >=25/50/100 full tasks")
for d in range(29):
    print(d, elig(d, 1, 1.0), flush=True)
for L in (2, 3, 7, 14, 29):
    for th in (1.0, 0.99):
        print(f"days 0..{L} th={th}", elig(0, L, th), flush=True)
