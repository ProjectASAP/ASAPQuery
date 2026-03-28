import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

row_limit = 5000000
df = pd.read_csv("../clickhouse-benchmark-pipeline/G1_1e7_1e2_0_0.csv").head(row_limit)

base_ts = int(pd.Timestamp("2024-01-01", tz="UTC").timestamp() * 1000)
timestamps = [base_ts + i * 10 for i in range(len(df))]

schema = pa.schema(
    [
        ("timestamp", pa.timestamp("ms")),
        ("v1", pa.float64()),
        ("v2", pa.float64()),
        ("v3", pa.float64()),
        ("id1", pa.string()),
        ("id2", pa.string()),
        ("id3", pa.string()),
        ("id4", pa.string()),
        ("id5", pa.string()),
        ("id6", pa.string()),
    ]
)

table = pa.table(
    {
        "timestamp": pa.array(timestamps, type=pa.timestamp("ms")),
        "v1": pa.array(df["v1"].astype(float), type=pa.float64()),
        "v2": pa.array(df["v2"].astype(float), type=pa.float64()),
        "v3": pa.array(df["v3"].astype(float), type=pa.float64()),
        "id1": pa.array(df["id1"].astype(str), type=pa.string()),
        "id2": pa.array(df["id2"].astype(str), type=pa.string()),
        "id3": pa.array(df["id3"].astype(str), type=pa.string()),
        "id4": pa.array(df["id4"].astype(str), type=pa.string()),
        "id5": pa.array(df["id5"].astype(str), type=pa.string()),
        "id6": pa.array(df["id6"].astype(str), type=pa.string()),
    },
    schema=schema,
)

pq.write_table(table, "h2o_file.parquet")
print("Done:", len(df), "rows")
