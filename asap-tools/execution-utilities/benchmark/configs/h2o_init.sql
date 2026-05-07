-- ClickHouse init for H2O GroupBy baseline (MergeTree, direct load)
-- Use this with export_to_database.py --dataset h2o --init-sql-file
-- Source: asap_benchmark_pipeline/h2o_init.sql

DROP TABLE IF EXISTS h2o_groupby;

CREATE TABLE IF NOT EXISTS h2o_groupby
(
    timestamp DateTime,
    id1 String,
    id2 String,
    id3 String,
    id4 Int32,
    id5 Int32,
    id6 Int32,
    v1 Int32,
    v2 Int32,
    v3 Float64
) ENGINE = MergeTree()
ORDER BY (id1, id2);
