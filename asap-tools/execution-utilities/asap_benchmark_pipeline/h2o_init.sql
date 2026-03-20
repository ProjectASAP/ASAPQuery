DROP VIEW IF EXISTS h2o_groupby_mv;
DROP TABLE IF EXISTS h2o_groupby;
DROP TABLE IF EXISTS h2o_groupby_queue;

CREATE TABLE IF NOT EXISTS h2o_groupby_queue
(
    timestamp String,
    id1 String,
    id2 String,
    id3 String,
    id4 Int32,
    id5 Int32,
    id6 Int32,
    v1 Int32,
    v2 Int32,
    v3 Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092',
         kafka_topic_list = 'h2o_groupby',
         kafka_group_name = 'clickhouse_h2o',
         kafka_format = 'JSONEachRow';

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
) ENGINE = MergeTree
ORDER BY (id1, id2, id3, id4);

CREATE MATERIALIZED VIEW IF NOT EXISTS h2o_groupby_mv TO h2o_groupby AS
SELECT parseDateTimeBestEffort(timestamp) AS timestamp, id1, id2, id3, id4, id5, id6, v1, v2, v3
FROM h2o_groupby_queue;