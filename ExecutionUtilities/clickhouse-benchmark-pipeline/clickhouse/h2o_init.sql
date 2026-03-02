CREATE TABLE IF NOT EXISTS h2o_groupby_queue
(
    timestamp BIGINT,
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
    timestamp BIGINT,
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
SELECT * FROM h2o_groupby_queue;