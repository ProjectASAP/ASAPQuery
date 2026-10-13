DROP TABLE IF EXISTS fake_metric_total;

CREATE TABLE fake_metric_total
(
    ts DateTime,
    instance LowCardinality(String),
    job LowCardinality(String),
    label_0 LowCardinality(String),
    label_1 LowCardinality(String),
    label_2 LowCardinality(String),
    value Float64
)
ENGINE = MergeTree
ORDER BY (instance, label_0, label_1, label_2, ts);
