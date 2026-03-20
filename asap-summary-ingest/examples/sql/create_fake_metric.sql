CREATE TABLE your_table (
  timestamp DOUBLE,
  value DOUBLE,
  metric_name TEXT,
  __name__ TEXT,
  instance TEXT,
  job TEXT,
  label_0 TEXT,
  label_1 TEXT,
  label_2 TEXT
) WITH (
  connector = 'filesystem',
  type = 'source',
  path = '/Users/milindsrivastava/Desktop/cmu/research/sketch_db_for_prometheus/code/arroyo_files/inputs/',
  format = 'json',
  'source.regex-pattern' = 'fake_metric_total_10\.json'
);
CREATE TABLE output_table (
  sums DOUBLE,
  instance TEXT,
  job TEXT,
  label_0 TEXT,
  label_1 TEXT,
  label_2 TEXT
) WITH (
  connector = 'filesystem',
  type = 'sink',
  path = '/Users/milindsrivastava/Desktop/cmu/research/sketch_db_for_prometheus/code/arroyo_files/outputs/',
  format = 'json'
);
INSERT INTO output_table
SELECT
  SUM(value) as sums,
  instance,
  job,
  label_0,
  label_1,
  label_2
FROM your_table
WHERE __name__ = 'fake_metric_total'
GROUP BY
  TUMBLE(INTERVAL '5 seconds'),
  instance,
  job,
  label_0,
  label_1,
  label_2;
