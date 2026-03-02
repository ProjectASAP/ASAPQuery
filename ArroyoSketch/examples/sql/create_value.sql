CREATE TABLE your_table (
  value TEXT
) WITH (
  connector = 'filesystem',
  type = 'source',
  path = '/Users/milindsrivastava/Desktop/cmu/research/sketch_db_for_prometheus/code/arroyo_files/inputs/',
  format = 'json',
  'source.regex-pattern' = 'value\.json'
);
CREATE TABLE output_table (
  value TEXT
) WITH (
  connector = 'filesystem',
  type = 'sink',
  path = '/Users/milindsrivastava/Desktop/cmu/research/sketch_db_for_prometheus/code/arroyo_files/outputs/',
  format = 'json'
);
INSERT INTO output_table
SELECT value FROM your_table;
