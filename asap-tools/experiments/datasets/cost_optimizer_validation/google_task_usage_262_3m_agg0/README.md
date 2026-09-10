# Derived Google task-usage input for cost-profile consistency validation

This is a temporary, materialized input for the first same-data E2E
consistency experiment. It preserves the headerless Google `task_usage` CSV
shape expected by `cluster_data_exporter` and contains one correctly named
part file:

`part-00262-of-00500.csv.gz`

## Derivation

Source:

`../../../../benchmarks/metrics_observability/data/google-cluster-data/ClusterData2011/clusterdata-2011-2/task_usage/part-00262-of-00500.csv.gz`

Selection, using raw trace microseconds and inclusive interval containment:

```text
start_time >= 1313535000000
end_time   <= 1313715000000
aggregation_type is absent or 0
```

The absent-value rule matches the exporter, which treats a missing
`aggregation_type` as zero and exports it in the `_0` metric family.

## Provenance

- Rows: 20,051
- Source-time interval: `[1313535000000, 1313715000000]` microseconds (180 s)
- Distinct machine IDs: 6,481
- Distinct `(job_id, task_index, machine_id)` series: 10,379
- Derived file SHA-256: `13dd00844558627365848f5252d9f86fe9c302afe875ae2fd702016b4acdf70f`

This avoids adding source-time/filter options to the E2E exporter during the
integration check. Replace it with native filtering before treating the setup
as a reusable benchmark interface.
