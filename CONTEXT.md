# ASAPQuery

Terms used by the cost-based sketch optimizer and its benchmarking pipeline.

## Language

**Grouping labels**:
PromQL `GROUP BY` labels that partition a metric stream into label groups; a
non-subpopulation sketch is instantiated once per distinct group.
_Avoid_: keying labels

**Sketch key**:
The value or label tuple inserted into and queried from a keyed sketch such as
HLL, CMS, or Hydra. Its distribution, cardinality, and encoded size may affect
the measured atomic cost and accuracy.
_Avoid_: grouping label, partition key

**Benchmark profile**:
A reproducible description of the input trace slice used to measure an
atomic-cost table for a sketch family and configuration.

**Planner context**:
Query- and deployment-specific structural inputs, such as label-group count
and retained-window count, that scale atomic costs into a plan cost.

**Summary**:
A stored, materialized node of an installed ASAPPlanner plan (ASAPPlanner's
materialized view), built at ingest and read at query time.
_Avoid_: aggregation, view, precomputed output

**Summary window**:
One stored state of a summary, covering one time window.

**Raw data**:
Ingested samples or rows as they arrive, before any summary is built.

**Planned query**:
A query whose text is one of the installed plan's query texts.

**Capability-matched query**:
A query not in the plan that can still be answered from installed summaries.

**Not ready**:
A planned or capability-matched query whose summary windows don't yet cover
its whole evaluation range; it is answered by the fallback backend.

**Fallback**:
Forwarding a query the engines don't answer to the real backend (Prometheus,
ClickHouse, Elastic).
