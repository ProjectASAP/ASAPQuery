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
