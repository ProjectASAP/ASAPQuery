# Workload Generator

A script to generate experiment configuration YAML files with randomized query workloads based on building blocks and distribution patterns.

## Overview

The `generate_workload.py` script creates experiment configs by randomly composing queries from 6 building blocks according to a specified distribution pattern.

### Query Building Blocks

| Block | Type | Example |
|-------|------|---------|
| **B1** | quantile by () | `quantile by (label_0) (0.95, fake_metric_total)` |
| **B2** | sum/count by | `sum by (label_0) (fake_metric_total)` |
| **B3** | quantile_over_time | `quantile_over_time(0.95, fake_metric_total[15m])` |
| **B4** | sum/count_over_time | `sum_over_time(fake_metric_total[15m])` |
| **B5** | rate/increase | `rate(fake_metric_total[15m])` |
| **B6** | nested aggregation | `sum by (label_0) (sum_over_time(fake_metric_total[15m]))` |

Each block randomly selects parameters:
- **Quantiles**: 0.5, 0.7, 0.8, 0.9, 0.95, 0.99
- **Aggregations**: sum, count
- **Time functions**: rate, increase, sum_over_time, count_over_time
- **Time range**: 15m (hardcoded, extensible)
- **By label**: label_0 (hardcoded, extensible)

## Usage

### Basic Examples

```bash
# Generate 5 uniform workloads with 20 queries each
python generate_workload.py --num-queries 20 --distribution uniform --num-configs 5

# Generate heavy-tailed workload favoring blocks 1, 3, and 5
python generate_workload.py --num-queries 50 --distribution heavy_tailed \
    --favor-blocks 1,3,5 --num-configs 3 --seed 42

# Allow duplicate queries
python generate_workload.py --num-queries 30 --distribution uniform \
    --num-configs 2 --allow-duplicates
```

### Required Arguments

- `--num-queries N`: Total number of queries per config file
- `--distribution {uniform,heavy_tailed}`: Distribution type
- `--num-configs K`: Number of config files to generate

### Optional Arguments

- `--seed S`: Random seed for reproducibility (optional, auto-generated if not provided)
- `--favor-blocks B1,B2,...`: Comma-separated block IDs to favor for heavy-tailed distribution
- `--allow-duplicates`: Allow duplicate queries in a config (default: enforce uniqueness)
- `--output-dir PATH`: Output directory (default: `config/experiment_type/generated`)

## Distribution Types

### Uniform Distribution

Divides queries equally across all 6 building blocks.

Example with `--num-queries 30`:
- B1: 5 queries
- B2: 5 queries
- B3: 5 queries
- B4: 5 queries
- B5: 5 queries
- B6: 5 queries

### Heavy-Tailed Distribution

Uses ordered exponential decay to favor certain blocks.

Example with `--num-queries 30 --favor-blocks 1,3`:
- Block ordering: [B1, B3, B2, B4, B5, B6]
- Weights: [1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125]
- Resulting distribution (approx):
  - B1: ~16 queries
  - B3: ~8 queries
  - B2: ~4 queries
  - B4: ~2 queries
  - B5: ~0-1 queries
  - B6: ~0-1 queries

## Output

Generated files are saved to `config/experiment_type/generated/` with naming pattern:
```
generated_workload_YYYYMMDD_HHMMSS_N.yaml
```

Where `N` is the config index (1, 2, 3, ...).

### Multiple Configs

When generating multiple configs (`--num-configs > 1`):
- Same distribution and query count
- Different random query selection
- Seeds increment: if `--seed 42`, configs use seeds 42, 43, 44, ...
- Ensures reproducibility while maintaining variation

## Extensibility

The script is designed for easy extension. Key extensible functions:

### Parameter Functions (lines 20-64)

```python
def get_aggregation_label() -> str:
    """Modify to support multiple labels or different selection"""
    return "label_0"

def get_time_range() -> str:
    """Modify to support variable time ranges"""
    return "15m"

def get_quantile_values() -> List[float]:
    """Modify to add/remove quantile options"""
    return [0.5, 0.7, 0.8, 0.9, 0.95, 0.99]
```

### Adding New Building Blocks

1. Create generator function:
```python
def generate_b7_query() -> str:
    """B7: Your new query type"""
    # Your logic here
    return query_string
```

2. Add to `BLOCK_GENERATORS` dict (line 141):
```python
BLOCK_GENERATORS = {
    1: generate_b1_query,
    # ... existing blocks ...
    7: generate_b7_query,  # Add your block
}
```

3. Update `num_blocks` parameter in distribution functions

### Modifying Base Config

Edit `get_base_config()` function (line 245) to change:
- Exporter configuration (labels, cardinality, dataset type)
- Monitoring settings
- Client options (repetitions, delays)
- Controller options (SLA thresholds)

## Examples with Output

### Example 1: Uniform Distribution
```bash
python generate_workload.py --num-queries 12 --distribution uniform --num-configs 1 --seed 100
```

Generated queries (sample):
```yaml
queries:
  - quantile by (label_0) (0.95, fake_metric_total)
  - sum by (label_0) (fake_metric_total)
  - rate(fake_metric_total[15m])
  - quantile_over_time(0.9, fake_metric_total[15m])
  - sum_over_time(fake_metric_total[15m])
  - sum by (label_0) (count_over_time(fake_metric_total[15m]))
  - quantile by (label_0) (0.8, fake_metric_total)
  - count by (label_0) (fake_metric_total)
  - increase(fake_metric_total[15m])
  - quantile_over_time(0.5, fake_metric_total[15m])
  - count_over_time(fake_metric_total[15m])
  - count by (label_0) (sum_over_time(fake_metric_total[15m]))
```

### Example 2: Heavy-Tailed Favoring Quantile Queries
```bash
python generate_workload.py --num-queries 20 --distribution heavy_tailed \
    --favor-blocks 1,3 --num-configs 1 --seed 200
```

Distribution:
- ~11 queries from B1 (quantile by)
- ~5 queries from B3 (quantile_over_time)
- ~2-3 queries from other blocks
- Remaining 1-2 queries distributed among B2, B4, B5, B6

## Tips

1. **Reproducibility**: Always use `--seed` for experiments you need to reproduce
2. **Uniqueness**: By default, queries are unique. Use `--allow-duplicates` only if needed
3. **Heavy-tailed**: Order in `--favor-blocks` matters! First block gets the most queries
4. **Testing**: Start with small `--num-queries` values to verify behavior
5. **Batch generation**: Use `--num-configs` to generate multiple variations in one run

## Current Hardcoded Values

These are currently hardcoded but can be easily modified:

| Parameter | Current Value | Function to Modify |
|-----------|--------------|-------------------|
| Aggregation label | `label_0` | `get_aggregation_label()` |
| Time range | `15m` | `get_time_range()` |
| Metric name | `fake_metric_total` | `get_metric_name()` |
| Metric type | `counter` | `get_metric_type()` |
| Number of labels | 3 | `get_base_config()` |
| Values per label | 20 | `get_base_config()` |
| Dataset type | `zipf` | `get_base_config()` |

## Future Extensions

Potential enhancements:
- Support for multiple aggregation labels: `by (label_0, label_1)`
- Variable time ranges: random selection from [5m, 15m, 30m, 1h]
- Custom distribution weights via CLI: `--weights 0.3,0.2,0.2,0.1,0.1,0.1`
- Template-based configuration for exporters
- Support for gauge metrics alongside counters
- Additional distributions (zipf, exponential with configurable decay)
- Label selector support: `{label_0="value1"}`
