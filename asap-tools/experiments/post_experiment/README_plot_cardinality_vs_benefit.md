# plot_cardinality_vs_benefit.py

Plots latency benefit vs lookback period with one line per data scale (cardinality).

## Overview

Analyzes experiments following the naming pattern: `<query_type>_<lookback>_1_card_2_<exp>`

Example: `qot_30m_1_card_2_5` = quantile_over_time, 30m lookback, cardinality 2^5

**Plot output:**
- **X-axis**: Lookback period (log₂ scale, base=15m) - equally spaced points
- **Y-axis**: Latency benefit ratio (prometheus/sketchdb)
- **Lines**: One per data scale (2^0 through 2^9)

## X-axis Transformation

Uses log₂(T/15) so lookback periods are equally spaced:
```
1m   → -3.91
15m  →  0.00
30m  →  1.00
60m  →  2.00
120m →  3.00
```

## Usage

```bash
# Print summary table
python3 plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --print

# Generate and save plot
python3 plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --plot --save output.png

# Both print and plot
python3 plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --print --plot --save output.png

# Filter specific cardinalities (cleaner plot)
python3 plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" \
  --cardinalities 0 3 5 7 9 \
  --plot --save sparse.png

# Different metric (default: p95)
python3 plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" \
  --metric median \
  --print

# Multiple patterns
python3 plot_cardinality_vs_benefit.py "qot_15m_*" "qot_30m_*" \
  --plot --save subset.png
```

## Options

| Flag | Description |
|------|-------------|
| `patterns` | Glob patterns for experiment names (positional, required) |
| `--metric` | Latency metric: `median`, `p95`, `p99`, `mean`, `sum` (default: `p95`) |
| `--cardinalities` | Filter to specific cardinality exponents (e.g., `0 2 4 6 8`) |
| `--print` | Print summary table |
| `--plot` | Generate plot (requires `--save` or `--show`) |
| `--save FILE` | Save plot to file |
| `--show` | Display plot |

## Requirements

- Experiments must follow naming pattern: `<query_type>_<lookback>_1_card_2_<exp>`
- Each experiment must have both `prometheus/` and `sketchdb/` subdirectories
- Lookback format: `1m`, `15m`, `30m`, `60m`, `120m`, etc.

## Output Example

**Summary Table:**
```
Query Type: qot
----------------------------------------------------------------------------------------------------
lookback_str   1m  15m   30m   60m  120m
2^0 (1)      1.43 1.56  1.81  1.88  2.29
2^5 (32)     1.33 3.47  5.04  9.56 16.16
2^9 (512)    1.44 6.34 11.32 21.78 35.75
```

**Plot:** One line per cardinality showing how benefit changes with lookback period.

## Dependencies

- plotnine
- pandas
- numpy
- PyYAML
- Local modules: `constants`, `results_loader`, `compare_latencies`
