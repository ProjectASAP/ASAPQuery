# Hydra Configuration Usage Guide

This guide explains how to use the Hydra framework integration for experiment scripts.

## Quick Start

```bash
# Basic experiment run
python experiment_run_e2e.py \
  experiment.name=my_test \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us \
  prometheus.local_config_dir=/path/to/prometheus/config
```

## Parameter Types

### Required Infrastructure Parameters
All experiment scripts require these core parameters:
- `experiment.name`: Human-readable experiment name
- `cloudlab.num_nodes`: Number of CloudLab nodes
- `cloudlab.username`: Your CloudLab username
- `cloudlab.hostname_suffix`: CloudLab experiment hostname suffix

### Script-Specific Required Parameters
- **experiment_run_e2e.py**: `experiment_type`, `prometheus.local_config_dir`
- **experiment_run_empty_flink.py**: `experiment.config_file`
- **experiment_run_e2e_no_queryengine.py**: `experiment.config_file`, `prometheus.local_config_dir`
- **experiment_run_sketchdboffline.py**: `experiment_variants.sketchdboffline.*` parameters
- **experiment_run_flink_with_different_num_aggregations.py**: `experiment_variants.flink_aggregations.*` parameters
- **experiment_run_exporters_and_prometheus.py**: No additional required parameters

### Optional Override Parameters
```yaml
# Logging and debugging
logging.level: DEBUG|INFO|WARNING|ERROR

# Profiling options
profiling.query_engine: true/false
profiling.prometheus_time: 60  # seconds
profiling.flink: true/false
profiling.arroyo: true/false

# Manual mode
manual.query_engine: true/false
manual.remote_monitor: true/false

# Experiment flow
flow.no_teardown: true/false
flow.steady_state_wait: 300  # seconds

# Streaming engine
streaming.engine: flink|arroyo
streaming.flink_input_format: json|avro-json|avro-binary
streaming.flink_output_format: json|byte
streaming.enable_object_reuse: true/false
streaming.do_local_flink: true/false
streaming.forward_unsupported_queries: true/false

# Prometheus configuration
prometheus.scrape_interval: "5s"
prometheus.evaluation_interval: "1s"
prometheus.query_log_file: "/path/to/queries.log"
prometheus.recording_rules.interval: "5s"

# Fake exporter language
fake_exporter_language: python|rust
```

## Experiment Config Groups (experiment_run_e2e.py)

The main experiment script uses **config groups** to organize experiment configurations:

### Available Experiment Types
```bash
# List available experiment types
ls config/experiment_type/
```

### Using Config Groups
```bash
# Select experiment type
python experiment_run_e2e.py experiment_type=cloud_demo [other params...]
python experiment_run_e2e.py experiment_type=flink_compress_config [other params...]
python experiment_run_e2e.py experiment_type=my_exp_config [other params...]
```

### Override Experiment Parameters
```bash
# Override parameters from the experiment config
python experiment_run_e2e.py \
  experiment_type=cloud_demo \
  experiment_params.query_groups.0.repetition_delay=60 \
  experiment_params.query_groups.0.client_options.repetitions=20 \
  experiment_params.exporters.exporter_list.fake_exporter.num_ports_per_server=4 \
  [other params...]
```

## Common Use Cases

### Development Testing
```bash
# Quick test with minimal resources
python experiment_run_e2e.py \
  experiment.name=dev_test \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=2 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=dev.cloudlab.us \
  prometheus.local_config_dir=/path/to/prometheus/config \
  logging.level=DEBUG \
  flow.steady_state_wait=60
```

### Performance Testing
```bash
# Test with different streaming engines
python experiment_run_e2e.py \
  experiment.name=arroyo_perf \
  experiment_type=my_exp_config \
  cloudlab.num_nodes=8 \
  streaming.engine=arroyo \
  streaming.enable_object_reuse=true \
  profiling.arroyo=true \
  [required params...]
```

### Grid Search Examples
```bash
# Test different repetition delays
python experiment_run_e2e.py experiment.name=delay_30 experiment_type=cloud_demo experiment_params.query_groups.0.repetition_delay=30 [required params...]
python experiment_run_e2e.py experiment.name=delay_60 experiment_type=cloud_demo experiment_params.query_groups.0.repetition_delay=60 [required params...]
python experiment_run_e2e.py experiment.name=delay_120 experiment_type=cloud_demo experiment_params.query_groups.0.repetition_delay=120 [required params...]

# Test different node counts
python experiment_run_e2e.py experiment.name=nodes_4 experiment_type=cloud_demo cloudlab.num_nodes=4 [required params...]
python experiment_run_e2e.py experiment.name=nodes_8 experiment_type=cloud_demo cloudlab.num_nodes=8 [required params...]
python experiment_run_e2e.py experiment.name=nodes_16 experiment_type=cloud_demo cloudlab.num_nodes=16 [required params...]

# Test different streaming engines
python experiment_run_e2e.py experiment.name=flink_test experiment_type=cloud_demo streaming.engine=flink [required params...]
python experiment_run_e2e.py experiment.name=arroyo_test experiment_type=cloud_demo streaming.engine=arroyo [required params...]
```

## All Experiment Scripts

### experiment_run_e2e.py (Full E2E Pipeline)
```bash
# Full pipeline with query engine
python experiment_run_e2e.py \
  experiment.name=full_test \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us \
  prometheus.local_config_dir=/path/to/prometheus/config
```

### experiment_run_exporters_and_prometheus.py (Monitoring Only)
```bash
# Only exporters and Prometheus - no streaming
python experiment_run_exporters_and_prometheus.py \
  experiment.name=monitoring_test \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us
```

### experiment_run_empty_flink.py (Simplified Streaming)
```bash
# Flink + Prometheus + Exporters (no query engine)
python experiment_run_empty_flink.py \
  experiment.name=empty_flink_test \
  experiment.config_file=/path/to/config.yml \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us
```

### experiment_run_e2e_no_queryengine.py (E2E without Query Engine)
```bash
# Full streaming pipeline without query engine
python experiment_run_e2e_no_queryengine.py \
  experiment.name=e2e_no_qe \
  experiment.config_file=/path/to/config.yml \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us \
  prometheus.local_config_dir=/path/to/prometheus/config
```

### experiment_run_sketchdboffline.py (Offline Analysis)
```bash
# Offline analysis of existing data
python experiment_run_sketchdboffline.py \
  experiment_variants.sketchdboffline.experiment_dir=/path/to/data \
  experiment_variants.sketchdboffline.groupby=[label_0,instance] \
  experiment_variants.sketchdboffline.aggregation=avg
```

### experiment_run_flink_with_different_num_aggregations.py (Performance Analysis)
```bash
# Flink performance testing with aggregation scaling
python experiment_run_flink_with_different_num_aggregations.py \
  experiment.name=flink_perf \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us \
  experiment_variants.flink_aggregations.config=/path/to/config.yaml \
  experiment_variants.flink_aggregations.aggregation_id=0 \
  experiment_variants.flink_aggregations.min_aggregations=1 \
  experiment_variants.flink_aggregations.max_aggregations=10
```

## Hydra Features

### Configuration Inspection
```bash
# View resolved configuration
python experiment_run_e2e.py --cfg job [params...]

# Show help
python experiment_run_e2e.py --help
```

### Output Directory Structure
Hydra creates organized output directories:
```
outputs/
├── <experiment_name>/
│   └── <timestamp>/
│       ├── .hydra/
│       │   ├── config.yaml      # Final resolved config
│       │   ├── hydra.yaml       # Hydra runtime config
│       │   └── overrides.yaml   # Applied overrides
│       └── experiment_outputs...
```

## Migration from argparse

### Old argparse approach
```bash
python experiment_run_e2e.py \
  --experiment_name test \
  --experiment_config config.yaml \
  --num_nodes 4 \
  --cloudlab_username user \
  --hostname_suffix exp.cloudlab.us \
  --prometheus_local_config_dir /path/to/config
```

### New Hydra approach
```bash
python experiment_run_e2e.py \
  experiment.name=test \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=4 \
  cloudlab.username=user \
  cloudlab.hostname_suffix=exp.cloudlab.us \
  prometheus.local_config_dir=/path/to/config
```

## Key Differences from Original

1. **Config Groups**: Use `experiment_type=config_name` instead of `experiment.config_file=path`
2. **Parameter Overrides**: Use `experiment_params.query_groups.0.repetition_delay=30` to override experiment parameters
3. **Structured Configuration**: All parameters are organized in logical groups (cloudlab, streaming, prometheus, etc.)
4. **No File Paths**: Experiment configs are referenced by name, not file paths
