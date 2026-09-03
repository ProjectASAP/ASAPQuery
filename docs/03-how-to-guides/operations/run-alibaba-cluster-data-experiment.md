# Run an Alibaba Cluster-Data Experiment

This guide runs `asap-tools/experiments/experiment_run_e2e.py` with the
Alibaba 2021 Node trace exporter instead of the synthetic `fake_exporter`.
It uses one CloudLab worker plus the coordinator:

- `node18`: coordinator, Prometheus, controller, and query client
- `node19`: cluster-data exporter and system exporters

In this repository, `providers.cloudlab.num_nodes=1` means one worker in
addition to the coordinator.

## Prerequisites

Run the local preparation commands from the ASAPQuery repository root:

```bash
cd /home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery
```

The Alibaba data in this checkout is under:

```text
../../benchmarks/metrics_observability/data/alibaba-cluster-data/cluster-trace-microservices-v2021/data
```

The exporter requires sorted `.csv.gz` files. The downloaded 2021 Node data
is initially an archive/CSV and must be preprocessed.

## Prepare the 2021 Node data

This processes the `Node_*.tar.gz` files and writes `Node_*.csv.gz` beside
them. It does not process the much larger `MSResource` data.

```bash
ALIBABA_DATA="$PWD/../../benchmarks/metrics_observability/data/alibaba-cluster-data/cluster-trace-microservices-v2021/data"

./asap-tools/data-sources/prometheus-exporters/cluster_data_exporter/bin/alibaba/sort_and_format.sh \
  "$ALIBABA_DATA" \
  --year 2021 \
  -n
```

For the run documented here, this produced one file:

```text
Node/Node_0.csv.gz
```

It contains 18,868,400 records and 13,118 distinct nodes. The compressed
size is approximately 1.16 GB and the uncompressed size is approximately
2.26 GB.

## Copy the data to CloudLab

The exporter runs on `node19`. `/data` is not writable by the CloudLab user,
so use a directory under `/scratch`:

```bash
CDE_HOST="node19.scratch2.cloudmigration-PG0.utah.cloudlab.us"

ssh -o StrictHostKeyChecking=no "milindsr@$CDE_HOST" \
  'mkdir -p /scratch/sketch_db_for_prometheus/cluster_traces'

rsync -ah --info=progress2 -e 'ssh -o StrictHostKeyChecking=no' \
  "$ALIBABA_DATA"/Node/Node_*.csv.gz \
  "milindsr@$CDE_HOST:/scratch/sketch_db_for_prometheus/cluster_traces/"
```

## Build the exporter image

The Docker image must be built on `node19`, because Docker images are local
to each node:

```bash
ssh -o StrictHostKeyChecking=no "milindsr@$CDE_HOST" \
  'cd /scratch/sketch_db_for_prometheus/code/asap-tools/data-sources/prometheus-exporters/cluster_data_exporter &&
   bash installation/install.sh'
```

This creates:

```text
sketchdb-cluster-data-exporter:latest
```

## Run the experiment

The checked-in experiment configuration is
`asap-tools/experiments/config/experiment_type/cluster_data_alibaba_node_2021.yaml`.
It runs four global CPU quantiles, ten repetitions each, at 3x replay speed.

`controller.punting` is set to `false` in
`asap-tools/experiments/config/config.yaml`.

```bash
cd /home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery/asap-tools/experiments

python3 experiment_run_e2e.py \
  experiment.name=alibaba_node_2021_offset18 \
  experiment_type=cluster_data_alibaba_node_2021 \
  providers.cloudlab.username=milindsr \
  providers.cloudlab.hostname_suffix=scratch2.cloudmigration-PG0.utah.cloudlab.us \
  providers.cloudlab.node_offset=18 \
  providers.cloudlab.num_nodes=1 \
  cluster_data_directory=/scratch/sketch_db_for_prometheus/cluster_traces
```

The default run takes about three minutes and tears down the services when
it finishes. Use `flow.no_teardown=true` only when debugging service state.

## Change the query's aggregation column or grouping label

The raw 2021 Node CSV has columns like:

```text
timestamp,nodeid,node_cpu_usage,node_memory_usage
```

The exporter maps these to Prometheus as:

| Raw data | Prometheus metric/label |
|---|---|
| `node_cpu_usage` | `alibaba_node_cpu_usage` |
| `node_memory_usage` | `alibaba_node_memory_usage` |
| `nodeid` | `node_id` label |

The query must use Prometheus names, not the raw CSV names.

### Select CPU or memory

To query CPU, use:

```promql
quantile by () (0.95, alibaba_node_cpu_usage)
```

To query memory instead, change the metric name:

```promql
quantile by () (0.95, alibaba_node_memory_usage)
```

When changing the metric, update both sections in the experiment config:

```yaml
query_groups:
- id: 1
  queries:
  - quantile by () (0.95, alibaba_node_memory_usage)

metrics:
- metric: alibaba_node_memory_usage
  labels: [instance, job, node_id]
  exporter: cluster_data_exporter
```

### Group by a label

The current queries use `by ()`, which combines all node series into one
global result. To get one result per node, group by `node_id`:

```promql
quantile by (node_id) (0.95, alibaba_node_cpu_usage)
```

For a sum by node:

```promql
sum by (node_id) (alibaba_node_cpu_usage)
```

Available labels for Node data are `node_id`, `instance`, and `job`.
`node_id` is the useful data label; `instance` and `job` are added by the
Prometheus scrape configuration.

## Inspect results

Results are written under:

```text
experiment_outputs/<experiment-name>/
```

For the documented run:

```text
experiment_outputs/alibaba_node_2021_offset18/
```

Useful files include:

- `hydra_config.yaml`: fully resolved Hydra configuration
- `cmdline_args.txt`: resolved runner arguments
- `experiment_config/experiment_params.yaml`: experiment queries/exporter config
- `sketchdb/prometheus_client_output/query_results.jsonl.gz`: query results
- `sketchdb/prometheus_client_output/query_latencies.jsonl.gz`: query latencies
- `sketchdb/remote_monitor_output/monitor_output.json`: CPU/memory samples

## Analyze latencies and resource usage

The latency wrapper assumes separate `baseline` and `sketchdb` experiment
modes. The Alibaba example has one `sketchdb` mode with both Prometheus and
SketchDB servers, so compare those servers directly:

```bash
cd /home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery/asap-tools/experiments/post_experiment

PYTHONPATH=/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery/asap-common/dependencies/py/promql_utilities \
python3 compare_latencies.py \
  --experiment_name alibaba_node_2021_offset18 \
  --exact_experiment_mode sketchdb \
  --exact_experiment_server_name prometheus \
  --estimate_experiment_mode sketchdb \
  --estimate_experiment_server_name sketchdb \
  --print_per_query
```

Analyze the monitor output with:

```bash
python3 analyze_monitor_output.py \
  /home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery/experiment_outputs/alibaba_node_2021_offset18/sketchdb/remote_monitor_output/monitor_output.json \
  --print
```

The `run_compare_latencies.sh` wrapper currently hardcodes `baseline` versus
`sketchdb`, and its documented `--per_query` flag does not match the
underlying script's `--print_per_query` flag. Use the direct command above for
this single-mode experiment.
