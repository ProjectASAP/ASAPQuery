# ASAPQuery Quickstart Demo

## What is ASAPQuery?

ASAPQuery is a drop-in accelerator that reduces query latency by 100x. The current version of ASAPQuery (v0.1.0) sits between Prometheus and Grafana and accelerates repeating PromQL queries.
ASAPQuery is compatible with the Prometheus query API and integrates seamlessly with existing Grafana dashboards.

## What This Demo Shows

This quickstart simulates a typical monitoring deployment with components you might already have:
- Exporters - generating metrics
- Prometheus - collecting metrics from exporters and storing them in a time-series database
- Grafana - send queries to Prometheus and visualizing query results

Then it adds ASAPQuery's components on top:
- **Query Engine** - Prometheus-compatible API with sketch-based acceleration
- **Arroyo + ArroyoSketch** - Streaming engine with pipelines configured for building sketches
- **Kafka** - Message broker for streaming data from Arroyo to the Query Engine
- **Controller** - Automatically configures sketches from PromQL queries

Once you run the quickstart, you will see a pre-configured Grafana dashboard that compares Prometheues and ASAPQuery side-by-side. You will see **visually indistinguishable** results from Prometheus and ASAPQuery, with ASAPQuery being 100x faster

## Prerequisites

- **Docker & Docker Compose** v2.0+
- **Ports available**: 3000 (Grafana), 5115 (Arroyo), 8088 (ASAPQuery), 9090 (Prometheus)

## Quick Start

### Step 1: Start the Demo

```bash
cd quickstart
docker compose up -d
```

### Step 2: Wait for all Services to Start

The docker-compose pulls official Prometheus and Grafana images, and pre-built ASAPQuery images. After pulling these images, it runs scripts to configure these components. This may take a few minutes.

### Step 3: Open Grafana

1. Go to **http://localhost:3000**
2. If prompted, login with: `admin` / `admin`
3. Navigate to **Dashboards** → **ASAPQuery Dashboards** folder
4. Open **"ASAPQuery vs Prometheus Comparison"**

## What you should see immediately

The dashboard shows 4 rows, each with 2 columns. Each row shows the same query executing on ASAPQuery (left panel) and Prometheus (right panel).

![Screenshot of Grafana after running ASAPQuery's quickstart showing visually-indistinguishable dashboards served by ASAPQuery and Prometheus](/assets/img/quickstart_grafana_screenshot.png)

The dashboard auto-refreshes every 10 seconds. You will notice:
- ASAPQuery dashboard panels (left side) refresh **almost instantaneously**
- Prometheus panels (right side) lag considerably after each dashboard refresh

For reference, here are the 4 queries, one per row:
- "quantile by (pattern) (0.99, sensor_reading)"
- "quantile by (pattern) (0.95, sensor_reading)"
- "quantile by (pattern) (0.90, sensor_reading)"
- "quantile by (pattern) (0.50, sensor_reading)"

## What you should see after 5 minutes

Take a coffee break and come back after 5 minutes. By now, Prometheus will have loaded more data. Open the dashboard and you will see that:
- ASAPQuery dashboards (left side) still refresh almost instantaneously
- Prometheus queries (right side) are severly lagging (~5 seconds to refresh)

## Stopping the Demo

```bash
docker compose down
```

## Next Steps

### Changing the cardinality of ingested data

To change the cardinality of data exported by each exporter, use [set_data_cardinality.py](quickstart/set_data_cardinality.py).
Run `python3 set_data_cardinality.py <M> <N>` where `M` is the number of labels (currently configured to 3) and `N` is the number of unique values per label (currently configured to 30). The script changes all the `fake-exporter-*` services in `docker-compose.yml` to have:
```yaml
  - "--num-labels=M"              # Number of label dimensions
  - "--num-values-per-label=N"    # Cardinality per label
```

### Changing PromQL queries

To modify the queries in the Grafana dashboard and run ASAPQuery against those:

#### 1. Edit the Controller Config

Edit `config/controller-config.yaml`:

```yaml
query_groups:
  - id: 1
    queries:
      - "quantile by (label_0) (0.99, fake_metric)"
      - "quantile by (label_0) (0.50, fake_metric)"
      # Add your queries here
```

#### 2. Regenerate Grafana Dashboard

```bash
python3 generate_dashboards.py
```

This automatically creates a new dashboard with panels for all your queries.

#### 3. Restart Services

```bash
docker compose restart
```

### Running ASAPQuery with your own Grafana and Prometheus setup

**Coming Soon**: A drop-in ASAPQuery artifact that works with your existing already-configured Grafana and Prometheus deployments.
