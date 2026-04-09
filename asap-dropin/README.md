# ASAPQuery Drop-in for Existing Prometheus + Grafana Stacks

A self-contained Docker Compose that adds ASAPQuery to an existing Prometheus and Grafana deployment. No additional services need to be installed or managed beyond what is in this compose file.

ASAPQuery auto-discovers all metrics from your Prometheus and generates quantile sketches for them — no query configuration needed.

## Prerequisites

- Docker and Docker Compose
- A running Prometheus instance
- A running Grafana instance (with a Prometheus datasource)

## Quick Start

### 1. Set environment variables

Copy and edit the `.env` file:

| Variable | Default | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://host.docker.internal:9090` | URL of your Prometheus, reachable from inside Docker |
| `PROMETHEUS_SCRAPE_INTERVAL` | `15` | Your Prometheus scrape interval in seconds |
| `REMOTE_WRITE_PORT` | `9091` | Host port for the remote-write receiver |
| `QUERY_ENGINE_PORT` | `8088` | Host port for the ASAPQuery query engine |

**Finding the right `PROMETHEUS_URL`:**
- **Docker Desktop (Mac/Windows):** `http://host.docker.internal:9090` (default)
- **Linux (Prometheus on host):** `http://172.17.0.1:9090` (default Docker bridge gateway)
- **Prometheus in another Docker Compose:** create a shared external network

### 2. Start ASAPQuery

```bash
docker compose up -d
```

### 3. Add remote_write to your Prometheus

Add this to your `prometheus.yml` and reload Prometheus:

```yaml
remote_write:
  - url: http://localhost:9091/receive
    queue_config:
      batch_send_deadline: 1s
      sample_age_limit: 5m
```

### 4. Point Grafana at ASAPQuery

Change your Grafana Prometheus datasource URL from your Prometheus address to:

```
http://localhost:8088
```

ASAPQuery speaks the Prometheus query API. Queries it can accelerate are answered from sketches; all others are transparently forwarded to your upstream Prometheus.

## Architecture

```
Your Prometheus ──remote_write──▸ Arroyo (:9091/receive)
                                     │
                                     ▼
                                   Kafka
                                     │
                                     ▼
Your Grafana ◂──query──── ASAPQuery Query Engine (:8088)
                                     │
                                     ▼ (fallback)
                              Your Prometheus
```

## Future: single-container mode

This compose currently runs six containers (Kafka, Arroyo, planner, summary-ingest, query engine, plus kafka-init). Once issues #242, #243, and #244 are completed and the precompute_engine cutover is done, the query engine will be able to:

- Receive Prometheus remote_write directly (precompute engine, `--enable-prometheus-remote-write`)
- Auto-discover metrics and run the planner in-process (already embedded via #271/#272)
- Hot-reload sketch and ingest configs at runtime (#242/#243/#244)

At that point this compose should be collapsed to a single `queryengine` container — Kafka, Arroyo, the planner init container, and asap-summary-ingest all become unnecessary.

## Development

To build from local source instead of pulling pre-built images:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```
