# ASAPQuery Drop-in for Existing Prometheus + Grafana Stacks

A self-contained single-container Docker Compose that adds ASAPQuery to an existing Prometheus and Grafana deployment.

On startup, all queries are forwarded transparently to your upstream Prometheus. After one observation window (default 10 min), the engine automatically plans and activates sketch-based acceleration based on the real queries it observed from Grafana.

## Prerequisites

- Docker and Docker Compose
- A running Prometheus instance
- A running Grafana instance (with a Prometheus datasource)

## Quick Start

### 1. Configure environment

Edit `.env`:

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
Your Prometheus ──remote_write──▸ ASAPQuery (:9091/receive)
                                      │
                                      ▼
Your Grafana ◂──query──── ASAPQuery Query Engine (:8088)
                                      │
                                      ▼ (fallback / passthrough)
                               Your Prometheus
```

The query engine embeds the planner and runs it automatically after observing real Grafana queries for one observation window. No separate planner container, no Kafka, no Arroyo.

## Development

To build from local source instead of pulling pre-built images:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```
