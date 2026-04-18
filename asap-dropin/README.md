# ASAPQuery Drop-in for Existing Prometheus + Grafana Stacks

A self-contained single-container Docker Compose that adds ASAPQuery to an existing Prometheus and Grafana deployment.

On startup, all queries are forwarded transparently to your upstream Prometheus. After one observation window (default 180s), the engine automatically plans and activates sketch-based acceleration based on the real queries it observed from Grafana.

## Prerequisites

- Docker and Docker Compose
- A running Prometheus instance
- A running Grafana instance (with a Prometheus datasource)

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

## Setup

### Step 1 — Configure environment

Edit `.env` to match your deployment:

| Variable | Default | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://host.docker.internal:9090` | URL of your Prometheus, reachable from inside the ASAPQuery container |
| `PROMETHEUS_SCRAPE_INTERVAL` | `15` | Your Prometheus scrape interval in seconds |
| `REMOTE_WRITE_PORT` | `9091` | Host port for the remote-write receiver |
| `QUERY_ENGINE_PORT` | `8088` | Host port for the ASAPQuery query engine |
| `TRACKER_OBSERVATION_WINDOW_SECS` | `180` | How long to observe queries before planning (see note below) |

**Finding the right `PROMETHEUS_URL`:**
- **Docker Desktop (Mac/Windows):** `http://host.docker.internal:9090` (default)
- **Linux (Prometheus on host):** `http://172.17.0.1:9090` (default Docker bridge gateway)
- **Prometheus in another Docker Compose:** use a shared external Docker network and the Prometheus service name

**Setting `TRACKER_OBSERVATION_WINDOW_SECS`:**
Set this to at least 3× your Grafana dashboard refresh interval so ASAPQuery sees enough query repetitions to build a useful plan.
- Grafana refresh 30s → set to 90 or higher
- Grafana refresh 1m → set to 180 or higher (default)
- Grafana refresh 5m → set to 900 or higher

### Step 2 — Start ASAPQuery

```bash
docker compose up -d
```

Verify it started:

```bash
docker compose logs -f queryengine
```

You should see a line confirming Prometheus is reachable, then the engine waiting for the observation window.

### Step 3 — Configure Prometheus remote_write

Prometheus needs to send all ingested samples to ASAPQuery so it can build sketches.

**Add this block to your `prometheus.yml`:**

```yaml
remote_write:
  - url: http://localhost:9091/receive
    queue_config:
      batch_send_deadline: 1s
      sample_age_limit: 5m
```

> **Finding the right `remote_write` URL:** The URL is from Prometheus's perspective, not your browser's.
> - **Prometheus on the same host as Docker:** `http://localhost:9091/receive` (default above)
> - **Prometheus in Docker on the same host:** `http://host.docker.internal:9091/receive` (Mac/Windows) or `http://172.17.0.1:9091/receive` (Linux)
> - Change `9091` if you set a different `REMOTE_WRITE_PORT` in `.env`

**Reload Prometheus to apply the change:**

If Prometheus was started with `--web.enable-lifecycle`:
```bash
curl -X POST http://localhost:9090/-/reload
```

Otherwise, restart your Prometheus process or container:
```bash
# systemd
sudo systemctl restart prometheus

# Docker Compose
docker compose restart prometheus
```

**Verify remote_write is active** by checking Prometheus logs for a line like:
```
level=info msg="Remote storage started"
```

### Step 4 — Point Grafana at ASAPQuery

Grafana needs to send its queries to ASAPQuery instead of directly to Prometheus.

1. Open Grafana in your browser
2. Go to **Connections → Data Sources** (or **Configuration → Data Sources** in older Grafana)
3. Click on your existing Prometheus datasource
4. Change the **URL** field from your current Prometheus address to:
   ```
   http://localhost:8088
   ```
   (Change the port if you set a different `QUERY_ENGINE_PORT` in `.env`)
5. Click **Save & Test** — you should see "Data source is working"

ASAPQuery speaks the Prometheus HTTP API. Grafana does not need any other changes.

### Step 5 — Verify end-to-end

Open your Grafana dashboards and use them normally. During the observation window, all queries pass through to Prometheus transparently — your dashboards continue to work.

After the observation window elapses, check the ASAPQuery logs:

```bash
docker compose logs queryengine | grep query_tracker
```

You should see lines like:
```
query_tracker: planner succeeded — streaming aggregations: N, inference queries: M
```

From this point on, queries that ASAPQuery can accelerate are served from sketches. Check the routing in the logs:

```bash
docker compose logs queryengine | grep "destination="
```

Lines with `destination=asap` are served by ASAPQuery; lines with `destination=prometheus` are forwarded to your upstream Prometheus.

## Development

To build from local source instead of pulling pre-built images:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```
