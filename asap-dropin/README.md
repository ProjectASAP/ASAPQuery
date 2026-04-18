# ASAPQuery Drop-in for Existing Prometheus + Grafana Stacks

A self-contained single-container Docker Compose that adds ASAPQuery to an existing Prometheus and Grafana deployment.

On startup, all queries are forwarded transparently to your upstream Prometheus. After one observation window (default 180s), the engine automatically plans and activates sketch-based acceleration based on the real queries it observed from Grafana.

## Prerequisites

- Docker and Docker Compose
- A running Prometheus instance
- A running Grafana instance (with a Prometheus datasource)

## Architecture

```
Prometheus ──remote_write──▶ ASAPQuery (:9091)
    ▲                              │
    │ unsupported queries          ▼ builds sketches
    └──────────── ASAPQuery (:8088) ◀── Grafana
```

## Setup

### Step 1 — Configure environment

Edit `.env` to match your deployment:

| Variable | Default | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://host.docker.internal:9090` | URL of your Prometheus, reachable from inside the ASAPQuery container |
| `PROMETHEUS_SCRAPE_INTERVAL` | `15` | Your Prometheus scrape interval in seconds |
| `REMOTE_WRITE_PORT` | `9091` | ASAPQuery data ingest port — must be free on the host |
| `QUERY_ENGINE_PORT` | `8088` | ASAPQuery query endpoint port — must be free on the host |
| `TRACKER_OBSERVATION_WINDOW_SECS` | `180` | How long to observe queries before planning (see note below) |

**Finding the right `PROMETHEUS_URL`:**
- **Prometheus on the same host as Docker:** `http://172.17.0.1:9090` (default Docker bridge gateway on Linux)
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
docker compose logs queryengine
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

> **Finding the right `remote_write` URL:** The URL is from Prometheus's perspective.
> - **Prometheus on the same host as Docker:** `http://localhost:9091/receive` (default above)
> - **Prometheus in Docker on the same host:** `http://host.docker.internal:9091/receive` (Mac/Windows) or `http://172.17.0.1:9091/receive` (Linux)
> - Change `9091` if you set a different `REMOTE_WRITE_PORT` in `.env`

**Reload Prometheus to apply the change:**

If Prometheus was started with `--web.enable-lifecycle`:
```bash
curl -X POST http://localhost:9090/-/reload
```

Otherwise, send SIGHUP to the Prometheus process:
```bash
kill -HUP $(pgrep prometheus)
```

See the [Prometheus configuration docs](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) for more details on reloading.

### Step 4 — Add an ASAPQuery datasource in Grafana

Create a new datasource in Grafana pointing at ASAPQuery, then switch your dashboards to use it.

1. Open Grafana in your browser
2. Go to **Connections → Data Sources**
3. Click **Add new data source** and select **Prometheus**
4. Set the **Name** to something like `ASAPQuery`
5. Set the **URL** to:
   ```
   http://localhost:8088
   ```
   (Change the port if you set a different `QUERY_ENGINE_PORT` in `.env`)
6. Click **Save & Test** — you should see "Data source is working"
7. Open your dashboards and switch their datasource to `ASAPQuery`

ASAPQuery speaks the Prometheus query API. Queries it can accelerate are answered from sketches; all others are transparently forwarded to your upstream Prometheus, so your dashboards continue to work.

### Step 5 — Verify end-to-end

Use your Grafana dashboards normally. During the observation window, all queries pass through to Prometheus transparently.

After the observation window elapses, check the ASAPQuery logs:

```bash
docker compose logs queryengine | grep query_tracker
```

You should see lines like:
```
query_tracker: planner succeeded — streaming aggregations: N, inference queries: M
```

From this point on, check the routing in the logs:

```bash
docker compose logs queryengine | grep "destination="
```

Lines with `destination=asap` are served by ASAPQuery; lines with `destination=prometheus` are forwarded to your upstream Prometheus.

## Development

To build from local source instead of pulling pre-built images:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```
