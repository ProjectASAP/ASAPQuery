# ASAPQuery Drop-in for Existing Prometheus + Grafana Stacks

A self-contained single-container Docker Compose that adds ASAPQuery to an existing Prometheus and Grafana deployment.

On startup, all queries are forwarded transparently to your upstream Prometheus. After one observation window (default 60s), the engine automatically plans and activates sketch-based acceleration based on the real queries it observed from Grafana.

## Prerequisites

- Docker and Docker Compose
- A running Prometheus instance
- A running Grafana instance (with a Prometheus datasource)

## Deployment modes

How you configure `PROMETHEUS_URL` (the URL ASAPQuery uses to reach Prometheus) and the `remote_write` URL (the URL Prometheus uses to reach ASAPQuery) depends on how Prometheus is running:

| Mode | Description |
|---|---|
| **A — Prometheus on bare metal** | Prometheus runs as a process directly on the host |
| **B — Prometheus in Docker, host network** | Prometheus runs in a container with `--network host` |
| **C — Prometheus in Docker, bridge/compose network** | Prometheus runs in a container on a named Docker network |

Modes A and B behave identically from a networking perspective. Mode C requires an extra step to join ASAPQuery to Prometheus's Docker network.

## Setup

### Step 1 — Configure environment

Edit `.env` to match your deployment:

| Variable | Default | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://localhost:9090` | URL of your Prometheus, reachable from inside the ASAPQuery container |
| `PROMETHEUS_SCRAPE_INTERVAL` | `15` | Your Prometheus scrape interval in seconds |
| `ASAPQUERY_DATA_PORT` | `9091` | ASAPQuery data ingest port — must be free on the host |
| `ASAPQUERY_QUERY_PORT` | `8088` | ASAPQuery query endpoint port — must be free on the host |
| `TRACKER_OBSERVATION_WINDOW_SECS` | `60` | How long to observe queries before planning (see note below) |

**Finding the right `PROMETHEUS_URL`** (from inside the ASAPQuery container):

- **Mode A / B:** `http://localhost:9090` — ASAPQuery runs on the host network (see Step 2a), so `localhost` resolves to the host ← default
- **Mode C:** use a shared Docker network (see Step 2b) and the Prometheus service name, e.g. `http://prometheus:9090`

**Setting `TRACKER_OBSERVATION_WINDOW_SECS`:**
Set this to at least 3× your Grafana dashboard refresh interval so ASAPQuery sees enough query repetitions to build a useful plan.
- Grafana refresh 10s → set to 30 or higher
- Grafana refresh 30s → set to 90 or higher
- Grafana refresh 1m → set to 180 or higher

### Step 2a — Start ASAPQuery (Mode A and B)

Run ASAPQuery on the host network so it can reach Prometheus at `localhost`. Create a `docker-compose.override.yml`:

```yaml
services:
  queryengine:
    network_mode: host
```

Then start:

```bash
docker compose up -d
```

Verify it started:

```bash
docker compose logs queryengine
```

You should see these two lines confirming Prometheus is reachable and the tracker is running:

```
INFO  Prometheus reachable at http://localhost:9090
INFO  Query tracker enabled (observation window: 60s)
```

### Step 2b — Start ASAPQuery and join Prometheus's network (Mode C)

First, identify the name of the Docker network Prometheus is attached to:

```bash
docker inspect <prometheus-container-name> --format '{{json .NetworkSettings.Networks}}' | jq 'keys'
```

Add that network as an external network in a `docker-compose.override.yml`:

```yaml
networks:
  prometheus-net:
    external: true
    name: <the-network-name-from-above>

services:
  queryengine:
    networks:
      - prometheus-net
```

Set `PROMETHEUS_URL` in `.env` to the Prometheus service name on that network, e.g.:

```
PROMETHEUS_URL=http://prometheus:9090
```

Then start:

```bash
docker compose up -d
```

Verify it started:

```bash
docker compose logs queryengine
```

You should see:

```
INFO  Prometheus reachable at http://prometheus:9090
INFO  Query tracker enabled (observation window: 60s)
```

### Step 3 — Configure Prometheus remote_write

Prometheus needs to send all ingested samples to ASAPQuery so it can build sketches.

The `remote_write` URL is written from **Prometheus's perspective**, so it depends on where Prometheus is running.
Change `9091` if you set a different `ASAPQUERY_DATA_PORT` in `.env`.

#### Mode A and B — Prometheus on bare metal or host network

ASAPQuery is on the host network (Step 2a), so Prometheus reaches it at `localhost`:

```yaml
remote_write:
  - url: http://localhost:9091/api/v1/write
    queue_config:
      batch_send_deadline: 1s
      sample_age_limit: 5m
```

#### Mode C — Prometheus in Docker, bridge/compose network

After joining the shared network (Step 2b), Prometheus can reach ASAPQuery by container hostname:

```yaml
remote_write:
  - url: http://queryengine:9091/api/v1/write
    queue_config:
      batch_send_deadline: 1s
      sample_age_limit: 5m
```

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

> **Note:** Most editors (vim, VS Code, etc.) replace the file on save rather than writing in place, which changes the inode. If Prometheus is running in Docker, its bind-mount tracks the original inode, so `/-/reload` will silently reload the old content. To avoid this: mount the config **directory** instead of the file (e.g. `-v ./prometheus-config:/etc/prometheus`), which lets Docker follow file replacements correctly. Alternatively, restart the Prometheus container after editing.

### Step 4 — Add an ASAPQuery datasource in Grafana

Create a new datasource in Grafana pointing at ASAPQuery, then switch your dashboards to use it.

1. Open Grafana in your browser
2. Go to **Connections → Data Sources**
3. Click **Add new data source** and select **Prometheus**
4. Set the **Name** to `ASAPQuery`
5. Set the **URL** to the ASAPQuery query endpoint, as seen from Grafana:
   - **Grafana on bare metal (Mode A/B, or any mode):** `http://localhost:8088`
   - **Grafana in Docker on the same network as ASAPQuery (Mode C):** `http://queryengine:8088`

   Change the port if you set a different `ASAPQUERY_QUERY_PORT` in `.env`.
6. Click **Save & Test** — you should see "Data source is working"
7. Open your dashboards and switch their datasource to `ASAPQuery`

ASAPQuery speaks the Prometheus query API. Queries it can accelerate are answered from sketches; all others are transparently forwarded to your upstream Prometheus, so your dashboards continue to work.

### Step 5 — Verify end-to-end

Use your Grafana dashboards normally. During the observation window, all queries pass through to Prometheus transparently.

After the observation window elapses, check the ASAPQuery logs:

```bash
docker compose logs queryengine | grep query_tracker
```

You should see a line like:

```
INFO query_tracker: planner succeeded — streaming aggregations: N, inference queries: M, punted: P
```

From this point on, check the routing in the logs:

```bash
docker compose logs queryengine | grep "destination="
```

Lines with `destination=asap` are served by ASAPQuery; lines with `destination=prometheus` are forwarded to your upstream Prometheus; lines with `destination=none_unsupported` are queries ASAPQuery does not support (also forwarded).

## Development

To build from local source instead of pulling pre-built images:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```
