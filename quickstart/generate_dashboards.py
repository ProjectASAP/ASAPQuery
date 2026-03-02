#!/usr/bin/env python3
"""Generate a single Grafana dashboard with all queries comparing Prometheus vs ASAPQuery."""

import json
import yaml
import os
import glob

# Configuration
# Query time offset accounts for data freshness delay in the pipeline:
# exporter → prometheus scrape → remote write → arroyo → kafka → query engine
# This prevents queries from requesting data that hasn't been processed yet
QUERY_TIME_OFFSET = 10  # seconds

# Range query parameters - must match values passed to Controller
# These ensure deterministic query behavior across Grafana and the query engine
RANGE_DURATION = 60  # seconds (end - start)
STEP = 10  # seconds (query resolution)

# Read controller config to get queries
config_path = "config/controller-config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

queries = []
for query_group in config["query_groups"]:
    queries.extend(query_group["queries"])

output_dir = "config/grafana/provisioning/dashboards"
os.makedirs(output_dir, exist_ok=True)


def get_percentile_name(query):
    """Extract percentile name from query."""
    if "0.99" in query:
        return "P99"
    elif "0.95" in query:
        return "P95"
    elif "0.90" in query:
        return "P90"
    elif "0.80" in query:
        return "P80"
    elif "0.70" in query:
        return "P70"
    elif "0.60" in query:
        return "P60"
    elif "0.50" in query:
        return "P50"
    elif "0.40" in query:
        return "P40"
    elif "0.30" in query:
        return "P30"
    elif "0.20" in query:
        return "P20"
    return "Query"


def create_panel(query, datasource_name, datasource_uid, panel_id, x_pos, y_pos):
    """Create a single panel."""
    percentile = get_percentile_name(query)
    title = f"{percentile} - {datasource_name}"

    return {
        "datasource": {"type": "prometheus", "uid": datasource_uid},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic-by-name"},
                "min": 0,
                "custom": {
                    "axisBorderShow": False,
                    "axisCenteredZero": False,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "drawStyle": "line",
                    "fillOpacity": 0,
                    "gradientMode": "none",
                    "hideFrom": {"tooltip": False, "viz": False, "legend": False},
                    "insertNulls": False,
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {"type": "linear"},
                    "showPoints": "auto",
                    "spanNulls": False,
                    "stacking": {"group": "A", "mode": "none"},
                    "thresholdsStyle": {"mode": "off"},
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}],
                },
            },
            "overrides": [],
        },
        "gridPos": {"h": 8, "w": 12, "x": x_pos, "y": y_pos},
        "id": panel_id,
        "options": {
            "legend": {
                "calcs": [],
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": True,
                "sortBy": "Name",
                "sortDesc": False,
            },
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "targets": [
            {
                "datasource": {"type": "prometheus", "uid": datasource_uid},
                "editorMode": "code",
                "expr": query,
                "instant": False,
                "interval": f"{STEP}s",
                "legendFormat": "{{label_0}}",
                "range": True,
                "refId": "A",
            }
        ],
        "title": title,
        "type": "timeseries",
    }


# Create dashboard
panels = []
panel_id = 1
y_position = 0

for query_idx, query in enumerate(queries):
    # ASAPQuery panel (left side)
    panels.append(
        create_panel(query, "ASAPQuery", "ASAPQuery", panel_id, 0, y_position)
    )
    panel_id += 1

    # Prometheus panel (right side)
    panels.append(
        create_panel(query, "Prometheus", "Prometheus", panel_id, 12, y_position)
    )
    panel_id += 1

    # Move to next row
    y_position += 8

dashboard = {
    "annotations": {"list": []},
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 0,
    "id": None,
    "links": [],
    "panels": panels,
    "refresh": "10s",
    "schemaVersion": 39,
    "tags": ["asap", "quickstart", "comparison"],
    "templating": {"list": []},
    "time": {
        "from": f"now-{RANGE_DURATION}s-{QUERY_TIME_OFFSET}s",
        "to": f"now-{QUERY_TIME_OFFSET}s",
    },
    "timepicker": {},
    "timezone": "browser",
    "title": "ASAPQuery vs Prometheus - Quantile Comparison",
    "uid": "asap-comparison",
    "version": 0,
    "weekStart": "",
}

for old_file in glob.glob(os.path.join(output_dir, "p*_*.json")):
    os.remove(old_file)
    print(f"Removed: {os.path.basename(old_file)}")

# Write the combined dashboard
filepath = os.path.join(output_dir, "asap-comparison.json")
with open(filepath, "w") as f:
    json.dump(dashboard, f, indent=2)

print("\nCreated: asap-comparison.json")
print(f"Total panels: {len(panels)}")
print(f"Total rows: {len(queries)}")
print(f"Layout: {len(queries)} rows × 2 panels per row")
