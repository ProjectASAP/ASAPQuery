# SketchDB Prometheus Exporters

This repository contains multiple Prometheus exporters for exposing various types of metrics that can be scraped by a Prometheus server.

## Available Exporters

- **Cluster Data Exporter** (Rust) - Exposes cluster resource usage metrics from Google and Alibaba cluster trace datasets
- **Fake Exporter** (Rust or Python) - Generates synthetic, pseudorandom Prometheus metrics
- **Query Cost Exporter** (Python) - Exports query cost metrics and resource usage statistics
- **Query Latency Exporter** (Python) - Monitors and exports query latency metrics

## Metrics Endpoint

All exporters expose metrics at:
```
http://localhost:<port>/metrics
```
