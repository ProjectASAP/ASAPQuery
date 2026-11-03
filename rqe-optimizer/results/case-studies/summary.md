## synthetic/case/anomaly/base=0/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 3.31 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/drilldown_endpoints=100/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0776 | 2 | 82.5 |
| 3.0 | infeasible | | |
| 10.0 | 0.151 | 4 | 4.75 |
| 30.0 | 0.113 | 3 | 16.4 |
| 100.0 | 0.0776 | 2 | 82.5 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

Plan at SLA 10.0:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,service,endpoint,status) window 300s slide 300s: p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 30.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (endpoint,region,service,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1), p99_by_region,endpoint[5m] (merges 1), p99_by_region,service,endpoint,status[5m] (merges 1)

## synthetic/case/anomaly/drilldown_endpoints=10/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0435 | 1 | 33.1 |
| 3.0 | 0.113 | 3 | 1.54 |
| 10.0 | 0.0757 | 2 | 8.19 |
| 30.0 | 0.0757 | 2 | 8.19 |
| 100.0 | 0.0435 | 1 | 33.1 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20), p99_by_service,region,status[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20), p99_by_region,service,endpoint,status[5m] (merges 20)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (endpoint,region,service,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1), p99_by_region,endpoint[5m] (merges 1), p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 10.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

## synthetic/case/anomaly/drilldown_endpoints=25/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0522 | 1 | 82.7 |
| 3.0 | 0.188 | 5 | 1.19 |
| 10.0 | 0.113 | 3 | 4.02 |
| 30.0 | 0.076 | 2 | 20.5 |
| 100.0 | 0.0522 | 1 | 82.7 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20), p99_by_service,region,status[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20), p99_by_region,service,endpoint,status[5m] (merges 20)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,service,endpoint,status) window 300s slide 300s: p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 10.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (endpoint,region,service,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1), p99_by_region,endpoint[5m] (merges 1), p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 30.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

## synthetic/case/anomaly/drilldown_endpoints=2/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0388 | 1 | 6.61 |
| 3.0 | 0.0755 | 2 | 1.64 |
| 10.0 | 0.0388 | 1 | 6.61 |
| 30.0 | 0.0388 | 1 | 6.61 |
| 100.0 | 0.0388 | 1 | 6.61 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20), p99_by_service,region,status[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20), p99_by_region,service,endpoint,status[5m] (merges 20)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

## synthetic/case/anomaly/drilldown_endpoints=50/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0668 | 1 | 165 |
| 3.0 | 0.188 | 5 | 2.38 |
| 10.0 | 0.113 | 3 | 8.15 |
| 30.0 | 0.113 | 3 | 8.15 |
| 100.0 | 0.0765 | 2 | 41.2 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20), p99_by_service,region,status[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20), p99_by_region,service,endpoint,status[5m] (merges 20)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,service,endpoint,status) window 300s slide 300s: p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 10.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (endpoint,region,service,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1), p99_by_region,endpoint[5m] (merges 1), p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 100.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

## synthetic/case/anomaly/drilldown_endpoints=5/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0406 | 1 | 16.5 |
| 3.0 | 0.113 | 3 | 0.825 |
| 10.0 | 0.0756 | 2 | 4.1 |
| 30.0 | 0.0406 | 1 | 16.5 |
| 100.0 | 0.0406 | 1 | 16.5 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20), p99_by_service,region,status[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20), p99_by_region,service,endpoint,status[5m] (merges 20)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (endpoint,region,service,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1), p99_by_region,endpoint[5m] (merges 1), p99_by_region,service,endpoint,status[5m] (merges 1)

Plan at SLA 10.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region,service,status) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5), p99_by_service,region,status[5m] (merges 5), p99_by_region,endpoint[5m] (merges 5), p99_by_region,service,endpoint,status[5m] (merges 5)

## synthetic/case/anomaly/endpoints=100/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 4.43 |
| 0.3 | infeasible | | |
| 0.5 | infeasible | | |
| 1.0 | infeasible | | |
| 2.0 | 0.301 | 4 | 1.19 |
| 3.0 | 0.301 | 4 | 1.19 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/endpoints=10/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 3.31 |
| 0.3 | 0.377 | 6 | 0.199 |
| 0.5 | 0.188 | 4 | 0.443 |
| 1.0 | 0.151 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 0.825 |
| 3.0 | 0.151 | 4 | 0.825 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/endpoints=25/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 3.31 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/endpoints=2/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 1.65 |
| 0.3 | 0.226 | 6 | 0.199 |
| 0.5 | 0.188 | 5 | 0.406 |
| 1.0 | 0.151 | 4 | 0.797 |
| 2.0 | 0.113 | 3 | 1.65 |
| 3.0 | 0.113 | 3 | 1.65 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (endpoint,region) window 15s slide 15s: p99_by_region[5m] (merges 20), p99_by_region,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/endpoints=50/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 3.31 |
| 0.3 | infeasible | | |
| 0.5 | infeasible | | |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.301 | 4 | 0.825 |
| 3.0 | 0.151 | 4 | 2.22 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/endpoints=5/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 3.31 |
| 0.3 | 0.226 | 6 | 0.222 |
| 0.5 | 0.188 | 4 | 0.412 |
| 1.0 | 0.151 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.03 |
| 3.0 | 0.151 | 4 | 1.03 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/interval=15/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.116 | 3 | 19.9 |
| 0.3 | 2.03 | 4 | 0.297 |
| 0.5 | 1.51 | 3 | 0.333 |
| 1.0 | 1.09 | 3 | 0.849 |
| 2.0 | 0.603 | 3 | 1.65 |
| 3.0 | 0.528 | 3 | 2.4 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 15s slide 15s: p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,endpoint) window 15s slide 15s: p99_by_region,endpoint[5m] (merges 20)

Plan at SLA 0.3:
- dd(alpha=0.1) by (region,service) window 60s slide 15s: p99_by_service[5m] (merges 5), p99_by_region[5m] (merges 5), p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 15s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 150s slide 15s: p99_by_service,region,status[5m] (merges 2)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 15s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service,status) window 150s slide 15s: p99_by_service[5m] (merges 2), p99_by_region[5m] (merges 2), p99_by_service,region[5m] (merges 2), p99_by_service,region,status[5m] (merges 2)
- dd(alpha=0.1) by (service,endpoint) window 150s slide 15s: p99_by_service,endpoint[5m] (merges 2)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 15s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service,status) window 60s slide 15s: p99_by_service[5m] (merges 5), p99_by_region[5m] (merges 5), p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 75s slide 15s: p99_by_service,endpoint[5m] (merges 4)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 15s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (endpoint,service) window 60s slide 15s: p99_by_service[5m] (merges 5), p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,service,status) window 30s slide 15s: p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10), p99_by_service,region,status[5m] (merges 10)
- dd(alpha=0.1) by (region,endpoint) window 150s slide 15s: p99_by_region,endpoint[5m] (merges 2)

Plan at SLA 3.0:
- dd(alpha=0.1) by (region,service,status) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10), p99_by_service,region,status[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 30s slide 15s: p99_by_service,endpoint[5m] (merges 10)
- dd(alpha=0.1) by (region,endpoint) window 150s slide 15s: p99_by_region,endpoint[5m] (merges 2)

## synthetic/case/anomaly/interval=300/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 0.297 |
| 0.3 | 0.113 | 3 | 0.297 |
| 0.5 | 0.113 | 3 | 0.297 |
| 1.0 | 0.113 | 3 | 0.297 |
| 2.0 | 0.113 | 3 | 0.297 |
| 3.0 | 0.113 | 3 | 0.297 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 300s slide 300s: p99_by_service[5m] (merges 1), p99_by_region[5m] (merges 1), p99_by_service,region[5m] (merges 1), p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 300s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/interval=60/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 4.43 |
| 0.3 | 0.565 | 3 | 0.297 |
| 0.5 | 0.565 | 3 | 0.297 |
| 1.0 | 0.414 | 3 | 0.825 |
| 2.0 | 0.264 | 3 | 1.11 |
| 3.0 | 0.264 | 3 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service[5m] (merges 5), p99_by_region[5m] (merges 5), p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 60s slide 60s: p99_by_region,endpoint[5m] (merges 5)

Plan at SLA 0.3:
- dd(alpha=0.1) by (region,service,status) window 300s slide 60s: p99_by_service[5m] (merges 1), p99_by_region[5m] (merges 1), p99_by_service,region[5m] (merges 1), p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 60s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (endpoint,service) window 300s slide 60s: p99_by_service[5m] (merges 1), p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_region[5m] (merges 5), p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 60s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service[5m] (merges 5), p99_by_region[5m] (merges 5), p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 60s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/rate=20000000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 1.13 | 3 | 3.31 |
| 0.3 | infeasible | | |
| 0.5 | 3.39 | 4 | 0.412 |
| 1.0 | 3.01 | 4 | 0.825 |
| 2.0 | 1.51 | 4 | 1.11 |
| 3.0 | 1.51 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/rate=2000000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 3.31 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/rate=250000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0146 | 3 | 3.31 |
| 0.3 | 0.0471 | 6 | 0.297 |
| 0.5 | 0.0424 | 4 | 0.412 |
| 1.0 | 0.0378 | 4 | 0.825 |
| 2.0 | 0.019 | 4 | 1.11 |
| 3.0 | 0.019 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/rate=500000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0288 | 3 | 3.31 |
| 0.3 | 0.0942 | 6 | 0.297 |
| 0.5 | 0.0848 | 4 | 0.412 |
| 1.0 | 0.0754 | 4 | 0.825 |
| 2.0 | 0.0378 | 4 | 1.11 |
| 3.0 | 0.0378 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/statuses=16/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.115 | 3 | 13.2 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/statuses=2/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.113 | 3 | 1.65 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 5 | 0.384 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.113 | 3 | 1.65 |
| 3.0 | 0.113 | 3 | 1.65 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 60s slide 60s: p99_by_service,region[5m] (merges 5), p99_by_service,region,status[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/statuses=4/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 3.31 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/anomaly/statuses=8/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.114 | 3 | 6.61 |
| 0.3 | 0.377 | 6 | 0.297 |
| 0.5 | 0.339 | 4 | 0.412 |
| 1.0 | 0.301 | 4 | 0.825 |
| 2.0 | 0.151 | 4 | 1.11 |
| 3.0 | 0.151 | 4 | 1.11 |

Plan at SLA none:
- dd(alpha=0.1) by (endpoint,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_service,endpoint[5m] (merges 20)
- dd(alpha=0.1) by (region,service,status) window 15s slide 15s: p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20), p99_by_service,region,status[5m] (merges 20)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.3:
- dd(alpha=0.1) by (service) window 15s slide 15s: p99_by_service[5m] (merges 20)
- dd(alpha=0.1) by (region) window 15s slide 15s: p99_by_region[5m] (merges 20)
- dd(alpha=0.1) by (service,region) window 60s slide 60s: p99_by_service,region[5m] (merges 5)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 0.5:
- dd(alpha=0.1) by (region,service) window 30s slide 15s: p99_by_service[5m] (merges 10), p99_by_region[5m] (merges 10), p99_by_service,region[5m] (merges 10)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 1.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 300s slide 60s: p99_by_service,endpoint[5m] (merges 1)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

Plan at SLA 2.0:
- dd(alpha=0.1) by (region,service) window 15s slide 15s: p99_by_service[5m] (merges 20), p99_by_region[5m] (merges 20), p99_by_service,region[5m] (merges 20)
- dd(alpha=0.1) by (service,endpoint) window 60s slide 60s: p99_by_service,endpoint[5m] (merges 5)
- dd(alpha=0.1) by (service,region,status) window 300s slide 300s: p99_by_service,region,status[5m] (merges 1)
- dd(alpha=0.1) by (region,endpoint) window 300s slide 300s: p99_by_region,endpoint[5m] (merges 1)

## synthetic/case/perf/base=0/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.105 | 2 | 2.38e+04 |
| 10.0 | 1.24 | 5 | 9.45 |
| 30.0 | 0.643 | 5 | 21.9 |
| 100.0 | 0.229 | 4 | 96.3 |
| 300.0 | 0.155 | 4 | 195 |
| 1000.0 | 0.126 | 3 | 493 |
| 3000.0 | 0.126 | 3 | 493 |
| 10000.0 | 0.122 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/long_interval=15/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.627 | 3 | 2.97e+03 |
| 10.0 | infeasible | | |
| 30.0 | infeasible | | |
| 100.0 | infeasible | | |
| 300.0 | 3.53 | 4 | 274 |
| 1000.0 | 1.07 | 3 | 989 |
| 3000.0 | 0.627 | 3 | 2.97e+03 |
| 10000.0 | 0.627 | 3 | 2.97e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 15s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15), quantile_over_time_p0.9[6h] (merges 180), quantile_over_time_p0.9[1d] (merges 720)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1350s slide 15s: quantile_over_time_p0.9[6h] (merges 16), quantile_over_time_p0.9[1d] (merges 64)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40)
- dd(alpha=0.1) by (series) window 360s slide 15s: quantile_over_time_p0.9[30m] (merges 5), quantile_over_time_p0.9[6h] (merges 60), quantile_over_time_p0.9[1d] (merges 240)

## synthetic/case/perf/long_interval=1800/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.105 | 2 | 2.38e+04 |
| 10.0 | 1.24 | 5 | 9.45 |
| 30.0 | 0.643 | 5 | 21.9 |
| 100.0 | 0.229 | 4 | 96.3 |
| 300.0 | 0.155 | 4 | 195 |
| 1000.0 | 0.126 | 3 | 493 |
| 3000.0 | 0.126 | 3 | 493 |
| 10000.0 | 0.122 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/long_interval=300/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.131 | 3 | 1.19e+03 |
| 10.0 | infeasible | | |
| 30.0 | 2.53 | 5 | 26 |
| 100.0 | 0.606 | 4 | 96.3 |
| 300.0 | 0.269 | 4 | 295 |
| 1000.0 | 0.166 | 3 | 592 |
| 3000.0 | 0.131 | 3 | 1.19e+03 |
| 10000.0 | 0.131 | 3 | 1.19e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 300s slide 300s: quantile_over_time_p0.9[6h] (merges 72), quantile_over_time_p0.9[1d] (merges 288)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 300s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 300s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 300s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1200s slide 300s: quantile_over_time_p0.9[6h] (merges 18), quantile_over_time_p0.9[1d] (merges 72)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 600s slide 300s: quantile_over_time_p0.9[6h] (merges 36), quantile_over_time_p0.9[1d] (merges 144)

## synthetic/case/perf/long_interval=60/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.216 | 3 | 2.97e+03 |
| 10.0 | infeasible | | |
| 30.0 | infeasible | | |
| 100.0 | 2.87 | 4 | 88 |
| 300.0 | 0.877 | 4 | 295 |
| 1000.0 | 0.327 | 3 | 989 |
| 3000.0 | 0.216 | 3 | 2.97e+03 |
| 10000.0 | 0.216 | 3 | 2.97e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15), quantile_over_time_p0.9[6h] (merges 180), quantile_over_time_p0.9[1d] (merges 720)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 4320s slide 60s: quantile_over_time_p0.9[6h] (merges 5), quantile_over_time_p0.9[1d] (merges 20)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1200s slide 60s: quantile_over_time_p0.9[6h] (merges 18), quantile_over_time_p0.9[1d] (merges 72)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40)
- dd(alpha=0.1) by (series) window 360s slide 60s: quantile_over_time_p0.9[30m] (merges 5), quantile_over_time_p0.9[6h] (merges 60), quantile_over_time_p0.9[1d] (merges 240)

## synthetic/case/perf/per_series=1000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.406 | 2 | 2.38e+04 |
| 10.0 | infeasible | | |
| 30.0 | 3.2 | 5 | 26 |
| 100.0 | 1.13 | 4 | 96.3 |
| 300.0 | 0.758 | 4 | 195 |
| 1000.0 | 0.578 | 3 | 493 |
| 3000.0 | 0.578 | 3 | 493 |
| 10000.0 | 0.574 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.001s slide 0.001s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/per_series=100/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0651 | 3 | 5.95e+03 |
| 10.0 | 0.622 | 5 | 9.45 |
| 30.0 | 0.322 | 5 | 21.9 |
| 100.0 | 0.116 | 4 | 96.3 |
| 300.0 | 0.0799 | 4 | 195 |
| 1000.0 | 0.0694 | 3 | 493 |
| 3000.0 | 0.0694 | 3 | 493 |
| 10000.0 | 0.0651 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.01s slide 0.01s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

## synthetic/case/perf/per_series=200/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.105 | 2 | 2.38e+04 |
| 10.0 | 1.24 | 5 | 9.45 |
| 30.0 | 0.643 | 5 | 21.9 |
| 100.0 | 0.229 | 4 | 96.3 |
| 300.0 | 0.155 | 4 | 195 |
| 1000.0 | 0.126 | 3 | 493 |
| 3000.0 | 0.126 | 3 | 493 |
| 10000.0 | 0.122 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/per_series=20/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0197 | 4 | 195 |
| 10.0 | 0.125 | 5 | 9.45 |
| 30.0 | 0.0663 | 5 | 21.9 |
| 100.0 | 0.0258 | 4 | 96.3 |
| 300.0 | 0.0197 | 4 | 195 |
| 1000.0 | 0.0197 | 4 | 195 |
| 3000.0 | 0.0197 | 4 | 195 |
| 10000.0 | 0.0197 | 4 | 195 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.05s slide 0.05s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.05s slide 0.05s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.05s slide 0.05s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.05s slide 0.05s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

## synthetic/case/perf/per_series=500/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.218 | 2 | 2.38e+04 |
| 10.0 | infeasible | | |
| 30.0 | 1.6 | 5 | 21.9 |
| 100.0 | 0.568 | 4 | 96.3 |
| 300.0 | 0.381 | 4 | 195 |
| 1000.0 | 0.295 | 3 | 493 |
| 3000.0 | 0.295 | 3 | 493 |
| 10000.0 | 0.291 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.002s slide 0.002s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/per_series=50/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.0368 | 3 | 5.95e+03 |
| 10.0 | 0.312 | 5 | 9.45 |
| 30.0 | 0.162 | 5 | 21.9 |
| 100.0 | 0.0597 | 4 | 96.3 |
| 300.0 | 0.0423 | 4 | 195 |
| 1000.0 | 0.0411 | 3 | 493 |
| 3000.0 | 0.0411 | 3 | 493 |
| 10000.0 | 0.0368 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.02s slide 0.02s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

## synthetic/case/perf/series=100000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 1.05 | 2 | 2.38e+05 |
| 10.0 | infeasible | | |
| 30.0 | infeasible | | |
| 100.0 | infeasible | | |
| 300.0 | infeasible | | |
| 1000.0 | 2.29 | 4 | 963 |
| 3000.0 | 1.55 | 4 | 1.95e+03 |
| 10000.0 | 1.26 | 3 | 4.93e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 3000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

## synthetic/case/perf/series=10000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.105 | 2 | 2.38e+04 |
| 10.0 | 1.24 | 5 | 9.45 |
| 30.0 | 0.643 | 5 | 21.9 |
| 100.0 | 0.229 | 4 | 96.3 |
| 300.0 | 0.155 | 4 | 195 |
| 1000.0 | 0.126 | 3 | 493 |
| 3000.0 | 0.126 | 3 | 493 |
| 10000.0 | 0.122 | 3 | 5.95e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 10.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 30s slide 15s: quantile_over_time_p0.5[1m] (merges 2), quantile_over_time_p0.9[1m] (merges 2)
- dd(alpha=0.1) by (series) window 600s slide 60s: quantile_over_time_p0.9[10m] (merges 1), quantile_over_time_p0.9[30m] (merges 3)
- dd(alpha=0.1) by (series) window 7200s slide 1800s: quantile_over_time_p0.9[6h] (merges 3)
- dd(alpha=0.1) by (series) window 28800s slide 1800s: quantile_over_time_p0.9[1d] (merges 3)

Plan at SLA 30.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6)
- dd(alpha=0.1) by (series) window 14400s slide 1800s: quantile_over_time_p0.9[1d] (merges 6)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 10000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30), quantile_over_time_p0.9[6h] (merges 360), quantile_over_time_p0.9[1d] (merges 1440)

## synthetic/case/perf/series=30000/tp95

| SLA (ms) | vCPU | MVs | latency (ms) |
|---|---|---|---|
| none | 0.314 | 2 | 7.14e+04 |
| 10.0 | infeasible | | |
| 30.0 | infeasible | | |
| 100.0 | 1.48 | 4 | 90.4 |
| 300.0 | 0.687 | 4 | 289 |
| 1000.0 | 0.466 | 4 | 586 |
| 3000.0 | 0.378 | 3 | 1.48e+03 |
| 10000.0 | 0.378 | 3 | 1.48e+03 |

Plan at SLA none:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120), quantile_over_time_p0.9[6h] (merges 1440), quantile_over_time_p0.9[1d] (merges 5760)

Plan at SLA 100.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 300s slide 60s: quantile_over_time_p0.9[10m] (merges 2), quantile_over_time_p0.9[30m] (merges 6)
- dd(alpha=0.1) by (series) window 10800s slide 1800s: quantile_over_time_p0.9[6h] (merges 2), quantile_over_time_p0.9[1d] (merges 8)

Plan at SLA 300.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 120s slide 60s: quantile_over_time_p0.9[10m] (merges 5), quantile_over_time_p0.9[30m] (merges 15)
- dd(alpha=0.1) by (series) window 3600s slide 1800s: quantile_over_time_p0.9[6h] (merges 6), quantile_over_time_p0.9[1d] (merges 24)

Plan at SLA 1000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4)
- dd(alpha=0.1) by (series) window 60s slide 60s: quantile_over_time_p0.9[10m] (merges 10), quantile_over_time_p0.9[30m] (merges 30)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)

Plan at SLA 3000.0:
- dd(alpha=0.1) by (job) window 0.005s slide 0.005s: quantile_by_job_p0.5[1 scrape] (merges 1), quantile_by_job_p0.9[1 scrape] (merges 1)
- dd(alpha=0.1) by (series) window 15s slide 15s: quantile_over_time_p0.5[1m] (merges 4), quantile_over_time_p0.9[1m] (merges 4), quantile_over_time_p0.9[10m] (merges 40), quantile_over_time_p0.9[30m] (merges 120)
- dd(alpha=0.1) by (series) window 1800s slide 1800s: quantile_over_time_p0.9[6h] (merges 12), quantile_over_time_p0.9[1d] (merges 48)
