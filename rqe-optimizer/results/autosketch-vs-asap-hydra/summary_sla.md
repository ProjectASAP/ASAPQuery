# Version 2: cost by use under a batch latency SLA

Each method's cost by use (`w_cpu · AUC(CPU) + w_mem · AUC(memory)`) at each SLA; ASAP and PerQuery plan for the SLA (the cheapest plan whose batch latency, the longest chain, is at most it), AutoSketch's one plan ignores it.

## mixed: 42 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 169 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 33.4 | 96.9 | 33.4 | 12.2 | yes |
| cpu | 100 | ASAP (no roll-ups) | 33.4 | 96.9 | 33.4 | 12.2 | yes |
| cpu | 100 | PerQuery-CostAware | 56 | 97.4 | 56 | 40.2 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.7e+03 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| cpu | 300 | ASAP | 10.7 | 295 | 10.7 | 10.4 | yes |
| cpu | 300 | ASAP (no roll-ups) | 10.7 | 295 | 10.7 | 10.4 | yes |
| cpu | 300 | PerQuery-CostAware | 19.4 | 295 | 19.4 | 36.8 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.7e+03 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.49 | 989 | 4.49 | 10.8 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.49 | 989 | 4.49 | 10.8 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 10.1 | 989 | 10.1 | 37.9 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.7e+03 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.74 | 2.68e+03 | 2.74 | 11.7 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 2.74 | 2.68e+03 | 2.74 | 11.7 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 7.78 | 2.97e+03 | 7.78 | 38.5 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.7e+03 | 1.01e+03 | 2.7e+03 | 4e+03 | yes |
| cpu | 1e+04 | ASAP | 2.74 | 2.68e+03 | 2.74 | 11.7 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 2.74 | 2.68e+03 | 2.74 | 11.7 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 7.67 | 3.33e+03 | 7.67 | 39.8 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.7e+03 | 1.01e+03 | 2.7e+03 | 4e+03 | yes |
| fargate | 100 | ASAP | 1.41 | 96.9 | 33.4 | 12.2 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.41 | 96.9 | 33.4 | 12.2 | yes |
| fargate | 100 | PerQuery-CostAware | 2.45 | 97.4 | 56 | 40.2 | yes |
| fargate | 100 | AutoSketch-Adapted | 127 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| fargate | 300 | ASAP | 0.48 | 295 | 10.7 | 10.4 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.48 | 295 | 10.7 | 10.4 | yes |
| fargate | 300 | PerQuery-CostAware | 0.951 | 295 | 19.4 | 36.8 | yes |
| fargate | 300 | AutoSketch-Adapted | 127 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.23 | 989 | 4.49 | 10.8 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.23 | 989 | 4.49 | 10.8 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.576 | 989 | 10.1 | 37.9 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 127 | 1.01e+03 | 2.7e+03 | 4e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.163 | 2.68e+03 | 2.74 | 11.7 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.163 | 2.68e+03 | 2.74 | 11.7 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.487 | 2.97e+03 | 7.78 | 38.5 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 127 | 1.01e+03 | 2.7e+03 | 4e+03 | yes |
| fargate | 1e+04 | ASAP | 0.163 | 2.68e+03 | 2.74 | 11.7 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.163 | 2.68e+03 | 2.74 | 11.7 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.487 | 2.97e+03 | 7.78 | 38.5 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 127 | 1.01e+03 | 2.7e+03 | 4e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 11 | 11 | 33.4 | 33.4 | 0.0% |
| cpu | 300 | 0 | 11 | 11 | 10.7 | 10.7 | 0.0% |
| cpu | 1e+03 | 0 | 8 | 8 | 4.49 | 4.49 | 0.0% |
| cpu | 3e+03 | 0 | 7 | 7 | 2.74 | 2.74 | 0.0% |
| cpu | 1e+04 | 0 | 7 | 7 | 2.74 | 2.74 | 0.0% |
| fargate | 100 | 0 | 11 | 11 | 1.41 | 1.41 | 0.0% |
| fargate | 300 | 0 | 11 | 11 | 0.48 | 0.48 | 0.0% |
| fargate | 1e+03 | 0 | 8 | 8 | 0.23 | 0.23 | 0.0% |
| fargate | 3e+03 | 0 | 7 | 7 | 0.163 | 0.163 | 0.0% |
| fargate | 1e+04 | 0 | 7 | 7 | 0.163 | 0.163 | 0.0% |

## multi-grouping: 62 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 227 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 9.65 | 490 | 9.65 | 3.51 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 9.65 | 490 | 9.65 | 3.51 | **no** |
| cpu | 1e+03 | ASAP | 0.22 | 557 | 0.22 | 1.01 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 0.55 | 959 | 0.55 | 2.47 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.51 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 9.65 | 490 | 9.65 | 3.51 | yes |
| cpu | 3e+03 | ASAP | 0.22 | 557 | 0.22 | 1.01 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 0.55 | 959 | 0.55 | 2.47 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.51 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 9.65 | 490 | 9.65 | 3.51 | yes |
| cpu | 1e+04 | ASAP | 0.22 | 557 | 0.22 | 1.01 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 0.55 | 959 | 0.55 | 2.47 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.51 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 9.65 | 490 | 9.65 | 3.51 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 0.406 | 490 | 9.65 | 3.51 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 0.406 | 490 | 9.65 | 3.51 | **no** |
| fargate | 1e+03 | ASAP | 0.0134 | 557 | 0.22 | 1.01 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.0333 | 959 | 0.55 | 2.47 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.0589 | 959 | 1.07 | 3.51 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 0.406 | 490 | 9.65 | 3.51 | yes |
| fargate | 3e+03 | ASAP | 0.0134 | 557 | 0.22 | 1.01 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.0333 | 959 | 0.55 | 2.47 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.0589 | 959 | 1.07 | 3.51 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 0.406 | 490 | 9.65 | 3.51 | yes |
| fargate | 1e+04 | ASAP | 0.0134 | 557 | 0.22 | 1.01 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.0333 | 959 | 0.55 | 2.47 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.0589 | 959 | 1.07 | 3.51 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 0.406 | 490 | 9.65 | 3.51 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 52 | 5 | 26 | 0.22 | 0.55 | 60.0% |
| cpu | 3e+03 | 52 | 5 | 26 | 0.22 | 0.55 | 60.0% |
| cpu | 1e+04 | 52 | 5 | 26 | 0.22 | 0.55 | 60.0% |
| fargate | 1e+03 | 52 | 5 | 26 | 0.0134 | 0.0333 | 59.7% |
| fargate | 3e+03 | 52 | 5 | 26 | 0.0134 | 0.0333 | 59.7% |
| fargate | 1e+04 | 52 | 5 | 26 | 0.0134 | 0.0333 | 59.7% |

## mixed, r = 8: 76 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 169 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 33.4 | 96.9 | 33.4 | 12.2 | yes |
| cpu | 100 | ASAP (no roll-ups) | 33.4 | 96.9 | 33.4 | 12.2 | yes |
| cpu | 100 | PerQuery-CostAware | 68.9 | 97.4 | 68.9 | 47.3 | yes |
| cpu | 100 | AutoSketch-Adapted | 3.24e+03 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| cpu | 300 | ASAP | 10.7 | 295 | 10.7 | 10.4 | yes |
| cpu | 300 | ASAP (no roll-ups) | 10.7 | 295 | 10.7 | 10.4 | yes |
| cpu | 300 | PerQuery-CostAware | 26.7 | 295 | 26.7 | 43.9 | yes |
| cpu | 300 | AutoSketch-Adapted | 3.24e+03 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.52 | 989 | 4.52 | 10.8 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.52 | 989 | 4.52 | 10.8 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 16 | 989 | 16 | 45.2 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.24e+03 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.8 | 2.68e+03 | 2.8 | 11.7 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 2.8 | 2.68e+03 | 2.8 | 11.7 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 13.6 | 2.97e+03 | 13.6 | 45.8 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.24e+03 | 1.01e+03 | 3.24e+03 | 4.16e+03 | yes |
| cpu | 1e+04 | ASAP | 2.8 | 2.68e+03 | 2.8 | 11.7 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 2.8 | 2.68e+03 | 2.8 | 11.7 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 13.5 | 3.33e+03 | 13.5 | 47 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.24e+03 | 1.01e+03 | 3.24e+03 | 4.16e+03 | yes |
| fargate | 100 | ASAP | 1.41 | 96.9 | 33.4 | 12.2 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.41 | 96.9 | 33.4 | 12.2 | yes |
| fargate | 100 | PerQuery-CostAware | 3 | 97.4 | 68.9 | 47.3 | yes |
| fargate | 100 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| fargate | 300 | ASAP | 0.481 | 295 | 10.7 | 10.4 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.481 | 295 | 10.7 | 10.4 | yes |
| fargate | 300 | PerQuery-CostAware | 1.28 | 295 | 26.7 | 43.9 | yes |
| fargate | 300 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.231 | 989 | 4.52 | 10.8 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.231 | 989 | 4.52 | 10.8 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.851 | 989 | 16 | 45.2 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.24e+03 | 4.16e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.165 | 2.68e+03 | 2.8 | 11.7 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.165 | 2.68e+03 | 2.8 | 11.7 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.754 | 2.97e+03 | 13.6 | 45.8 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.24e+03 | 4.16e+03 | yes |
| fargate | 1e+04 | ASAP | 0.165 | 2.68e+03 | 2.8 | 11.7 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.165 | 2.68e+03 | 2.8 | 11.7 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.754 | 2.97e+03 | 13.6 | 45.8 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.24e+03 | 4.16e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 11 | 11 | 33.4 | 33.4 | 0.0% |
| cpu | 300 | 0 | 11 | 11 | 10.7 | 10.7 | 0.0% |
| cpu | 1e+03 | 0 | 8 | 8 | 4.52 | 4.52 | 0.0% |
| cpu | 3e+03 | 0 | 7 | 7 | 2.8 | 2.8 | 0.0% |
| cpu | 1e+04 | 0 | 7 | 7 | 2.8 | 2.8 | 0.0% |
| fargate | 100 | 0 | 11 | 11 | 1.41 | 1.41 | 0.0% |
| fargate | 300 | 0 | 11 | 11 | 0.481 | 0.481 | 0.0% |
| fargate | 1e+03 | 0 | 8 | 8 | 0.231 | 0.231 | 0.0% |
| fargate | 3e+03 | 0 | 7 | 7 | 0.165 | 0.165 | 0.0% |
| fargate | 1e+04 | 0 | 7 | 7 | 0.165 | 0.165 | 0.0% |

## mixed + multi-grouping: 104 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 396 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 2.71e+03 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 2.71e+03 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.71 | 989 | 4.71 | 11.8 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 5.04 | 989 | 5.04 | 13.2 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 11.1 | 989 | 11.1 | 41.4 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.71e+03 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.96 | 2.68e+03 | 2.96 | 12.7 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 3.29 | 2.68e+03 | 3.29 | 14.2 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 8.85 | 2.97e+03 | 8.85 | 42 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.71e+03 | 1.01e+03 | 2.71e+03 | 4.01e+03 | yes |
| cpu | 1e+04 | ASAP | 2.96 | 2.68e+03 | 2.96 | 12.7 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 3.29 | 2.68e+03 | 3.29 | 14.2 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 8.74 | 3.33e+03 | 8.74 | 43.3 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.71e+03 | 1.01e+03 | 2.71e+03 | 4.01e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 128 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 128 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.243 | 989 | 4.71 | 11.8 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.263 | 989 | 5.04 | 13.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.635 | 989 | 11.1 | 41.4 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 128 | 1.01e+03 | 2.71e+03 | 4.01e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.177 | 2.68e+03 | 2.96 | 12.7 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.197 | 2.68e+03 | 3.29 | 14.2 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.545 | 2.97e+03 | 8.85 | 42 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 128 | 1.01e+03 | 2.71e+03 | 4.01e+03 | yes |
| fargate | 1e+04 | ASAP | 0.177 | 2.68e+03 | 2.96 | 12.7 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.197 | 2.68e+03 | 3.29 | 14.2 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.545 | 2.97e+03 | 8.85 | 42 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 128 | 1.01e+03 | 2.71e+03 | 4.01e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 52 | 13 | 34 | 4.71 | 5.04 | 6.6% |
| cpu | 3e+03 | 52 | 12 | 33 | 2.96 | 3.29 | 10.0% |
| cpu | 1e+04 | 52 | 12 | 33 | 2.96 | 3.29 | 10.0% |
| fargate | 1e+03 | 52 | 13 | 34 | 0.243 | 0.263 | 7.6% |
| fargate | 3e+03 | 52 | 12 | 33 | 0.177 | 0.197 | 10.1% |
| fargate | 1e+04 | 52 | 12 | 33 | 0.177 | 0.197 | 10.1% |

## multi-grouping, r = 8: 124 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 227 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 11.6 | 490 | 11.6 | 4.46 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 11.6 | 490 | 11.6 | 4.46 | **no** |
| cpu | 1e+03 | ASAP | 0.241 | 557 | 0.241 | 1.01 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 0.572 | 959 | 0.572 | 2.47 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.47 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 11.6 | 490 | 11.6 | 4.46 | yes |
| cpu | 3e+03 | ASAP | 0.241 | 557 | 0.241 | 1.01 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 0.572 | 959 | 0.572 | 2.47 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.47 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 11.6 | 490 | 11.6 | 4.46 | yes |
| cpu | 1e+04 | ASAP | 0.241 | 557 | 0.241 | 1.01 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 0.572 | 959 | 0.572 | 2.47 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.47 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 11.6 | 490 | 11.6 | 4.46 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 0.489 | 490 | 11.6 | 4.46 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 0.489 | 490 | 11.6 | 4.46 | **no** |
| fargate | 1e+03 | ASAP | 0.0143 | 557 | 0.241 | 1.01 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.0342 | 959 | 0.572 | 2.47 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.103 | 959 | 2.04 | 4.47 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 0.489 | 490 | 11.6 | 4.46 | yes |
| fargate | 3e+03 | ASAP | 0.0143 | 557 | 0.241 | 1.01 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.0342 | 959 | 0.572 | 2.47 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.103 | 959 | 2.04 | 4.47 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 0.489 | 490 | 11.6 | 4.46 | yes |
| fargate | 1e+04 | ASAP | 0.0143 | 557 | 0.241 | 1.01 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.0342 | 959 | 0.572 | 2.47 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.103 | 959 | 2.04 | 4.47 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 0.489 | 490 | 11.6 | 4.46 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 104 | 5 | 26 | 0.241 | 0.572 | 57.9% |
| cpu | 3e+03 | 104 | 5 | 26 | 0.241 | 0.572 | 57.9% |
| cpu | 1e+04 | 104 | 5 | 26 | 0.241 | 0.572 | 57.9% |
| fargate | 1e+03 | 104 | 5 | 26 | 0.0143 | 0.0342 | 58.2% |
| fargate | 3e+03 | 104 | 5 | 26 | 0.0143 | 0.0342 | 58.2% |
| fargate | 1e+04 | 104 | 5 | 26 | 0.0143 | 0.0342 | 58.2% |

## mixed + multi-grouping, r = 8: 200 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 396 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 3.25e+03 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 3.25e+03 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.76 | 989 | 4.76 | 11.8 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 5.09 | 989 | 5.09 | 13.2 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 18.1 | 989 | 18.1 | 49.6 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.25e+03 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.04 | 2.68e+03 | 3.04 | 12.7 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 3.37 | 2.68e+03 | 3.37 | 14.2 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 15.6 | 2.97e+03 | 15.6 | 50.2 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.25e+03 | 1.01e+03 | 3.25e+03 | 4.17e+03 | yes |
| cpu | 1e+04 | ASAP | 3.04 | 2.68e+03 | 3.04 | 12.7 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 3.37 | 2.68e+03 | 3.37 | 14.2 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 15.5 | 3.33e+03 | 15.5 | 51.5 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.25e+03 | 1.01e+03 | 3.25e+03 | 4.17e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.245 | 989 | 4.76 | 11.8 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.265 | 989 | 5.09 | 13.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.953 | 989 | 18.1 | 49.6 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.25e+03 | 4.17e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.18 | 2.68e+03 | 3.04 | 12.7 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.2 | 2.68e+03 | 3.37 | 14.2 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.856 | 2.97e+03 | 15.6 | 50.2 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.25e+03 | 4.17e+03 | yes |
| fargate | 1e+04 | ASAP | 0.18 | 2.68e+03 | 3.04 | 12.7 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.2 | 2.68e+03 | 3.37 | 14.2 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.856 | 2.97e+03 | 15.6 | 50.2 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 150 | 1.01e+03 | 3.25e+03 | 4.17e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 104 | 13 | 34 | 4.76 | 5.09 | 6.5% |
| cpu | 3e+03 | 104 | 12 | 33 | 3.04 | 3.37 | 9.8% |
| cpu | 1e+04 | 104 | 12 | 33 | 3.04 | 3.37 | 9.8% |
| fargate | 1e+03 | 104 | 13 | 34 | 0.245 | 0.265 | 7.5% |
| fargate | 3e+03 | 104 | 12 | 33 | 0.18 | 0.2 | 10.0% |
| fargate | 1e+04 | 104 | 12 | 33 | 0.18 | 0.2 | 10.0% |

## mixed, m = 8: 336 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 1.36e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 267 | 96.9 | 267 | 97.7 | yes |
| cpu | 100 | ASAP (no roll-ups) | 267 | 96.9 | 267 | 97.7 | yes |
| cpu | 100 | PerQuery-CostAware | 448 | 97.4 | 448 | 322 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.16e+04 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| cpu | 300 | ASAP | 85.8 | 295 | 85.8 | 83.2 | yes |
| cpu | 300 | ASAP (no roll-ups) | 85.8 | 295 | 85.8 | 83.2 | yes |
| cpu | 300 | PerQuery-CostAware | 155 | 295 | 155 | 294 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.16e+04 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| cpu | 1e+03 | ASAP | 35.9 | 989 | 35.9 | 86.2 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 35.9 | 989 | 35.9 | 86.2 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 80.5 | 989 | 80.5 | 303 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.16e+04 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| cpu | 3e+03 | ASAP | 22 | 2.68e+03 | 22 | 93.7 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 22 | 2.68e+03 | 22 | 93.7 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 62.3 | 2.97e+03 | 62.3 | 308 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.16e+04 | 1.01e+03 | 2.16e+04 | 3.2e+04 | yes |
| cpu | 1e+04 | ASAP | 22 | 2.68e+03 | 22 | 93.7 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 22 | 2.68e+03 | 22 | 93.7 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 61.3 | 3.33e+03 | 61.3 | 318 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.16e+04 | 1.01e+03 | 2.16e+04 | 3.2e+04 | yes |
| fargate | 100 | ASAP | 11.3 | 96.9 | 267 | 97.7 | yes |
| fargate | 100 | ASAP (no roll-ups) | 11.3 | 96.9 | 267 | 97.7 | yes |
| fargate | 100 | PerQuery-CostAware | 19.6 | 97.4 | 448 | 322 | yes |
| fargate | 100 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| fargate | 300 | ASAP | 3.84 | 295 | 85.8 | 83.2 | yes |
| fargate | 300 | ASAP (no roll-ups) | 3.84 | 295 | 85.8 | 83.2 | yes |
| fargate | 300 | PerQuery-CostAware | 7.61 | 295 | 155 | 294 | yes |
| fargate | 300 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| fargate | 1e+03 | ASAP | 1.84 | 989 | 35.9 | 86.2 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 1.84 | 989 | 35.9 | 86.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 4.61 | 989 | 80.5 | 303 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.16e+04 | 3.2e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.31 | 2.68e+03 | 22 | 93.7 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 1.31 | 2.68e+03 | 22 | 93.7 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 3.89 | 2.97e+03 | 62.3 | 308 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.16e+04 | 3.2e+04 | yes |
| fargate | 1e+04 | ASAP | 1.31 | 2.68e+03 | 22 | 93.7 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 1.31 | 2.68e+03 | 22 | 93.7 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 3.89 | 2.97e+03 | 62.3 | 308 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.16e+04 | 3.2e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 88 | 88 | 267 | 267 | 0.0% |
| cpu | 300 | 0 | 88 | 88 | 85.8 | 85.8 | 0.0% |
| cpu | 1e+03 | 0 | 64 | 64 | 35.9 | 35.9 | 0.0% |
| cpu | 3e+03 | 0 | 56 | 56 | 22 | 22 | 0.0% |
| cpu | 1e+04 | 0 | 56 | 56 | 22 | 22 | 0.0% |
| fargate | 100 | 0 | 88 | 88 | 11.3 | 11.3 | 0.0% |
| fargate | 300 | 0 | 88 | 88 | 3.84 | 3.84 | 0.0% |
| fargate | 1e+03 | 0 | 64 | 64 | 1.84 | 1.84 | 0.0% |
| fargate | 3e+03 | 0 | 56 | 56 | 1.31 | 1.31 | 0.0% |
| fargate | 1e+04 | 0 | 56 | 56 | 1.31 | 1.31 | 0.0% |

## multi-grouping, m = 8: 496 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 1.81e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 77.2 | 490 | 77.2 | 28.1 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 77.2 | 490 | 77.2 | 28.1 | **no** |
| cpu | 1e+03 | ASAP | 1.76 | 557 | 1.76 | 8.12 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.4 | 959 | 4.4 | 19.8 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 8.54 | 959 | 8.54 | 28.1 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 77.2 | 490 | 77.2 | 28.1 | yes |
| cpu | 3e+03 | ASAP | 1.76 | 557 | 1.76 | 8.12 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 4.4 | 959 | 4.4 | 19.8 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 8.54 | 959 | 8.54 | 28.1 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 77.2 | 490 | 77.2 | 28.1 | yes |
| cpu | 1e+04 | ASAP | 1.76 | 557 | 1.76 | 8.12 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 4.4 | 959 | 4.4 | 19.8 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 8.54 | 959 | 8.54 | 28.1 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 77.2 | 490 | 77.2 | 28.1 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 3.25 | 490 | 77.2 | 28.1 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 3.25 | 490 | 77.2 | 28.1 | **no** |
| fargate | 1e+03 | ASAP | 0.107 | 557 | 1.76 | 8.12 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.266 | 959 | 4.4 | 19.8 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.471 | 959 | 8.54 | 28.1 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 3.25 | 490 | 77.2 | 28.1 | yes |
| fargate | 3e+03 | ASAP | 0.107 | 557 | 1.76 | 8.12 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.266 | 959 | 4.4 | 19.8 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.471 | 959 | 8.54 | 28.1 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 3.25 | 490 | 77.2 | 28.1 | yes |
| fargate | 1e+04 | ASAP | 0.107 | 557 | 1.76 | 8.12 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.266 | 959 | 4.4 | 19.8 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.471 | 959 | 8.54 | 28.1 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 3.25 | 490 | 77.2 | 28.1 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 416 | 40 | 208 | 1.76 | 4.4 | 60.0% |
| cpu | 3e+03 | 416 | 40 | 208 | 1.76 | 4.4 | 60.0% |
| cpu | 1e+04 | 416 | 40 | 208 | 1.76 | 4.4 | 60.0% |
| fargate | 1e+03 | 416 | 40 | 208 | 0.107 | 0.266 | 59.7% |
| fargate | 3e+03 | 416 | 40 | 208 | 0.107 | 0.266 | 59.7% |
| fargate | 1e+04 | 416 | 40 | 208 | 0.107 | 0.266 | 59.7% |

## mixed, m = 16: 672 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 2.71e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 535 | 96.9 | 535 | 195 | yes |
| cpu | 100 | ASAP (no roll-ups) | 535 | 96.9 | 535 | 195 | yes |
| cpu | 100 | PerQuery-CostAware | 896 | 97.4 | 896 | 644 | yes |
| cpu | 100 | AutoSketch-Adapted | 4.32e+04 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| cpu | 300 | ASAP | 172 | 295 | 172 | 166 | yes |
| cpu | 300 | ASAP (no roll-ups) | 172 | 295 | 172 | 166 | yes |
| cpu | 300 | PerQuery-CostAware | 311 | 295 | 311 | 589 | yes |
| cpu | 300 | AutoSketch-Adapted | 4.32e+04 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| cpu | 1e+03 | ASAP | 71.8 | 989 | 71.8 | 172 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 71.8 | 989 | 71.8 | 172 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 161 | 989 | 161 | 606 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.32e+04 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| cpu | 3e+03 | ASAP | 43.9 | 2.68e+03 | 43.9 | 187 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 43.9 | 2.68e+03 | 43.9 | 187 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 125 | 2.97e+03 | 125 | 616 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.32e+04 | 1.01e+03 | 4.32e+04 | 6.4e+04 | yes |
| cpu | 1e+04 | ASAP | 43.9 | 2.68e+03 | 43.9 | 187 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 43.9 | 2.68e+03 | 43.9 | 187 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 123 | 3.33e+03 | 123 | 636 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.32e+04 | 1.01e+03 | 4.32e+04 | 6.4e+04 | yes |
| fargate | 100 | ASAP | 22.5 | 96.9 | 535 | 195 | yes |
| fargate | 100 | ASAP (no roll-ups) | 22.5 | 96.9 | 535 | 195 | yes |
| fargate | 100 | PerQuery-CostAware | 39.2 | 97.4 | 896 | 644 | yes |
| fargate | 100 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| fargate | 300 | ASAP | 7.69 | 295 | 172 | 166 | yes |
| fargate | 300 | ASAP (no roll-ups) | 7.69 | 295 | 172 | 166 | yes |
| fargate | 300 | PerQuery-CostAware | 15.2 | 295 | 311 | 589 | yes |
| fargate | 300 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| fargate | 1e+03 | ASAP | 3.67 | 989 | 71.8 | 172 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 3.67 | 989 | 71.8 | 172 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 9.22 | 989 | 161 | 606 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.32e+04 | 6.4e+04 | **no** |
| fargate | 3e+03 | ASAP | 2.61 | 2.68e+03 | 43.9 | 187 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 2.61 | 2.68e+03 | 43.9 | 187 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 7.78 | 2.97e+03 | 125 | 616 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.32e+04 | 6.4e+04 | yes |
| fargate | 1e+04 | ASAP | 2.61 | 2.68e+03 | 43.9 | 187 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 2.61 | 2.68e+03 | 43.9 | 187 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 7.78 | 2.97e+03 | 125 | 616 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.32e+04 | 6.4e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 176 | 176 | 535 | 535 | 0.0% |
| cpu | 300 | 0 | 176 | 176 | 172 | 172 | 0.0% |
| cpu | 1e+03 | 0 | 128 | 128 | 71.8 | 71.8 | 0.0% |
| cpu | 3e+03 | 0 | 112 | 112 | 43.9 | 43.9 | 0.0% |
| cpu | 1e+04 | 0 | 112 | 112 | 43.9 | 43.9 | 0.0% |
| fargate | 100 | 0 | 176 | 176 | 22.5 | 22.5 | 0.0% |
| fargate | 300 | 0 | 176 | 176 | 7.69 | 7.69 | 0.0% |
| fargate | 1e+03 | 0 | 128 | 128 | 3.67 | 3.67 | 0.0% |
| fargate | 3e+03 | 0 | 112 | 112 | 2.61 | 2.61 | 0.0% |
| fargate | 1e+04 | 0 | 112 | 112 | 2.61 | 2.61 | 0.0% |

## mixed + multi-grouping, m = 8: 832 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.17e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 2.17e+04 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 2.17e+04 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| cpu | 1e+03 | ASAP | 37.6 | 989 | 37.6 | 94.3 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 40.3 | 989 | 40.3 | 106 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 89 | 989 | 89 | 331 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.17e+04 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| cpu | 3e+03 | ASAP | 23.7 | 2.68e+03 | 23.7 | 102 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 26.4 | 2.68e+03 | 26.4 | 113 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 70.8 | 2.97e+03 | 70.8 | 336 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.17e+04 | 1.01e+03 | 2.17e+04 | 3.2e+04 | yes |
| cpu | 1e+04 | ASAP | 23.7 | 2.68e+03 | 23.7 | 102 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 26.4 | 2.68e+03 | 26.4 | 113 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 69.9 | 3.33e+03 | 69.9 | 346 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.17e+04 | 1.01e+03 | 2.17e+04 | 3.2e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| fargate | 1e+03 | ASAP | 1.94 | 989 | 37.6 | 94.3 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 2.1 | 989 | 40.3 | 106 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 5.08 | 989 | 89 | 331 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.17e+04 | 3.2e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.41 | 2.68e+03 | 23.7 | 102 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 1.57 | 2.68e+03 | 26.4 | 113 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 4.36 | 2.97e+03 | 70.8 | 336 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.17e+04 | 3.2e+04 | yes |
| fargate | 1e+04 | ASAP | 1.41 | 2.68e+03 | 23.7 | 102 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 1.57 | 2.68e+03 | 26.4 | 113 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 4.36 | 2.97e+03 | 70.8 | 336 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.02e+03 | 1.01e+03 | 2.17e+04 | 3.2e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 416 | 104 | 272 | 37.6 | 40.3 | 6.6% |
| cpu | 3e+03 | 416 | 96 | 264 | 23.7 | 26.4 | 10.0% |
| cpu | 1e+04 | 416 | 96 | 264 | 23.7 | 26.4 | 10.0% |
| fargate | 1e+03 | 416 | 104 | 272 | 1.94 | 2.1 | 7.6% |
| fargate | 3e+03 | 416 | 96 | 264 | 1.41 | 1.57 | 10.1% |
| fargate | 1e+04 | 416 | 96 | 264 | 1.41 | 1.57 | 10.1% |

## multi-grouping, m = 16: 992 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 3.62e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 154 | 490 | 154 | 56.1 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 154 | 490 | 154 | 56.1 | **no** |
| cpu | 1e+03 | ASAP | 3.52 | 557 | 3.52 | 16.2 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 8.8 | 959 | 8.8 | 39.5 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 56.2 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 154 | 490 | 154 | 56.1 | yes |
| cpu | 3e+03 | ASAP | 3.52 | 557 | 3.52 | 16.2 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 8.8 | 959 | 8.8 | 39.5 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 56.2 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 154 | 490 | 154 | 56.1 | yes |
| cpu | 1e+04 | ASAP | 3.52 | 557 | 3.52 | 16.2 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 8.8 | 959 | 8.8 | 39.5 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 56.2 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 154 | 490 | 154 | 56.1 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 6.5 | 490 | 154 | 56.1 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 6.5 | 490 | 154 | 56.1 | **no** |
| fargate | 1e+03 | ASAP | 0.215 | 557 | 3.52 | 16.2 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.532 | 959 | 8.8 | 39.5 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.942 | 959 | 17.1 | 56.2 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 6.5 | 490 | 154 | 56.1 | yes |
| fargate | 3e+03 | ASAP | 0.215 | 557 | 3.52 | 16.2 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.532 | 959 | 8.8 | 39.5 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.942 | 959 | 17.1 | 56.2 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 6.5 | 490 | 154 | 56.1 | yes |
| fargate | 1e+04 | ASAP | 0.215 | 557 | 3.52 | 16.2 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.532 | 959 | 8.8 | 39.5 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.942 | 959 | 17.1 | 56.2 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 6.5 | 490 | 154 | 56.1 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 832 | 80 | 416 | 3.52 | 8.8 | 60.0% |
| cpu | 3e+03 | 832 | 80 | 416 | 3.52 | 8.8 | 60.0% |
| cpu | 1e+04 | 832 | 80 | 416 | 3.52 | 8.8 | 60.0% |
| fargate | 1e+03 | 832 | 80 | 416 | 0.215 | 0.532 | 59.7% |
| fargate | 3e+03 | 832 | 80 | 416 | 0.215 | 0.532 | 59.7% |
| fargate | 1e+04 | 832 | 80 | 416 | 0.215 | 0.532 | 59.7% |

## mixed + multi-grouping, m = 16: 1664 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 6.34e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 4.34e+04 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 4.34e+04 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| cpu | 1e+03 | ASAP | 75.3 | 989 | 75.3 | 189 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 80.6 | 989 | 80.6 | 212 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 178 | 989 | 178 | 663 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.34e+04 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| cpu | 3e+03 | ASAP | 47.4 | 2.68e+03 | 47.4 | 204 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 52.7 | 2.68e+03 | 52.7 | 227 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 142 | 2.97e+03 | 142 | 672 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.34e+04 | 1.01e+03 | 4.34e+04 | 6.41e+04 | yes |
| cpu | 1e+04 | ASAP | 47.4 | 2.68e+03 | 47.4 | 204 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 52.7 | 2.68e+03 | 52.7 | 227 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 140 | 3.33e+03 | 140 | 693 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.34e+04 | 1.01e+03 | 4.34e+04 | 6.41e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| fargate | 1e+03 | ASAP | 3.89 | 989 | 75.3 | 189 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 4.21 | 989 | 80.6 | 212 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 10.2 | 989 | 178 | 663 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.34e+04 | 6.41e+04 | **no** |
| fargate | 3e+03 | ASAP | 2.83 | 2.68e+03 | 47.4 | 204 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 3.14 | 2.68e+03 | 52.7 | 227 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 8.73 | 2.97e+03 | 142 | 672 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.34e+04 | 6.41e+04 | yes |
| fargate | 1e+04 | ASAP | 2.83 | 2.68e+03 | 47.4 | 204 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 3.14 | 2.68e+03 | 52.7 | 227 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 8.73 | 2.97e+03 | 142 | 672 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.04e+03 | 1.01e+03 | 4.34e+04 | 6.41e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 832 | 208 | 544 | 75.3 | 80.6 | 6.6% |
| cpu | 3e+03 | 832 | 192 | 528 | 47.4 | 52.7 | 10.0% |
| cpu | 1e+04 | 832 | 192 | 528 | 47.4 | 52.7 | 10.0% |
| fargate | 1e+03 | 832 | 208 | 544 | 3.89 | 4.21 | 7.6% |
| fargate | 3e+03 | 832 | 192 | 528 | 2.83 | 3.14 | 10.1% |
| fargate | 1e+04 | 832 | 192 | 528 | 2.83 | 3.14 | 10.1% |
