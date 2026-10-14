# Version 2: cost by use under a batch latency SLA

Each method's cost by use (`w_cpu · AUC(CPU) + w_mem · AUC(memory)`) at each SLA; ASAP and PerQuery plan for the SLA (the cheapest plan whose batch latency, the longest chain, is at most it), AutoSketch's one plan ignores it.

## mixed: 42 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 212 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 34.1 | 99.8 | 34.1 | 34.1 | yes |
| cpu | 100 | ASAP (no roll-ups) | 34.1 | 99.8 | 34.1 | 34.1 | yes |
| cpu | 100 | PerQuery-CostAware | 60.1 | 97.4 | 60.1 | 75.6 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.74e+03 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| cpu | 300 | ASAP | 10.6 | 291 | 10.6 | 15.5 | yes |
| cpu | 300 | ASAP (no roll-ups) | 10.6 | 291 | 10.6 | 15.5 | yes |
| cpu | 300 | PerQuery-CostAware | 21 | 291 | 21 | 67.7 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.74e+03 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.22 | 980 | 4.22 | 29.1 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.22 | 980 | 4.22 | 29.1 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 10.5 | 980 | 10.5 | 155 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.74e+03 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.41 | 2.68e+03 | 2.41 | 16.3 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 2.41 | 2.68e+03 | 2.41 | 16.3 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 7.98 | 2.68e+03 | 7.98 | 139 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.74e+03 | 1.01e+03 | 2.74e+03 | 5.62e+03 | yes |
| cpu | 1e+04 | ASAP | 2.41 | 2.68e+03 | 2.41 | 16.3 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 2.41 | 2.68e+03 | 2.41 | 16.3 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 7.77 | 3.33e+03 | 7.77 | 72.8 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.74e+03 | 1.01e+03 | 2.74e+03 | 5.62e+03 | yes |
| fargate | 100 | ASAP | 1.48 | 96.9 | 34.5 | 18.2 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.48 | 96.9 | 34.5 | 18.2 | yes |
| fargate | 100 | PerQuery-CostAware | 2.77 | 97.4 | 60.2 | 74.9 | yes |
| fargate | 100 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| fargate | 300 | ASAP | 0.499 | 291 | 10.6 | 15.5 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.499 | 291 | 10.6 | 15.5 | yes |
| fargate | 300 | PerQuery-CostAware | 1.15 | 291 | 21 | 67 | yes |
| fargate | 300 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.242 | 896 | 4.24 | 15.7 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.242 | 896 | 4.24 | 15.7 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.732 | 896 | 10.7 | 67.3 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.74e+03 | 5.62e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.17 | 2.68e+03 | 2.41 | 16.3 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.17 | 2.68e+03 | 2.41 | 16.3 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.628 | 2.68e+03 | 8.04 | 67.9 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.74e+03 | 5.62e+03 | yes |
| fargate | 1e+04 | ASAP | 0.17 | 2.68e+03 | 2.41 | 16.3 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.17 | 2.68e+03 | 2.41 | 16.3 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.623 | 3.26e+03 | 7.92 | 67.9 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.74e+03 | 5.62e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 11 | 11 | 34.1 | 34.1 | 0.0% |
| cpu | 300 | 0 | 10 | 10 | 10.6 | 10.6 | 0.0% |
| cpu | 1e+03 | 0 | 7 | 7 | 4.22 | 4.22 | 0.0% |
| cpu | 3e+03 | 0 | 6 | 6 | 2.41 | 2.41 | 0.0% |
| cpu | 1e+04 | 0 | 6 | 6 | 2.41 | 2.41 | 0.0% |
| fargate | 100 | 0 | 11 | 11 | 1.48 | 1.48 | 0.0% |
| fargate | 300 | 0 | 10 | 10 | 0.499 | 0.499 | 0.0% |
| fargate | 1e+03 | 0 | 7 | 7 | 0.242 | 0.242 | 0.0% |
| fargate | 3e+03 | 0 | 6 | 6 | 0.17 | 0.17 | 0.0% |
| fargate | 1e+04 | 0 | 6 | 6 | 0.17 | 0.17 | 0.0% |

## multi-grouping: 62 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 269 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 10.2 | 490 | 10.2 | 3.55 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 10.2 | 490 | 10.2 | 3.55 | **no** |
| cpu | 1e+03 | ASAP | 0.221 | 557 | 0.221 | 1.06 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 0.551 | 959 | 0.551 | 2.6 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.69 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 10.2 | 490 | 10.2 | 3.55 | yes |
| cpu | 3e+03 | ASAP | 0.221 | 557 | 0.221 | 1.06 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 0.551 | 959 | 0.551 | 2.6 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.69 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 10.2 | 490 | 10.2 | 3.55 | yes |
| cpu | 1e+04 | ASAP | 0.221 | 557 | 0.221 | 1.06 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 0.551 | 959 | 0.551 | 2.6 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 1.07 | 959 | 1.07 | 3.69 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 10.2 | 490 | 10.2 | 3.55 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 0.43 | 490 | 10.2 | 3.55 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 0.43 | 490 | 10.2 | 3.55 | **no** |
| fargate | 1e+03 | ASAP | 0.0137 | 557 | 0.224 | 1.03 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.0339 | 959 | 0.559 | 2.52 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.0597 | 959 | 1.08 | 3.61 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 0.43 | 490 | 10.2 | 3.55 | yes |
| fargate | 3e+03 | ASAP | 0.0137 | 557 | 0.224 | 1.03 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.0339 | 959 | 0.559 | 2.52 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.0597 | 959 | 1.08 | 3.61 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 0.43 | 490 | 10.2 | 3.55 | yes |
| fargate | 1e+04 | ASAP | 0.0137 | 557 | 0.224 | 1.03 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.0339 | 959 | 0.559 | 2.52 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.0597 | 959 | 1.08 | 3.61 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 0.43 | 490 | 10.2 | 3.55 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 52 | 5 | 26 | 0.221 | 0.551 | 59.9% |
| cpu | 3e+03 | 52 | 5 | 26 | 0.221 | 0.551 | 59.9% |
| cpu | 1e+04 | 52 | 5 | 26 | 0.221 | 0.551 | 59.9% |
| fargate | 1e+03 | 52 | 5 | 26 | 0.0137 | 0.0339 | 59.7% |
| fargate | 3e+03 | 52 | 5 | 26 | 0.0137 | 0.0339 | 59.7% |
| fargate | 1e+04 | 52 | 5 | 26 | 0.0137 | 0.0339 | 59.7% |

## mixed, r = 8: 76 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 212 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 34.1 | 99.8 | 34.1 | 34.1 | yes |
| cpu | 100 | ASAP (no roll-ups) | 34.1 | 99.8 | 34.1 | 34.1 | yes |
| cpu | 100 | PerQuery-CostAware | 74 | 97.4 | 74 | 106 | yes |
| cpu | 100 | AutoSketch-Adapted | 3.28e+03 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| cpu | 300 | ASAP | 10.6 | 291 | 10.6 | 15.5 | yes |
| cpu | 300 | ASAP (no roll-ups) | 10.6 | 291 | 10.6 | 15.5 | yes |
| cpu | 300 | PerQuery-CostAware | 28.8 | 291 | 28.8 | 98 | yes |
| cpu | 300 | AutoSketch-Adapted | 3.28e+03 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.25 | 980 | 4.25 | 29.1 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.25 | 980 | 4.25 | 29.1 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 16.5 | 980 | 16.5 | 185 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.28e+03 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.47 | 2.68e+03 | 2.47 | 16.3 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 2.47 | 2.68e+03 | 2.47 | 16.3 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 13.8 | 2.68e+03 | 13.8 | 169 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.28e+03 | 1.01e+03 | 3.28e+03 | 5.85e+03 | yes |
| cpu | 1e+04 | ASAP | 2.47 | 2.68e+03 | 2.47 | 16.3 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 2.47 | 2.68e+03 | 2.47 | 16.3 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 13.6 | 3.33e+03 | 13.6 | 103 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.28e+03 | 1.01e+03 | 3.28e+03 | 5.85e+03 | yes |
| fargate | 100 | ASAP | 1.48 | 96.9 | 34.5 | 18.2 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.48 | 96.9 | 34.5 | 18.2 | yes |
| fargate | 100 | PerQuery-CostAware | 3.4 | 97.4 | 74.4 | 87.7 | yes |
| fargate | 100 | AutoSketch-Adapted | 159 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| fargate | 300 | ASAP | 0.5 | 291 | 10.6 | 15.5 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.5 | 291 | 10.6 | 15.5 | yes |
| fargate | 300 | PerQuery-CostAware | 1.53 | 291 | 29 | 79.8 | yes |
| fargate | 300 | AutoSketch-Adapted | 159 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.243 | 896 | 4.26 | 15.7 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.243 | 896 | 4.26 | 15.7 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 1.04 | 896 | 16.8 | 80.3 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 159 | 1.01e+03 | 3.28e+03 | 5.85e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.173 | 2.68e+03 | 2.47 | 16.3 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.173 | 2.68e+03 | 2.47 | 16.3 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.924 | 2.68e+03 | 13.9 | 80.9 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 159 | 1.01e+03 | 3.28e+03 | 5.85e+03 | yes |
| fargate | 1e+04 | ASAP | 0.173 | 2.68e+03 | 2.47 | 16.3 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.173 | 2.68e+03 | 2.47 | 16.3 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.919 | 3.26e+03 | 13.8 | 80.9 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 159 | 1.01e+03 | 3.28e+03 | 5.85e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 11 | 11 | 34.1 | 34.1 | 0.0% |
| cpu | 300 | 0 | 10 | 10 | 10.6 | 10.6 | 0.0% |
| cpu | 1e+03 | 0 | 7 | 7 | 4.25 | 4.25 | 0.0% |
| cpu | 3e+03 | 0 | 6 | 6 | 2.47 | 2.47 | 0.0% |
| cpu | 1e+04 | 0 | 6 | 6 | 2.47 | 2.47 | 0.0% |
| fargate | 100 | 0 | 11 | 11 | 1.48 | 1.48 | 0.0% |
| fargate | 300 | 0 | 10 | 10 | 0.5 | 0.5 | 0.0% |
| fargate | 1e+03 | 0 | 7 | 7 | 0.243 | 0.243 | 0.0% |
| fargate | 3e+03 | 0 | 6 | 6 | 0.173 | 0.173 | 0.0% |
| fargate | 1e+04 | 0 | 6 | 6 | 0.173 | 0.173 | 0.0% |

## mixed + multi-grouping: 104 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 481 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 2.75e+03 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 2.75e+03 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.44 | 980 | 4.44 | 30.2 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.77 | 980 | 4.77 | 31.7 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 11.5 | 980 | 11.5 | 158 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.75e+03 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.64 | 2.68e+03 | 2.64 | 17.4 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 2.97 | 2.68e+03 | 2.97 | 18.9 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 9.05 | 2.68e+03 | 9.05 | 142 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.75e+03 | 1.01e+03 | 2.75e+03 | 5.62e+03 | yes |
| cpu | 1e+04 | ASAP | 2.64 | 2.68e+03 | 2.64 | 17.4 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 2.97 | 2.68e+03 | 2.97 | 18.9 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 8.84 | 3.33e+03 | 8.84 | 76.4 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.75e+03 | 1.01e+03 | 2.75e+03 | 5.62e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.255 | 896 | 4.46 | 16.7 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.275 | 959 | 4.8 | 18.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.792 | 959 | 11.8 | 70.9 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.75e+03 | 5.62e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.184 | 2.68e+03 | 2.64 | 17.4 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.204 | 2.68e+03 | 2.97 | 18.8 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.688 | 2.68e+03 | 9.12 | 71.5 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.75e+03 | 5.62e+03 | yes |
| fargate | 1e+04 | ASAP | 0.184 | 2.68e+03 | 2.64 | 17.4 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.204 | 2.68e+03 | 2.97 | 18.8 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.683 | 3.26e+03 | 9 | 71.5 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 136 | 1.01e+03 | 2.75e+03 | 5.62e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 52 | 12 | 33 | 4.44 | 4.77 | 6.9% |
| cpu | 3e+03 | 52 | 11 | 32 | 2.64 | 2.97 | 11.1% |
| cpu | 1e+04 | 52 | 11 | 32 | 2.64 | 2.97 | 11.1% |
| fargate | 1e+03 | 52 | 12 | 33 | 0.255 | 0.275 | 7.3% |
| fargate | 3e+03 | 52 | 11 | 32 | 0.184 | 0.204 | 9.9% |
| fargate | 1e+04 | 52 | 11 | 32 | 0.184 | 0.204 | 9.9% |

## multi-grouping, r = 8: 124 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 269 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 12.3 | 490 | 12.3 | 4.52 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 12.3 | 490 | 12.3 | 4.52 | **no** |
| cpu | 1e+03 | ASAP | 0.242 | 557 | 0.242 | 1.06 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 0.573 | 959 | 0.573 | 2.6 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.69 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 12.3 | 490 | 12.3 | 4.52 | yes |
| cpu | 3e+03 | ASAP | 0.242 | 557 | 0.242 | 1.06 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 0.573 | 959 | 0.573 | 2.6 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.69 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 12.3 | 490 | 12.3 | 4.52 | yes |
| cpu | 1e+04 | ASAP | 0.242 | 557 | 0.242 | 1.06 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 0.573 | 959 | 0.573 | 2.6 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 2.04 | 959 | 2.04 | 4.69 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 12.3 | 490 | 12.3 | 4.52 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 0.517 | 490 | 12.3 | 4.52 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 0.517 | 490 | 12.3 | 4.52 | **no** |
| fargate | 1e+03 | ASAP | 0.0145 | 557 | 0.245 | 1.03 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.0347 | 959 | 0.581 | 2.52 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.104 | 959 | 2.05 | 4.62 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 0.517 | 490 | 12.3 | 4.52 | yes |
| fargate | 3e+03 | ASAP | 0.0145 | 557 | 0.245 | 1.03 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.0347 | 959 | 0.581 | 2.52 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.104 | 959 | 2.05 | 4.62 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 0.517 | 490 | 12.3 | 4.52 | yes |
| fargate | 1e+04 | ASAP | 0.0145 | 557 | 0.245 | 1.03 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.0347 | 959 | 0.581 | 2.52 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.104 | 959 | 2.05 | 4.62 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 0.517 | 490 | 12.3 | 4.52 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 104 | 5 | 26 | 0.242 | 0.573 | 57.8% |
| cpu | 3e+03 | 104 | 5 | 26 | 0.242 | 0.573 | 57.8% |
| cpu | 1e+04 | 104 | 5 | 26 | 0.242 | 0.573 | 57.8% |
| fargate | 1e+03 | 104 | 5 | 26 | 0.0145 | 0.0347 | 58.2% |
| fargate | 3e+03 | 104 | 5 | 26 | 0.0145 | 0.0347 | 58.2% |
| fargate | 1e+04 | 104 | 5 | 26 | 0.0145 | 0.0347 | 58.2% |

## mixed + multi-grouping, r = 8: 200 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 481 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 3.3e+03 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 3.3e+03 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| cpu | 1e+03 | ASAP | 4.49 | 980 | 4.49 | 30.2 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.83 | 980 | 4.83 | 31.7 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 18.5 | 980 | 18.5 | 189 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.3e+03 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| cpu | 3e+03 | ASAP | 2.71 | 2.68e+03 | 2.71 | 17.4 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 3.04 | 2.68e+03 | 3.04 | 18.9 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 15.8 | 2.68e+03 | 15.8 | 173 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.3e+03 | 1.01e+03 | 3.3e+03 | 5.85e+03 | yes |
| cpu | 1e+04 | ASAP | 2.71 | 2.68e+03 | 2.71 | 17.4 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 3.04 | 2.68e+03 | 3.04 | 18.9 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 15.6 | 3.33e+03 | 15.6 | 107 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.3e+03 | 1.01e+03 | 3.3e+03 | 5.85e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 160 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 160 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.257 | 896 | 4.51 | 16.7 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.277 | 959 | 4.84 | 18.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 1.14 | 959 | 18.8 | 84.9 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 160 | 1.01e+03 | 3.3e+03 | 5.85e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.187 | 2.68e+03 | 2.72 | 17.4 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.207 | 2.68e+03 | 3.05 | 18.8 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 1.03 | 2.68e+03 | 16 | 85.5 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 160 | 1.01e+03 | 3.3e+03 | 5.85e+03 | yes |
| fargate | 1e+04 | ASAP | 0.187 | 2.68e+03 | 2.72 | 17.4 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.207 | 2.68e+03 | 3.05 | 18.8 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 1.02 | 3.26e+03 | 15.8 | 85.5 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 160 | 1.01e+03 | 3.3e+03 | 5.85e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 104 | 12 | 33 | 4.49 | 4.83 | 6.9% |
| cpu | 3e+03 | 104 | 11 | 32 | 2.71 | 3.04 | 10.9% |
| cpu | 1e+04 | 104 | 11 | 32 | 2.71 | 3.04 | 10.9% |
| fargate | 1e+03 | 104 | 12 | 33 | 0.257 | 0.277 | 7.3% |
| fargate | 3e+03 | 104 | 11 | 32 | 0.187 | 0.207 | 9.8% |
| fargate | 1e+04 | 104 | 11 | 32 | 0.187 | 0.207 | 9.8% |

## mixed, m = 8: 336 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 1.7e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 273 | 99.8 | 273 | 273 | yes |
| cpu | 100 | ASAP (no roll-ups) | 273 | 99.8 | 273 | 273 | yes |
| cpu | 100 | PerQuery-CostAware | 481 | 97.4 | 481 | 605 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.19e+04 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| cpu | 300 | ASAP | 85 | 291 | 85 | 124 | yes |
| cpu | 300 | ASAP (no roll-ups) | 85 | 291 | 85 | 124 | yes |
| cpu | 300 | PerQuery-CostAware | 168 | 291 | 168 | 542 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.19e+04 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| cpu | 1e+03 | ASAP | 33.8 | 980 | 33.8 | 233 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 33.8 | 980 | 33.8 | 233 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 83.7 | 980 | 83.7 | 1.24e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.19e+04 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| cpu | 3e+03 | ASAP | 19.3 | 2.68e+03 | 19.3 | 131 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 19.3 | 2.68e+03 | 19.3 | 131 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 63.9 | 2.68e+03 | 63.9 | 1.11e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.19e+04 | 1.01e+03 | 2.19e+04 | 4.49e+04 | yes |
| cpu | 1e+04 | ASAP | 19.3 | 2.68e+03 | 19.3 | 131 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 19.3 | 2.68e+03 | 19.3 | 131 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 62.2 | 3.33e+03 | 62.2 | 582 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.19e+04 | 1.01e+03 | 2.19e+04 | 4.49e+04 | yes |
| fargate | 100 | ASAP | 11.8 | 96.9 | 276 | 145 | yes |
| fargate | 100 | ASAP (no roll-ups) | 11.8 | 96.9 | 276 | 145 | yes |
| fargate | 100 | PerQuery-CostAware | 22.2 | 97.4 | 481 | 599 | yes |
| fargate | 100 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| fargate | 300 | ASAP | 4 | 291 | 85 | 124 | yes |
| fargate | 300 | ASAP (no roll-ups) | 4 | 291 | 85 | 124 | yes |
| fargate | 300 | PerQuery-CostAware | 9.2 | 291 | 168 | 536 | yes |
| fargate | 300 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| fargate | 1e+03 | ASAP | 1.93 | 896 | 33.9 | 126 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 1.93 | 896 | 33.9 | 126 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 5.85 | 896 | 85.4 | 538 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.19e+04 | 4.49e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.36 | 2.68e+03 | 19.3 | 131 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 1.36 | 2.68e+03 | 19.3 | 131 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 5.02 | 2.68e+03 | 64.3 | 543 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.19e+04 | 4.49e+04 | yes |
| fargate | 1e+04 | ASAP | 1.36 | 2.68e+03 | 19.3 | 131 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 1.36 | 2.68e+03 | 19.3 | 131 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 4.98 | 3.26e+03 | 63.4 | 543 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.19e+04 | 4.49e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 88 | 88 | 273 | 273 | 0.0% |
| cpu | 300 | 0 | 80 | 80 | 85 | 85 | 0.0% |
| cpu | 1e+03 | 0 | 56 | 56 | 33.8 | 33.8 | 0.0% |
| cpu | 3e+03 | 0 | 48 | 48 | 19.3 | 19.3 | 0.0% |
| cpu | 1e+04 | 0 | 48 | 48 | 19.3 | 19.3 | 0.0% |
| fargate | 100 | 0 | 88 | 88 | 11.8 | 11.8 | 0.0% |
| fargate | 300 | 0 | 80 | 80 | 4 | 4 | 0.0% |
| fargate | 1e+03 | 0 | 56 | 56 | 1.93 | 1.93 | 0.0% |
| fargate | 3e+03 | 0 | 48 | 48 | 1.36 | 1.36 | 0.0% |
| fargate | 1e+04 | 0 | 48 | 48 | 1.36 | 1.36 | 0.0% |

## multi-grouping, m = 8: 496 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 2.15e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 81.7 | 490 | 81.7 | 28.4 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 81.7 | 490 | 81.7 | 28.4 | **no** |
| cpu | 1e+03 | ASAP | 1.77 | 557 | 1.77 | 8.52 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 4.41 | 959 | 4.41 | 20.8 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 8.55 | 959 | 8.55 | 29.5 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 81.7 | 490 | 81.7 | 28.4 | yes |
| cpu | 3e+03 | ASAP | 1.77 | 557 | 1.77 | 8.52 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 4.41 | 959 | 4.41 | 20.8 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 8.55 | 959 | 8.55 | 29.5 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 81.7 | 490 | 81.7 | 28.4 | yes |
| cpu | 1e+04 | ASAP | 1.77 | 557 | 1.77 | 8.52 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 4.41 | 959 | 4.41 | 20.8 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 8.55 | 959 | 8.55 | 29.5 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 81.7 | 490 | 81.7 | 28.4 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 3.44 | 490 | 81.7 | 28.4 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 3.44 | 490 | 81.7 | 28.4 | **no** |
| fargate | 1e+03 | ASAP | 0.109 | 557 | 1.79 | 8.22 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.271 | 959 | 4.47 | 20.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.477 | 959 | 8.61 | 28.9 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 3.44 | 490 | 81.7 | 28.4 | yes |
| fargate | 3e+03 | ASAP | 0.109 | 557 | 1.79 | 8.22 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.271 | 959 | 4.47 | 20.2 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.477 | 959 | 8.61 | 28.9 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 3.44 | 490 | 81.7 | 28.4 | yes |
| fargate | 1e+04 | ASAP | 0.109 | 557 | 1.79 | 8.22 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.271 | 959 | 4.47 | 20.2 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.477 | 959 | 8.61 | 28.9 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 3.44 | 490 | 81.7 | 28.4 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 416 | 40 | 208 | 1.77 | 4.41 | 59.9% |
| cpu | 3e+03 | 416 | 40 | 208 | 1.77 | 4.41 | 59.9% |
| cpu | 1e+04 | 416 | 40 | 208 | 1.77 | 4.41 | 59.9% |
| fargate | 1e+03 | 416 | 40 | 208 | 0.109 | 0.271 | 59.7% |
| fargate | 3e+03 | 416 | 40 | 208 | 0.109 | 0.271 | 59.7% |
| fargate | 1e+04 | 416 | 40 | 208 | 0.109 | 0.271 | 59.7% |

## mixed, m = 16: 672 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.39e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 546 | 99.8 | 546 | 545 | yes |
| cpu | 100 | ASAP (no roll-ups) | 546 | 99.8 | 546 | 545 | yes |
| cpu | 100 | PerQuery-CostAware | 962 | 97.4 | 962 | 1.21e+03 | yes |
| cpu | 100 | AutoSketch-Adapted | 4.38e+04 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| cpu | 300 | ASAP | 170 | 291 | 170 | 248 | yes |
| cpu | 300 | ASAP (no roll-ups) | 170 | 291 | 170 | 248 | yes |
| cpu | 300 | PerQuery-CostAware | 336 | 291 | 336 | 1.08e+03 | yes |
| cpu | 300 | AutoSketch-Adapted | 4.38e+04 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| cpu | 1e+03 | ASAP | 67.6 | 980 | 67.6 | 466 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 67.6 | 980 | 67.6 | 466 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 167 | 980 | 167 | 2.48e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.38e+04 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| cpu | 3e+03 | ASAP | 38.6 | 2.68e+03 | 38.6 | 261 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 38.6 | 2.68e+03 | 38.6 | 261 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 128 | 2.68e+03 | 128 | 2.22e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.38e+04 | 1.01e+03 | 4.38e+04 | 8.99e+04 | yes |
| cpu | 1e+04 | ASAP | 38.6 | 2.68e+03 | 38.6 | 261 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 38.6 | 2.68e+03 | 38.6 | 261 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 124 | 3.33e+03 | 124 | 1.16e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.38e+04 | 1.01e+03 | 4.38e+04 | 8.99e+04 | yes |
| fargate | 100 | ASAP | 23.6 | 96.9 | 552 | 291 | yes |
| fargate | 100 | ASAP (no roll-ups) | 23.6 | 96.9 | 552 | 291 | yes |
| fargate | 100 | PerQuery-CostAware | 44.3 | 97.4 | 962 | 1.2e+03 | yes |
| fargate | 100 | AutoSketch-Adapted | 2.17e+03 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| fargate | 300 | ASAP | 7.99 | 291 | 170 | 248 | yes |
| fargate | 300 | ASAP (no roll-ups) | 7.99 | 291 | 170 | 248 | yes |
| fargate | 300 | PerQuery-CostAware | 18.4 | 291 | 336 | 1.07e+03 | yes |
| fargate | 300 | AutoSketch-Adapted | 2.17e+03 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| fargate | 1e+03 | ASAP | 3.86 | 896 | 67.8 | 251 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 3.86 | 896 | 67.8 | 251 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 11.7 | 896 | 171 | 1.08e+03 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.17e+03 | 1.01e+03 | 4.38e+04 | 8.99e+04 | **no** |
| fargate | 3e+03 | ASAP | 2.73 | 2.68e+03 | 38.6 | 261 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 2.73 | 2.68e+03 | 38.6 | 261 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 10 | 2.68e+03 | 129 | 1.09e+03 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.17e+03 | 1.01e+03 | 4.38e+04 | 8.99e+04 | yes |
| fargate | 1e+04 | ASAP | 2.73 | 2.68e+03 | 38.6 | 261 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 2.73 | 2.68e+03 | 38.6 | 261 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 9.97 | 3.26e+03 | 127 | 1.09e+03 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.17e+03 | 1.01e+03 | 4.38e+04 | 8.99e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 176 | 176 | 546 | 546 | 0.0% |
| cpu | 300 | 0 | 160 | 160 | 170 | 170 | 0.0% |
| cpu | 1e+03 | 0 | 112 | 112 | 67.6 | 67.6 | 0.0% |
| cpu | 3e+03 | 0 | 96 | 96 | 38.6 | 38.6 | 0.0% |
| cpu | 1e+04 | 0 | 96 | 96 | 38.6 | 38.6 | 0.0% |
| fargate | 100 | 0 | 176 | 176 | 23.6 | 23.6 | 0.0% |
| fargate | 300 | 0 | 160 | 160 | 7.99 | 7.99 | 0.0% |
| fargate | 1e+03 | 0 | 112 | 112 | 3.86 | 3.86 | 0.0% |
| fargate | 3e+03 | 0 | 96 | 96 | 2.73 | 2.73 | 0.0% |
| fargate | 1e+04 | 0 | 96 | 96 | 2.73 | 2.73 | 0.0% |

## mixed + multi-grouping, m = 8: 832 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.85e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 2.2e+04 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 2.2e+04 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| cpu | 1e+03 | ASAP | 35.5 | 980 | 35.5 | 242 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 38.2 | 980 | 38.2 | 254 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 92.3 | 980 | 92.3 | 1.27e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.2e+04 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| cpu | 3e+03 | ASAP | 21.1 | 2.68e+03 | 21.1 | 139 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 23.7 | 2.68e+03 | 23.7 | 151 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 72.4 | 2.68e+03 | 72.4 | 1.14e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.2e+04 | 1.01e+03 | 2.2e+04 | 4.5e+04 | yes |
| cpu | 1e+04 | ASAP | 21.1 | 2.68e+03 | 21.1 | 139 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 23.7 | 2.68e+03 | 23.7 | 151 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 70.7 | 3.33e+03 | 70.7 | 612 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.2e+04 | 1.01e+03 | 2.2e+04 | 4.5e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| fargate | 1e+03 | ASAP | 2.04 | 896 | 35.7 | 134 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 2.2 | 959 | 38.4 | 146 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 6.33 | 959 | 94 | 567 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.2e+04 | 4.5e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.47 | 2.68e+03 | 21.1 | 139 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 1.63 | 2.68e+03 | 23.8 | 151 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 5.5 | 2.68e+03 | 72.9 | 572 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.2e+04 | 4.5e+04 | yes |
| fargate | 1e+04 | ASAP | 1.47 | 2.68e+03 | 21.1 | 139 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 1.63 | 2.68e+03 | 23.8 | 151 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 5.46 | 3.26e+03 | 72 | 572 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.09e+03 | 1.01e+03 | 2.2e+04 | 4.5e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 416 | 96 | 264 | 35.5 | 38.2 | 6.9% |
| cpu | 3e+03 | 416 | 88 | 256 | 21.1 | 23.7 | 11.1% |
| cpu | 1e+04 | 416 | 88 | 256 | 21.1 | 23.7 | 11.1% |
| fargate | 1e+03 | 416 | 96 | 264 | 2.04 | 2.2 | 7.3% |
| fargate | 3e+03 | 416 | 88 | 256 | 1.47 | 1.63 | 9.9% |
| fargate | 1e+04 | 416 | 88 | 256 | 1.47 | 1.63 | 9.9% |

## multi-grouping, m = 16: 992 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 490 ms; planning time (search + measured benchmark) 4.31e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 163 | 490 | 163 | 56.8 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 163 | 490 | 163 | 56.8 | **no** |
| cpu | 1e+03 | ASAP | 3.53 | 557 | 3.53 | 17 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 8.81 | 959 | 8.81 | 41.5 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 59 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 163 | 490 | 163 | 56.8 | yes |
| cpu | 3e+03 | ASAP | 3.53 | 557 | 3.53 | 17 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 8.81 | 959 | 8.81 | 41.5 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 59 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 163 | 490 | 163 | 56.8 | yes |
| cpu | 1e+04 | ASAP | 3.53 | 557 | 3.53 | 17 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 8.81 | 959 | 8.81 | 41.5 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 17.1 | 959 | 17.1 | 59 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 163 | 490 | 163 | 56.8 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 6.87 | 490 | 163 | 56.8 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 6.87 | 490 | 163 | 56.8 | **no** |
| fargate | 1e+03 | ASAP | 0.218 | 557 | 3.59 | 16.4 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.542 | 959 | 8.94 | 40.4 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.955 | 959 | 17.2 | 57.8 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 6.87 | 490 | 163 | 56.8 | yes |
| fargate | 3e+03 | ASAP | 0.218 | 557 | 3.59 | 16.4 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.542 | 959 | 8.94 | 40.4 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.955 | 959 | 17.2 | 57.8 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 6.87 | 490 | 163 | 56.8 | yes |
| fargate | 1e+04 | ASAP | 0.218 | 557 | 3.59 | 16.4 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.542 | 959 | 8.94 | 40.4 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.955 | 959 | 17.2 | 57.8 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 6.87 | 490 | 163 | 56.8 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 832 | 80 | 416 | 3.53 | 8.81 | 59.9% |
| cpu | 3e+03 | 832 | 80 | 416 | 3.53 | 8.81 | 59.9% |
| cpu | 1e+04 | 832 | 80 | 416 | 3.53 | 8.81 | 59.9% |
| fargate | 1e+03 | 832 | 80 | 416 | 0.218 | 0.542 | 59.7% |
| fargate | 3e+03 | 832 | 80 | 416 | 0.218 | 0.542 | 59.7% |
| fargate | 1e+04 | 832 | 80 | 416 | 0.218 | 0.542 | 59.7% |

## mixed + multi-grouping, m = 16: 1664 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 7.7e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 4.4e+04 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 4.4e+04 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| cpu | 1e+03 | ASAP | 71.1 | 980 | 71.1 | 483 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 76.4 | 980 | 76.4 | 508 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 185 | 980 | 185 | 2.54e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.4e+04 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| cpu | 3e+03 | ASAP | 42.2 | 2.68e+03 | 42.2 | 278 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 47.4 | 2.68e+03 | 47.4 | 303 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 145 | 2.68e+03 | 145 | 2.28e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.4e+04 | 1.01e+03 | 4.4e+04 | 8.99e+04 | yes |
| cpu | 1e+04 | ASAP | 42.2 | 2.68e+03 | 42.2 | 278 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 47.4 | 2.68e+03 | 47.4 | 303 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 141 | 3.33e+03 | 141 | 1.22e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.4e+04 | 1.01e+03 | 4.4e+04 | 8.99e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 2.18e+03 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 2.18e+03 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| fargate | 1e+03 | ASAP | 4.08 | 896 | 71.4 | 268 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 4.41 | 959 | 76.7 | 292 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 12.7 | 959 | 188 | 1.13e+03 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.18e+03 | 1.01e+03 | 4.4e+04 | 8.99e+04 | **no** |
| fargate | 3e+03 | ASAP | 2.95 | 2.68e+03 | 42.2 | 278 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 3.27 | 2.68e+03 | 47.6 | 302 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 11 | 2.68e+03 | 146 | 1.14e+03 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.18e+03 | 1.01e+03 | 4.4e+04 | 8.99e+04 | yes |
| fargate | 1e+04 | ASAP | 2.95 | 2.68e+03 | 42.2 | 278 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 3.27 | 2.68e+03 | 47.6 | 302 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 10.9 | 3.26e+03 | 144 | 1.14e+03 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.18e+03 | 1.01e+03 | 4.4e+04 | 8.99e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 832 | 192 | 528 | 71.1 | 76.4 | 6.9% |
| cpu | 3e+03 | 832 | 176 | 512 | 42.2 | 47.4 | 11.1% |
| cpu | 1e+04 | 832 | 176 | 512 | 42.2 | 47.4 | 11.1% |
| fargate | 1e+03 | 832 | 192 | 528 | 4.08 | 4.41 | 7.3% |
| fargate | 3e+03 | 832 | 176 | 512 | 2.95 | 3.27 | 9.9% |
| fargate | 1e+04 | 832 | 176 | 512 | 2.95 | 3.27 | 9.9% |
