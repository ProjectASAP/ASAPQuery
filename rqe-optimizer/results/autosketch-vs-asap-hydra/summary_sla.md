# Version 2: cost by use under a batch latency SLA

Each method's cost by use (`w_cpu · AUC(CPU) + w_mem · AUC(memory)`) at each SLA; ASAP and PerQuery plan for the SLA (the cheapest plan whose batch latency, the longest chain, is at most it), AutoSketch's one plan ignores it.

## mixed: 50 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 212 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 36.7 | 99.8 | 36.7 | 34.8 | yes |
| cpu | 100 | ASAP (no roll-ups) | 36.7 | 99.8 | 36.7 | 34.8 | yes |
| cpu | 100 | PerQuery-CostAware | 64.5 | 97.4 | 64.5 | 76.5 | yes |
| cpu | 100 | AutoSketch-Adapted | 3.72e+03 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| cpu | 300 | ASAP | 12.7 | 291 | 12.7 | 16.5 | yes |
| cpu | 300 | ASAP (no roll-ups) | 12.7 | 291 | 12.7 | 16.5 | yes |
| cpu | 300 | PerQuery-CostAware | 24.7 | 291 | 24.7 | 68.9 | yes |
| cpu | 300 | AutoSketch-Adapted | 3.72e+03 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.67 | 980 | 5.67 | 30.7 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 5.67 | 980 | 5.67 | 30.7 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 13.5 | 980 | 13.5 | 157 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.72e+03 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.6 | 2.68e+03 | 3.6 | 18.9 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 3.6 | 2.68e+03 | 3.6 | 18.9 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 10.8 | 2.68e+03 | 10.8 | 142 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.72e+03 | 1.01e+03 | 3.72e+03 | 5.62e+03 | yes |
| cpu | 1e+04 | ASAP | 3.6 | 2.68e+03 | 3.6 | 18.9 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 3.6 | 2.68e+03 | 3.6 | 18.9 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 10.5 | 3.33e+03 | 10.5 | 77 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.72e+03 | 1.01e+03 | 3.72e+03 | 5.62e+03 | yes |
| fargate | 100 | ASAP | 1.59 | 96.9 | 37.1 | 18.8 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.59 | 96.9 | 37.1 | 18.8 | yes |
| fargate | 100 | PerQuery-CostAware | 2.95 | 97.4 | 64.6 | 75.7 | yes |
| fargate | 100 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| fargate | 300 | ASAP | 0.586 | 291 | 12.7 | 16.5 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.586 | 291 | 12.7 | 16.5 | yes |
| fargate | 300 | PerQuery-CostAware | 1.3 | 291 | 24.7 | 68.2 | yes |
| fargate | 300 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.307 | 896 | 5.69 | 17.2 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.307 | 896 | 5.69 | 17.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.864 | 896 | 13.7 | 69.7 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.72e+03 | 5.62e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.23 | 2.68e+03 | 3.6 | 18.9 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.23 | 2.68e+03 | 3.6 | 18.9 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.757 | 2.68e+03 | 10.9 | 70.9 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.72e+03 | 5.62e+03 | yes |
| fargate | 1e+04 | ASAP | 0.23 | 2.68e+03 | 3.6 | 18.9 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.23 | 2.68e+03 | 3.6 | 18.9 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.752 | 3.26e+03 | 10.8 | 70.9 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.72e+03 | 5.62e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 14 | 14 | 36.7 | 36.7 | 0.0% |
| cpu | 300 | 0 | 13 | 13 | 12.7 | 12.7 | 0.0% |
| cpu | 1e+03 | 0 | 9 | 9 | 5.67 | 5.67 | 0.0% |
| cpu | 3e+03 | 0 | 8 | 8 | 3.6 | 3.6 | 0.0% |
| cpu | 1e+04 | 0 | 8 | 8 | 3.6 | 3.6 | 0.0% |
| fargate | 100 | 0 | 14 | 14 | 1.59 | 1.59 | 0.0% |
| fargate | 300 | 0 | 13 | 13 | 0.586 | 0.586 | 0.0% |
| fargate | 1e+03 | 0 | 9 | 9 | 0.307 | 0.307 | 0.0% |
| fargate | 3e+03 | 0 | 8 | 8 | 0.23 | 0.23 | 0.0% |
| fargate | 1e+04 | 0 | 8 | 8 | 0.23 | 0.23 | 0.0% |

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

## mixed, r = 8: 92 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 212 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 36.7 | 99.8 | 36.7 | 34.8 | yes |
| cpu | 100 | ASAP (no roll-ups) | 36.7 | 99.8 | 36.7 | 34.8 | yes |
| cpu | 100 | PerQuery-CostAware | 80.8 | 97.4 | 80.8 | 107 | yes |
| cpu | 100 | AutoSketch-Adapted | 4.46e+03 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| cpu | 300 | ASAP | 12.7 | 291 | 12.7 | 16.5 | yes |
| cpu | 300 | ASAP (no roll-ups) | 12.7 | 291 | 12.7 | 16.5 | yes |
| cpu | 300 | PerQuery-CostAware | 34.8 | 291 | 34.8 | 99.4 | yes |
| cpu | 300 | AutoSketch-Adapted | 4.46e+03 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.71 | 980 | 5.71 | 30.7 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 5.71 | 980 | 5.71 | 30.7 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 21.6 | 980 | 21.6 | 187 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.46e+03 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.66 | 2.68e+03 | 3.66 | 18.9 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 3.66 | 2.68e+03 | 3.66 | 18.9 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 18.8 | 2.68e+03 | 18.8 | 172 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.46e+03 | 1.01e+03 | 4.46e+03 | 5.86e+03 | yes |
| cpu | 1e+04 | ASAP | 3.66 | 2.68e+03 | 3.66 | 18.9 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 3.66 | 2.68e+03 | 3.66 | 18.9 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 18.5 | 3.33e+03 | 18.5 | 107 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.46e+03 | 1.01e+03 | 4.46e+03 | 5.86e+03 | yes |
| fargate | 100 | ASAP | 1.59 | 96.9 | 37.1 | 18.8 | yes |
| fargate | 100 | ASAP (no roll-ups) | 1.59 | 96.9 | 37.1 | 18.8 | yes |
| fargate | 100 | PerQuery-CostAware | 3.69 | 97.4 | 81.3 | 88.7 | yes |
| fargate | 100 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| fargate | 300 | ASAP | 0.587 | 291 | 12.7 | 16.5 | yes |
| fargate | 300 | ASAP (no roll-ups) | 0.587 | 291 | 12.7 | 16.5 | yes |
| fargate | 300 | PerQuery-CostAware | 1.78 | 291 | 35 | 81.2 | yes |
| fargate | 300 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.308 | 896 | 5.72 | 17.2 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.308 | 896 | 5.72 | 17.2 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 1.26 | 896 | 21.9 | 83 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.46e+03 | 5.86e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.232 | 2.68e+03 | 3.66 | 18.9 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.232 | 2.68e+03 | 3.66 | 18.9 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 1.14 | 2.68e+03 | 18.9 | 84.2 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.46e+03 | 5.86e+03 | yes |
| fargate | 1e+04 | ASAP | 0.232 | 2.68e+03 | 3.66 | 18.9 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.232 | 2.68e+03 | 3.66 | 18.9 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 1.14 | 3.26e+03 | 18.8 | 84.2 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.46e+03 | 5.86e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 14 | 14 | 36.7 | 36.7 | 0.0% |
| cpu | 300 | 0 | 13 | 13 | 12.7 | 12.7 | 0.0% |
| cpu | 1e+03 | 0 | 9 | 9 | 5.71 | 5.71 | 0.0% |
| cpu | 3e+03 | 0 | 8 | 8 | 3.66 | 3.66 | 0.0% |
| cpu | 1e+04 | 0 | 8 | 8 | 3.66 | 3.66 | 0.0% |
| fargate | 100 | 0 | 14 | 14 | 1.59 | 1.59 | 0.0% |
| fargate | 300 | 0 | 13 | 13 | 0.587 | 0.587 | 0.0% |
| fargate | 1e+03 | 0 | 9 | 9 | 0.308 | 0.308 | 0.0% |
| fargate | 3e+03 | 0 | 8 | 8 | 0.232 | 0.232 | 0.0% |
| fargate | 1e+04 | 0 | 8 | 8 | 0.232 | 0.232 | 0.0% |

## mixed + multi-grouping: 112 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 481 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 3.73e+03 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 3.73e+03 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.9 | 980 | 5.9 | 31.7 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 6.23 | 980 | 6.23 | 33.3 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 14.5 | 980 | 14.5 | 161 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.73e+03 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.82 | 2.68e+03 | 3.82 | 19.9 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 4.15 | 2.68e+03 | 4.15 | 21.5 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 11.9 | 2.68e+03 | 11.9 | 145 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.73e+03 | 1.01e+03 | 3.73e+03 | 5.63e+03 | yes |
| cpu | 1e+04 | ASAP | 3.82 | 2.68e+03 | 3.82 | 19.9 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 4.15 | 2.68e+03 | 4.15 | 21.5 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 11.6 | 3.33e+03 | 11.6 | 80.7 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.73e+03 | 1.01e+03 | 3.73e+03 | 5.63e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.321 | 896 | 5.91 | 18.3 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.341 | 959 | 6.25 | 19.8 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.923 | 959 | 14.7 | 73.3 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.73e+03 | 5.63e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.243 | 2.68e+03 | 3.82 | 19.9 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.264 | 2.68e+03 | 4.16 | 21.4 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.817 | 2.68e+03 | 12 | 74.6 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.73e+03 | 5.63e+03 | yes |
| fargate | 1e+04 | ASAP | 0.243 | 2.68e+03 | 3.82 | 19.9 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.264 | 2.68e+03 | 4.16 | 21.4 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.812 | 3.26e+03 | 11.9 | 74.6 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 176 | 1.01e+03 | 3.73e+03 | 5.63e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 52 | 14 | 35 | 5.9 | 6.23 | 5.3% |
| cpu | 3e+03 | 52 | 13 | 34 | 3.82 | 4.15 | 8.0% |
| cpu | 1e+04 | 52 | 13 | 34 | 3.82 | 4.15 | 8.0% |
| fargate | 1e+03 | 52 | 14 | 35 | 0.321 | 0.341 | 5.9% |
| fargate | 3e+03 | 52 | 13 | 34 | 0.243 | 0.264 | 7.7% |
| fargate | 1e+04 | 52 | 13 | 34 | 0.243 | 0.264 | 7.7% |

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

## mixed + multi-grouping, r = 8: 216 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 481 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 4.47e+03 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 4.47e+03 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.95 | 980 | 5.95 | 31.7 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 6.28 | 980 | 6.28 | 33.3 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 23.7 | 980 | 23.7 | 192 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.47e+03 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.9 | 2.68e+03 | 3.9 | 19.9 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 4.23 | 2.68e+03 | 4.23 | 21.5 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 20.9 | 2.68e+03 | 20.9 | 177 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.47e+03 | 1.01e+03 | 4.47e+03 | 5.86e+03 | yes |
| cpu | 1e+04 | ASAP | 3.9 | 2.68e+03 | 3.9 | 19.9 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 4.23 | 2.68e+03 | 4.23 | 21.5 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 20.5 | 3.33e+03 | 20.5 | 112 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.47e+03 | 1.01e+03 | 4.47e+03 | 5.86e+03 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.323 | 896 | 5.96 | 18.3 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 0.343 | 959 | 6.3 | 19.8 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 1.36 | 959 | 24 | 87.6 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.47e+03 | 5.86e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.247 | 2.68e+03 | 3.91 | 19.9 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 0.267 | 2.68e+03 | 4.24 | 21.4 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 1.25 | 2.68e+03 | 21 | 88.8 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.47e+03 | 5.86e+03 | yes |
| fargate | 1e+04 | ASAP | 0.247 | 2.68e+03 | 3.91 | 19.9 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 0.267 | 2.68e+03 | 4.24 | 21.4 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 1.24 | 3.26e+03 | 20.9 | 88.8 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 207 | 1.01e+03 | 4.47e+03 | 5.86e+03 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 104 | 14 | 35 | 5.95 | 6.28 | 5.3% |
| cpu | 3e+03 | 104 | 13 | 34 | 3.9 | 4.23 | 7.8% |
| cpu | 1e+04 | 104 | 13 | 34 | 3.9 | 4.23 | 7.8% |
| fargate | 1e+03 | 104 | 14 | 35 | 0.323 | 0.343 | 5.9% |
| fargate | 3e+03 | 104 | 13 | 34 | 0.247 | 0.267 | 7.6% |
| fargate | 1e+04 | 104 | 13 | 34 | 0.247 | 0.267 | 7.6% |

## mixed, m = 8: 400 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 1.7e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 294 | 99.8 | 294 | 278 | yes |
| cpu | 100 | ASAP (no roll-ups) | 294 | 99.8 | 294 | 278 | yes |
| cpu | 100 | PerQuery-CostAware | 516 | 97.4 | 516 | 612 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.97e+04 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| cpu | 300 | ASAP | 101 | 291 | 101 | 132 | yes |
| cpu | 300 | ASAP (no roll-ups) | 101 | 291 | 101 | 132 | yes |
| cpu | 300 | PerQuery-CostAware | 198 | 291 | 198 | 551 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.97e+04 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| cpu | 1e+03 | ASAP | 45.4 | 980 | 45.4 | 245 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 45.4 | 980 | 45.4 | 245 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 108 | 980 | 108 | 1.26e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.97e+04 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| cpu | 3e+03 | ASAP | 28.8 | 2.68e+03 | 28.8 | 151 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 28.8 | 2.68e+03 | 28.8 | 151 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 86.7 | 2.68e+03 | 86.7 | 1.13e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.97e+04 | 1.01e+03 | 2.97e+04 | 4.5e+04 | yes |
| cpu | 1e+04 | ASAP | 28.8 | 2.68e+03 | 28.8 | 151 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 28.8 | 2.68e+03 | 28.8 | 151 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 84.1 | 3.33e+03 | 84.1 | 616 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.97e+04 | 1.01e+03 | 2.97e+04 | 4.5e+04 | yes |
| fargate | 100 | ASAP | 12.7 | 96.9 | 297 | 151 | yes |
| fargate | 100 | ASAP (no roll-ups) | 12.7 | 96.9 | 297 | 151 | yes |
| fargate | 100 | PerQuery-CostAware | 23.6 | 97.4 | 516 | 606 | yes |
| fargate | 100 | AutoSketch-Adapted | 1.4e+03 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| fargate | 300 | ASAP | 4.69 | 291 | 101 | 132 | yes |
| fargate | 300 | ASAP (no roll-ups) | 4.69 | 291 | 101 | 132 | yes |
| fargate | 300 | PerQuery-CostAware | 10.4 | 291 | 198 | 545 | yes |
| fargate | 300 | AutoSketch-Adapted | 1.4e+03 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| fargate | 1e+03 | ASAP | 2.46 | 896 | 45.5 | 138 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 2.46 | 896 | 45.5 | 138 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 6.91 | 896 | 109 | 558 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.4e+03 | 1.01e+03 | 2.97e+04 | 4.5e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.84 | 2.68e+03 | 28.8 | 151 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 1.84 | 2.68e+03 | 28.8 | 151 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 6.06 | 2.68e+03 | 87.2 | 568 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.4e+03 | 1.01e+03 | 2.97e+04 | 4.5e+04 | yes |
| fargate | 1e+04 | ASAP | 1.84 | 2.68e+03 | 28.8 | 151 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 1.84 | 2.68e+03 | 28.8 | 151 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 6.02 | 3.26e+03 | 86.2 | 568 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.4e+03 | 1.01e+03 | 2.97e+04 | 4.5e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 112 | 112 | 294 | 294 | 0.0% |
| cpu | 300 | 0 | 104 | 104 | 101 | 101 | 0.0% |
| cpu | 1e+03 | 0 | 72 | 72 | 45.4 | 45.4 | 0.0% |
| cpu | 3e+03 | 0 | 64 | 64 | 28.8 | 28.8 | 0.0% |
| cpu | 1e+04 | 0 | 64 | 64 | 28.8 | 28.8 | 0.0% |
| fargate | 100 | 0 | 112 | 112 | 12.7 | 12.7 | 0.0% |
| fargate | 300 | 0 | 104 | 104 | 4.69 | 4.69 | 0.0% |
| fargate | 1e+03 | 0 | 72 | 72 | 2.46 | 2.46 | 0.0% |
| fargate | 3e+03 | 0 | 64 | 64 | 1.84 | 1.84 | 0.0% |
| fargate | 1e+04 | 0 | 64 | 64 | 1.84 | 1.84 | 0.0% |

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

## mixed, m = 16: 800 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 87.7 ms, PerQuery 87.7 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.39e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 588 | 99.8 | 588 | 556 | yes |
| cpu | 100 | ASAP (no roll-ups) | 588 | 99.8 | 588 | 556 | yes |
| cpu | 100 | PerQuery-CostAware | 1.03e+03 | 97.4 | 1.03e+03 | 1.22e+03 | yes |
| cpu | 100 | AutoSketch-Adapted | 5.95e+04 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| cpu | 300 | ASAP | 203 | 291 | 203 | 264 | yes |
| cpu | 300 | ASAP (no roll-ups) | 203 | 291 | 203 | 264 | yes |
| cpu | 300 | PerQuery-CostAware | 395 | 291 | 395 | 1.1e+03 | yes |
| cpu | 300 | AutoSketch-Adapted | 5.95e+04 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| cpu | 1e+03 | ASAP | 90.8 | 980 | 90.8 | 491 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 90.8 | 980 | 90.8 | 491 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 215 | 980 | 215 | 2.52e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 5.95e+04 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| cpu | 3e+03 | ASAP | 57.6 | 2.68e+03 | 57.6 | 302 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 57.6 | 2.68e+03 | 57.6 | 302 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 173 | 2.68e+03 | 173 | 2.27e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 5.95e+04 | 1.01e+03 | 5.95e+04 | 9e+04 | yes |
| cpu | 1e+04 | ASAP | 57.6 | 2.68e+03 | 57.6 | 302 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 57.6 | 2.68e+03 | 57.6 | 302 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 168 | 3.33e+03 | 168 | 1.23e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 5.95e+04 | 1.01e+03 | 5.95e+04 | 9e+04 | yes |
| fargate | 100 | ASAP | 25.4 | 96.9 | 593 | 301 | yes |
| fargate | 100 | ASAP (no roll-ups) | 25.4 | 96.9 | 593 | 301 | yes |
| fargate | 100 | PerQuery-CostAware | 47.2 | 97.4 | 1.03e+03 | 1.21e+03 | yes |
| fargate | 100 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| fargate | 300 | ASAP | 9.38 | 291 | 203 | 264 | yes |
| fargate | 300 | ASAP (no roll-ups) | 9.38 | 291 | 203 | 264 | yes |
| fargate | 300 | PerQuery-CostAware | 20.9 | 291 | 395 | 1.09e+03 | yes |
| fargate | 300 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| fargate | 1e+03 | ASAP | 4.91 | 896 | 91 | 276 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 4.91 | 896 | 91 | 276 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 13.8 | 896 | 219 | 1.12e+03 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.95e+04 | 9e+04 | **no** |
| fargate | 3e+03 | ASAP | 3.68 | 2.68e+03 | 57.6 | 302 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 3.68 | 2.68e+03 | 57.6 | 302 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 12.1 | 2.68e+03 | 174 | 1.14e+03 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.95e+04 | 9e+04 | yes |
| fargate | 1e+04 | ASAP | 3.68 | 2.68e+03 | 57.6 | 302 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 3.68 | 2.68e+03 | 57.6 | 302 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 12 | 3.26e+03 | 172 | 1.14e+03 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.95e+04 | 9e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 100 | 0 | 224 | 224 | 588 | 588 | 0.0% |
| cpu | 300 | 0 | 208 | 208 | 203 | 203 | 0.0% |
| cpu | 1e+03 | 0 | 144 | 144 | 90.8 | 90.8 | 0.0% |
| cpu | 3e+03 | 0 | 128 | 128 | 57.6 | 57.6 | 0.0% |
| cpu | 1e+04 | 0 | 128 | 128 | 57.6 | 57.6 | 0.0% |
| fargate | 100 | 0 | 224 | 224 | 25.4 | 25.4 | 0.0% |
| fargate | 300 | 0 | 208 | 208 | 9.38 | 9.38 | 0.0% |
| fargate | 1e+03 | 0 | 144 | 144 | 4.91 | 4.91 | 0.0% |
| fargate | 3e+03 | 0 | 128 | 128 | 3.68 | 3.68 | 0.0% |
| fargate | 1e+04 | 0 | 128 | 128 | 3.68 | 3.68 | 0.0% |

## mixed + multi-grouping, m = 8: 896 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.85e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 2.98e+04 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 2.98e+04 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| cpu | 1e+03 | ASAP | 47.2 | 980 | 47.2 | 254 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 49.8 | 980 | 49.8 | 266 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 116 | 980 | 116 | 1.29e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.98e+04 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| cpu | 3e+03 | ASAP | 30.6 | 2.68e+03 | 30.6 | 159 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 33.2 | 2.68e+03 | 33.2 | 172 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 95.2 | 2.68e+03 | 95.2 | 1.16e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.98e+04 | 1.01e+03 | 2.98e+04 | 4.5e+04 | yes |
| cpu | 1e+04 | ASAP | 30.6 | 2.68e+03 | 30.6 | 159 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 33.2 | 2.68e+03 | 33.2 | 172 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 92.7 | 3.33e+03 | 92.7 | 646 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.98e+04 | 1.01e+03 | 2.98e+04 | 4.5e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 1.41e+03 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 1.41e+03 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| fargate | 1e+03 | ASAP | 2.57 | 896 | 47.3 | 146 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 2.73 | 959 | 50 | 158 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 7.39 | 959 | 118 | 586 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.41e+03 | 1.01e+03 | 2.98e+04 | 4.5e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.95 | 2.68e+03 | 30.6 | 159 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 2.11 | 2.68e+03 | 33.3 | 171 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 6.53 | 2.68e+03 | 95.8 | 596 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.41e+03 | 1.01e+03 | 2.98e+04 | 4.5e+04 | yes |
| fargate | 1e+04 | ASAP | 1.95 | 2.68e+03 | 30.6 | 159 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 2.11 | 2.68e+03 | 33.3 | 171 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 6.49 | 3.26e+03 | 94.8 | 596 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.41e+03 | 1.01e+03 | 2.98e+04 | 4.5e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 416 | 112 | 280 | 47.2 | 49.8 | 5.3% |
| cpu | 3e+03 | 416 | 104 | 272 | 30.6 | 33.2 | 8.0% |
| cpu | 1e+04 | 416 | 104 | 272 | 30.6 | 33.2 | 8.0% |
| fargate | 1e+03 | 416 | 112 | 280 | 2.57 | 2.73 | 5.9% |
| fargate | 3e+03 | 416 | 104 | 272 | 1.95 | 2.11 | 7.7% |
| fargate | 1e+04 | 416 | 104 | 272 | 1.95 | 2.11 | 7.7% |

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

## mixed + multi-grouping, m = 16: 1792 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 490 ms, PerQuery 490 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 7.7e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 100 | AutoSketch-Adapted | 5.96e+04 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| cpu | 300 | ASAP | infeasible (tightest 490 ms) |||||
| cpu | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| cpu | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| cpu | 300 | AutoSketch-Adapted | 5.96e+04 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| cpu | 1e+03 | ASAP | 94.3 | 980 | 94.3 | 508 | yes |
| cpu | 1e+03 | ASAP (no roll-ups) | 99.6 | 980 | 99.6 | 532 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 232 | 980 | 232 | 2.57e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 5.96e+04 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| cpu | 3e+03 | ASAP | 61.1 | 2.68e+03 | 61.1 | 319 | yes |
| cpu | 3e+03 | ASAP (no roll-ups) | 66.4 | 2.68e+03 | 66.4 | 343 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 190 | 2.68e+03 | 190 | 2.33e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 5.96e+04 | 1.01e+03 | 5.96e+04 | 9e+04 | yes |
| cpu | 1e+04 | ASAP | 61.1 | 2.68e+03 | 61.1 | 319 | yes |
| cpu | 1e+04 | ASAP (no roll-ups) | 66.4 | 2.68e+03 | 66.4 | 343 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 185 | 3.33e+03 | 185 | 1.29e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 5.96e+04 | 1.01e+03 | 5.96e+04 | 9e+04 | yes |
| fargate | 100 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 100 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 100 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 100 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| fargate | 300 | ASAP | infeasible (tightest 490 ms) |||||
| fargate | 300 | ASAP (no roll-ups) | infeasible (tightest 490 ms) |||||
| fargate | 300 | PerQuery-CostAware | infeasible (tightest 490 ms) |||||
| fargate | 300 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| fargate | 1e+03 | ASAP | 5.13 | 896 | 94.6 | 292 | yes |
| fargate | 1e+03 | ASAP (no roll-ups) | 5.46 | 959 | 100 | 316 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 14.8 | 959 | 236 | 1.17e+03 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.96e+04 | 9e+04 | **no** |
| fargate | 3e+03 | ASAP | 3.89 | 2.68e+03 | 61.2 | 318 | yes |
| fargate | 3e+03 | ASAP (no roll-ups) | 4.22 | 2.68e+03 | 66.5 | 342 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 13.1 | 2.68e+03 | 192 | 1.19e+03 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.96e+04 | 9e+04 | yes |
| fargate | 1e+04 | ASAP | 3.89 | 2.68e+03 | 61.2 | 318 | yes |
| fargate | 1e+04 | ASAP (no roll-ups) | 4.22 | 2.68e+03 | 66.5 | 342 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 13 | 3.26e+03 | 190 | 1.19e+03 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.81e+03 | 1.01e+03 | 5.96e+04 | 9e+04 | yes |

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | SLA (ms) | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|---|
| cpu | 1e+03 | 832 | 224 | 560 | 94.3 | 99.6 | 5.3% |
| cpu | 3e+03 | 832 | 208 | 544 | 61.1 | 66.4 | 8.0% |
| cpu | 1e+04 | 832 | 208 | 544 | 61.1 | 66.4 | 8.0% |
| fargate | 1e+03 | 832 | 224 | 560 | 5.13 | 5.46 | 5.9% |
| fargate | 3e+03 | 832 | 208 | 544 | 3.89 | 4.22 | 7.7% |
| fargate | 1e+04 | 832 | 208 | 544 | 3.89 | 4.22 | 7.7% |
