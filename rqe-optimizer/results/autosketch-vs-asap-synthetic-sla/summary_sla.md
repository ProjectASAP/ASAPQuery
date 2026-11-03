# Version 2: cost by use under a batch latency SLA

Each method's cost by use (`w_cpu · AUC(CPU) + w_mem · AUC(memory)`) at each SLA; ASAP and PerQuery plan for the SLA (the cheapest plan whose batch latency, the longest chain, is at most it), AutoSketch's one plan ignores it.

## mixed: 50 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 92 ms, PerQuery 92 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 197 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 34.5 | 98.1 | 34.5 | 33.6 | yes |
| cpu | 100 | PerQuery-CostAware | 59.2 | 98.1 | 59.2 | 170 | yes |
| cpu | 100 | AutoSketch-Adapted | 3.41e+03 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| cpu | 300 | ASAP | 12.4 | 295 | 12.4 | 16.3 | yes |
| cpu | 300 | PerQuery-CostAware | 23.6 | 295 | 23.6 | 68.3 | yes |
| cpu | 300 | AutoSketch-Adapted | 3.41e+03 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.12 | 982 | 5.12 | 31.3 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 12 | 982 | 12 | 157 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 3.41e+03 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.32 | 2.94e+03 | 3.32 | 32.4 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 9.74 | 2.94e+03 | 9.74 | 161 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 3.41e+03 | 1.01e+03 | 3.41e+03 | 5.65e+03 | yes |
| cpu | 1e+04 | ASAP | 3.32 | 2.94e+03 | 3.32 | 32.4 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 9.55 | 3.84e+03 | 9.55 | 161 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 3.41e+03 | 1.01e+03 | 3.41e+03 | 5.65e+03 | yes |
| fargate | 100 | ASAP | 1.5 | 98.1 | 35 | 18.7 | yes |
| fargate | 100 | PerQuery-CostAware | 2.84 | 98.1 | 61.7 | 75.2 | yes |
| fargate | 100 | AutoSketch-Adapted | 163 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| fargate | 300 | ASAP | 0.573 | 295 | 12.4 | 16.2 | yes |
| fargate | 300 | PerQuery-CostAware | 1.26 | 295 | 23.7 | 67.6 | yes |
| fargate | 300 | AutoSketch-Adapted | 163 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.284 | 989 | 5.13 | 17.1 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 0.813 | 989 | 12.4 | 69.8 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 163 | 1.01e+03 | 3.41e+03 | 5.65e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.22 | 2.94e+03 | 3.35 | 19 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 0.72 | 2.97e+03 | 9.97 | 71.1 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 163 | 1.01e+03 | 3.41e+03 | 5.65e+03 | yes |
| fargate | 1e+04 | ASAP | 0.22 | 2.94e+03 | 3.35 | 19 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 0.72 | 2.97e+03 | 9.97 | 71.1 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 163 | 1.01e+03 | 3.41e+03 | 5.65e+03 | yes |

## mixed, r = 8: 92 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 92 ms, PerQuery 92 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 197 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 34.5 | 98.1 | 34.5 | 33.6 | yes |
| cpu | 100 | PerQuery-CostAware | 74.2 | 98.1 | 74.2 | 200 | yes |
| cpu | 100 | AutoSketch-Adapted | 4.09e+03 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| cpu | 300 | ASAP | 12.4 | 295 | 12.4 | 16.3 | yes |
| cpu | 300 | PerQuery-CostAware | 32.5 | 296 | 32.5 | 82 | yes |
| cpu | 300 | AutoSketch-Adapted | 4.09e+03 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| cpu | 1e+03 | ASAP | 5.15 | 982 | 5.15 | 31.3 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 19.3 | 982 | 19.3 | 188 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 4.09e+03 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| cpu | 3e+03 | ASAP | 3.39 | 2.94e+03 | 3.39 | 32.4 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 16.9 | 2.94e+03 | 16.9 | 191 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 4.09e+03 | 1.01e+03 | 4.09e+03 | 5.88e+03 | yes |
| cpu | 1e+04 | ASAP | 3.39 | 2.94e+03 | 3.39 | 32.4 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 16.7 | 3.84e+03 | 16.7 | 191 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 4.09e+03 | 1.01e+03 | 4.09e+03 | 5.88e+03 | yes |
| fargate | 100 | ASAP | 1.5 | 98.1 | 35 | 18.7 | yes |
| fargate | 100 | PerQuery-CostAware | 3.52 | 98.1 | 77 | 88.8 | yes |
| fargate | 100 | AutoSketch-Adapted | 192 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| fargate | 300 | ASAP | 0.574 | 295 | 12.4 | 16.2 | yes |
| fargate | 300 | PerQuery-CostAware | 1.68 | 296 | 32.5 | 81.3 | yes |
| fargate | 300 | AutoSketch-Adapted | 192 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| fargate | 1e+03 | ASAP | 0.285 | 989 | 5.17 | 17.1 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 1.18 | 989 | 19.9 | 83.8 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 192 | 1.01e+03 | 4.09e+03 | 5.88e+03 | **no** |
| fargate | 3e+03 | ASAP | 0.223 | 2.94e+03 | 3.42 | 19 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 1.08 | 2.97e+03 | 17.2 | 85.1 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 192 | 1.01e+03 | 4.09e+03 | 5.88e+03 | yes |
| fargate | 1e+04 | ASAP | 0.223 | 2.94e+03 | 3.42 | 19 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 1.08 | 2.97e+03 | 17.2 | 85.1 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 192 | 1.01e+03 | 4.09e+03 | 5.88e+03 | yes |

## mixed, m = 8: 400 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 92 ms, PerQuery 92 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 1.58e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 276 | 98.1 | 276 | 268 | yes |
| cpu | 100 | PerQuery-CostAware | 473 | 98.1 | 473 | 1.36e+03 | yes |
| cpu | 100 | AutoSketch-Adapted | 2.73e+04 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| cpu | 300 | ASAP | 98.9 | 295 | 98.9 | 131 | yes |
| cpu | 300 | PerQuery-CostAware | 189 | 295 | 189 | 547 | yes |
| cpu | 300 | AutoSketch-Adapted | 2.73e+04 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| cpu | 1e+03 | ASAP | 41 | 982 | 41 | 251 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 96.2 | 982 | 96.2 | 1.26e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 2.73e+04 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| cpu | 3e+03 | ASAP | 26.6 | 2.94e+03 | 26.6 | 259 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 77.9 | 2.94e+03 | 77.9 | 1.29e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 2.73e+04 | 1.01e+03 | 2.73e+04 | 4.52e+04 | yes |
| cpu | 1e+04 | ASAP | 26.6 | 2.94e+03 | 26.6 | 259 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 76.4 | 3.84e+03 | 76.4 | 1.29e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 2.73e+04 | 1.01e+03 | 2.73e+04 | 4.52e+04 | yes |
| fargate | 100 | ASAP | 12 | 98.1 | 280 | 149 | yes |
| fargate | 100 | PerQuery-CostAware | 22.7 | 98.1 | 494 | 602 | yes |
| fargate | 100 | AutoSketch-Adapted | 1.31e+03 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| fargate | 300 | ASAP | 4.59 | 295 | 99 | 129 | yes |
| fargate | 300 | PerQuery-CostAware | 10.1 | 295 | 190 | 541 | yes |
| fargate | 300 | AutoSketch-Adapted | 1.31e+03 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| fargate | 1e+03 | ASAP | 2.27 | 989 | 41.1 | 136 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 6.5 | 989 | 99.1 | 559 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 1.31e+03 | 1.01e+03 | 2.73e+04 | 4.52e+04 | **no** |
| fargate | 3e+03 | ASAP | 1.76 | 2.94e+03 | 26.8 | 152 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 5.76 | 2.97e+03 | 79.8 | 569 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 1.31e+03 | 1.01e+03 | 2.73e+04 | 4.52e+04 | yes |
| fargate | 1e+04 | ASAP | 1.76 | 2.94e+03 | 26.8 | 152 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 5.76 | 2.97e+03 | 79.8 | 569 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 1.31e+03 | 1.01e+03 | 2.73e+04 | 4.52e+04 | yes |

## mixed, m = 16: 800 RQEs

Sanity violations: 0. Tightest feasible SLA: ASAP 92 ms, PerQuery 92 ms. AutoSketch latency 1.01e+03 ms; planning time (search + measured benchmark) 3.16e+03 s.

| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | meets SLA |
|---|---|---|---|---|---|---|---|
| cpu | 100 | ASAP | 553 | 98.1 | 553 | 537 | yes |
| cpu | 100 | PerQuery-CostAware | 946 | 98.1 | 946 | 2.72e+03 | yes |
| cpu | 100 | AutoSketch-Adapted | 5.46e+04 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| cpu | 300 | ASAP | 198 | 295 | 198 | 261 | yes |
| cpu | 300 | PerQuery-CostAware | 378 | 295 | 378 | 1.09e+03 | yes |
| cpu | 300 | AutoSketch-Adapted | 5.46e+04 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| cpu | 1e+03 | ASAP | 81.9 | 982 | 81.9 | 501 | yes |
| cpu | 1e+03 | PerQuery-CostAware | 192 | 982 | 192 | 2.52e+03 | yes |
| cpu | 1e+03 | AutoSketch-Adapted | 5.46e+04 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| cpu | 3e+03 | ASAP | 53.2 | 2.94e+03 | 53.2 | 518 | yes |
| cpu | 3e+03 | PerQuery-CostAware | 156 | 2.94e+03 | 156 | 2.58e+03 | yes |
| cpu | 3e+03 | AutoSketch-Adapted | 5.46e+04 | 1.01e+03 | 5.46e+04 | 9.03e+04 | yes |
| cpu | 1e+04 | ASAP | 53.2 | 2.94e+03 | 53.2 | 518 | yes |
| cpu | 1e+04 | PerQuery-CostAware | 153 | 3.84e+03 | 153 | 2.58e+03 | yes |
| cpu | 1e+04 | AutoSketch-Adapted | 5.46e+04 | 1.01e+03 | 5.46e+04 | 9.03e+04 | yes |
| fargate | 100 | ASAP | 24 | 98.1 | 560 | 299 | yes |
| fargate | 100 | PerQuery-CostAware | 45.4 | 98.1 | 988 | 1.2e+03 | yes |
| fargate | 100 | AutoSketch-Adapted | 2.61e+03 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| fargate | 300 | ASAP | 9.17 | 295 | 198 | 259 | yes |
| fargate | 300 | PerQuery-CostAware | 20.2 | 295 | 379 | 1.08e+03 | yes |
| fargate | 300 | AutoSketch-Adapted | 2.61e+03 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| fargate | 1e+03 | ASAP | 4.54 | 989 | 82.2 | 273 | yes |
| fargate | 1e+03 | PerQuery-CostAware | 13 | 989 | 198 | 1.12e+03 | yes |
| fargate | 1e+03 | AutoSketch-Adapted | 2.61e+03 | 1.01e+03 | 5.46e+04 | 9.03e+04 | **no** |
| fargate | 3e+03 | ASAP | 3.52 | 2.94e+03 | 53.6 | 303 | yes |
| fargate | 3e+03 | PerQuery-CostAware | 11.5 | 2.97e+03 | 160 | 1.14e+03 | yes |
| fargate | 3e+03 | AutoSketch-Adapted | 2.61e+03 | 1.01e+03 | 5.46e+04 | 9.03e+04 | yes |
| fargate | 1e+04 | ASAP | 3.52 | 2.94e+03 | 53.6 | 303 | yes |
| fargate | 1e+04 | PerQuery-CostAware | 11.5 | 2.97e+03 | 160 | 1.14e+03 | yes |
| fargate | 1e+04 | AutoSketch-Adapted | 2.61e+03 | 1.01e+03 | 5.46e+04 | 9.03e+04 | yes |
