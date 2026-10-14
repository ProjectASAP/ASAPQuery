# Synthetic workload: AutoSketch vs. ASAP, cost by use

Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; Fargate prices, in $/hour). Latency: a plan's query latency (its longest chain, compaction then query, CPU elastic), and the median over its RQEs. AutoSketch's planning time is its search plus its measured benchmark.

## mixed + multi-grouping

104 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 316 probes, 19 distinct (metric, config); measured benchmark 481 s at N = 1e+08 (paper rate 1.14e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.6 | 2.6 | 22.4 | 1.34e+04 | 51.1 | 0.939 | 4.44 |
| cpu | ASAP (no roll-ups) | 2.93 | 2.93 | 24 | 1.34e+04 | 39 | 0.907 | 4.77 |
| cpu | PerQuery-CostAware | 8.84 | 8.84 | 76.4 | 3.33e+03 | 59.9 | 0.64 | 11.5 |
| cpu | AutoSketch-Adapted | 2.75e+03 | 2.75e+03 | 5.62e+03 | 1.01e+03 | 10.3 | 481 | 2.75e+03 |
| fargate | ASAP | 0.184 | 2.64 | 17.4 | 2.68e+03 | 51.1 | 1.07 | 0.255 |
| fargate | ASAP (no roll-ups) | 0.204 | 2.97 | 18.8 | 2.68e+03 | 39 | 0.938 | 0.275 |
| fargate | PerQuery-CostAware | 0.683 | 9 | 71.5 | 3.26e+03 | 59.9 | 0.645 | 0.792 |
| fargate | AutoSketch-Adapted | 136 | 2.75e+03 | 5.62e+03 | 1.01e+03 | 10.3 | 481 | 136 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.26), (649, 5.97), (813, 5.21), (980, 4.44), (1.08e+03, 4.38), (1.48e+03, 4.05), (2.68e+03, 2.64), (1.34e+04, 2.6)
- cpu, ASAP (no roll-ups): (490, 7.76), (649, 6.34), (813, 5.57), (980, 4.77), (1.08e+03, 4.71), (1.48e+03, 4.38), (2.68e+03, 2.97), (1.34e+04, 2.93)
- cpu, PerQuery-CostAware: (490, 16.6), (575, 14.6), (673, 13.3), (813, 12.4), (980, 11.5), (1.14e+03, 10.9), (1.34e+03, 10.4), (1.63e+03, 9.94), (1.96e+03, 9.89), (2.29e+03, 9.43), (2.68e+03, 9.05), (3.33e+03, 8.84)
- fargate, ASAP: (490, 0.367), (557, 0.318), (649, 0.315), (765, 0.286), (896, 0.255), (1.08e+03, 0.251), (1.3e+03, 0.248), (1.48e+03, 0.241), (2.29e+03, 0.213), (2.68e+03, 0.184)
- fargate, ASAP (no roll-ups): (490, 0.395), (557, 0.34), (649, 0.337), (765, 0.308), (896, 0.277), (959, 0.275), (1.08e+03, 0.271), (1.3e+03, 0.268), (1.48e+03, 0.262), (2.29e+03, 0.233), (2.68e+03, 0.204)
- fargate, PerQuery-CostAware: (490, 0.993), (575, 0.905), (673, 0.854), (813, 0.816), (959, 0.792), (1.14e+03, 0.761), (1.34e+03, 0.738), (1.63e+03, 0.723), (2.29e+03, 0.702), (2.68e+03, 0.688), (3.26e+03, 0.683)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 11 | 32 | 2.6 | 2.93 | 11.2% |
| fargate | 52 | 11 | 32 | 0.184 | 0.204 | 9.9% |

## mixed + multi-grouping, m = 8

832 RQEs on 72 streams; dropped: 0; sanity violations: 0. AutoSketch: 2528 probes, 152 distinct (metric, config); measured benchmark 3.85e+03 s at N = 1e+08 (paper rate 9.12e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 20.8 | 20.8 | 179 | 1.34e+04 | 51.1 | 6.49 | 35.5 |
| cpu | ASAP (no roll-ups) | 23.5 | 23.5 | 192 | 1.34e+04 | 39 | 6.12 | 38.2 |
| cpu | PerQuery-CostAware | 70.7 | 70.7 | 612 | 3.33e+03 | 59.9 | 5.6 | 92.3 |
| cpu | AutoSketch-Adapted | 2.2e+04 | 2.2e+04 | 4.5e+04 | 1.01e+03 | 10.3 | 3.85e+03 | 2.2e+04 |
| fargate | ASAP | 1.47 | 21.1 | 139 | 2.68e+03 | 51.1 | 8.69 | 2.04 |
| fargate | ASAP (no roll-ups) | 1.63 | 23.8 | 151 | 2.68e+03 | 39 | 7.4 | 2.2 |
| fargate | PerQuery-CostAware | 5.46 | 72 | 572 | 3.26e+03 | 59.9 | 5.65 | 6.33 |
| fargate | AutoSketch-Adapted | 1.09e+03 | 2.2e+04 | 4.5e+04 | 1.01e+03 | 10.3 | 3.85e+03 | 1.09e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 58.1), (649, 47.8), (813, 41.7), (980, 35.5), (1.08e+03, 35), (1.48e+03, 32.4), (2.68e+03, 21.1), (1.34e+04, 20.8)
- cpu, ASAP (no roll-ups): (490, 62), (649, 50.7), (813, 44.6), (980, 38.2), (1.08e+03, 37.7), (1.48e+03, 35.1), (2.68e+03, 23.7), (1.34e+04, 23.5)
- cpu, PerQuery-CostAware: (490, 133), (575, 117), (673, 106), (813, 99), (980, 92.3), (1.14e+03, 87.4), (1.34e+03, 82.9), (1.63e+03, 79.5), (1.96e+03, 79.1), (2.29e+03, 75.4), (2.68e+03, 72.4), (3.33e+03, 70.7)
- fargate, ASAP: (490, 2.93), (557, 2.55), (649, 2.52), (765, 2.29), (896, 2.04), (1.08e+03, 2.01), (1.3e+03, 1.98), (1.48e+03, 1.93), (2.29e+03, 1.71), (2.68e+03, 1.47)
- fargate, ASAP (no roll-ups): (490, 3.16), (557, 2.72), (649, 2.7), (765, 2.46), (896, 2.22), (959, 2.2), (1.08e+03, 2.17), (1.3e+03, 2.14), (1.48e+03, 2.09), (2.29e+03, 1.87), (2.68e+03, 1.63)
- fargate, PerQuery-CostAware: (490, 7.95), (575, 7.24), (673, 6.84), (813, 6.53), (959, 6.33), (1.14e+03, 6.09), (1.34e+03, 5.91), (1.63e+03, 5.78), (2.29e+03, 5.62), (2.68e+03, 5.5), (3.26e+03, 5.46)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 88 | 256 | 20.8 | 23.5 | 11.2% |
| fargate | 416 | 88 | 256 | 1.47 | 1.63 | 9.9% |

## mixed + multi-grouping, m = 16

1664 RQEs on 144 streams; dropped: 0; sanity violations: 0. AutoSketch: 5056 probes, 304 distinct (metric, config); measured benchmark 7.7e+03 s at N = 1e+08 (paper rate 1.82e+04 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 41.7 | 41.7 | 359 | 1.34e+04 | 51.1 | 15.6 | 71.1 |
| cpu | ASAP (no roll-ups) | 47 | 47 | 383 | 1.34e+04 | 39 | 14.3 | 76.4 |
| cpu | PerQuery-CostAware | 141 | 141 | 1.22e+03 | 3.33e+03 | 59.9 | 12.9 | 185 |
| cpu | AutoSketch-Adapted | 4.4e+04 | 4.4e+04 | 8.99e+04 | 1.01e+03 | 10.3 | 7.7e+03 | 4.4e+04 |
| fargate | ASAP | 2.95 | 42.2 | 278 | 2.68e+03 | 51.1 | 22.6 | 4.08 |
| fargate | ASAP (no roll-ups) | 3.27 | 47.6 | 302 | 2.68e+03 | 39 | 18.6 | 4.41 |
| fargate | PerQuery-CostAware | 10.9 | 144 | 1.14e+03 | 3.26e+03 | 59.9 | 13.1 | 12.7 |
| fargate | AutoSketch-Adapted | 2.18e+03 | 4.4e+04 | 8.99e+04 | 1.01e+03 | 10.3 | 7.7e+03 | 2.18e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 116), (649, 95.6), (813, 83.3), (980, 71.1), (1.08e+03, 70.1), (1.48e+03, 64.8), (2.68e+03, 42.2), (1.34e+04, 41.7)
- cpu, ASAP (no roll-ups): (490, 124), (649, 101), (813, 89.1), (980, 76.4), (1.08e+03, 75.4), (1.48e+03, 70.1), (2.68e+03, 47.4), (1.34e+04, 47)
- cpu, PerQuery-CostAware: (490, 266), (575, 233), (673, 212), (813, 198), (980, 185), (1.14e+03, 175), (1.34e+03, 166), (1.63e+03, 159), (1.96e+03, 158), (2.29e+03, 151), (2.68e+03, 145), (3.33e+03, 141)
- fargate, ASAP: (490, 5.87), (557, 5.09), (649, 5.05), (765, 4.58), (896, 4.08), (1.08e+03, 4.01), (1.3e+03, 3.97), (1.48e+03, 3.86), (2.29e+03, 3.41), (2.68e+03, 2.95)
- fargate, ASAP (no roll-ups): (490, 6.32), (557, 5.45), (649, 5.4), (765, 4.93), (896, 4.43), (959, 4.41), (1.08e+03, 4.34), (1.3e+03, 4.29), (1.48e+03, 4.19), (2.29e+03, 3.73), (2.68e+03, 3.27)
- fargate, PerQuery-CostAware: (490, 15.9), (575, 14.5), (673, 13.7), (813, 13.1), (959, 12.7), (1.14e+03, 12.2), (1.34e+03, 11.8), (1.63e+03, 11.6), (2.29e+03, 11.2), (2.68e+03, 11), (3.26e+03, 10.9)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 176 | 512 | 41.7 | 47 | 11.2% |
| fargate | 832 | 176 | 512 | 2.95 | 3.27 | 9.9% |

## mixed + multi-grouping, r = 8

200 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 596 probes, 19 distinct (metric, config); measured benchmark 481 s at N = 1e+08 (paper rate 1.14e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.71 | 2.71 | 17.4 | 2.68e+03 | 51.1 | 1.72 | 4.49 |
| cpu | ASAP (no roll-ups) | 3.04 | 3.04 | 18.9 | 2.68e+03 | 39 | 1.64 | 4.83 |
| cpu | PerQuery-CostAware | 15.6 | 15.6 | 107 | 3.33e+03 | 34 | 1.26 | 18.5 |
| cpu | AutoSketch-Adapted | 3.3e+03 | 3.3e+03 | 5.85e+03 | 1.01e+03 | 5.33 | 481 | 3.3e+03 |
| fargate | ASAP | 0.187 | 2.72 | 17.4 | 2.68e+03 | 51.1 | 1.99 | 0.257 |
| fargate | ASAP (no roll-ups) | 0.207 | 3.05 | 18.8 | 2.68e+03 | 39 | 1.82 | 0.277 |
| fargate | PerQuery-CostAware | 1.02 | 15.8 | 85.5 | 3.26e+03 | 34 | 1.27 | 1.14 |
| fargate | AutoSketch-Adapted | 160 | 3.3e+03 | 5.85e+03 | 1.01e+03 | 5.33 | 481 | 160 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.29), (557, 6.09), (649, 6.02), (765, 5.28), (896, 4.51), (980, 4.49), (1.08e+03, 4.44), (1.3e+03, 4.38), (1.48e+03, 4.11), (2.29e+03, 3.42), (2.68e+03, 2.71)
- cpu, ASAP (no roll-ups): (490, 7.79), (557, 6.46), (649, 6.38), (765, 5.65), (896, 4.87), (980, 4.83), (1.08e+03, 4.77), (1.3e+03, 4.71), (1.48e+03, 4.44), (2.29e+03, 3.75), (2.68e+03, 3.04)
- cpu, PerQuery-CostAware: (490, 24.3), (575, 21.7), (673, 20.3), (813, 19.3), (980, 18.5), (1.14e+03, 17.9), (1.34e+03, 17.2), (1.63e+03, 16.7), (1.96e+03, 16.7), (2.29e+03, 16.2), (2.68e+03, 15.8), (3.33e+03, 15.6)
- fargate, ASAP: (490, 0.368), (557, 0.32), (649, 0.317), (765, 0.288), (896, 0.257), (1.08e+03, 0.253), (1.3e+03, 0.25), (1.48e+03, 0.244), (2.29e+03, 0.216), (2.68e+03, 0.187)
- fargate, ASAP (no roll-ups): (490, 0.396), (557, 0.342), (649, 0.339), (765, 0.31), (896, 0.279), (959, 0.277), (1.08e+03, 0.273), (1.3e+03, 0.27), (1.48e+03, 0.264), (2.29e+03, 0.236), (2.68e+03, 0.207)
- fargate, PerQuery-CostAware: (490, 1.37), (575, 1.26), (673, 1.2), (813, 1.16), (959, 1.14), (1.14e+03, 1.11), (1.34e+03, 1.08), (1.63e+03, 1.06), (2.29e+03, 1.04), (2.68e+03, 1.03), (3.26e+03, 1.02)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 11 | 32 | 2.71 | 3.04 | 10.9% |
| fargate | 104 | 11 | 32 | 0.187 | 0.207 | 9.8% |

## mixed

42 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 150 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.38 | 2.38 | 21.4 | 1.34e+04 | 97.4 | 0.436 | 4.22 |
| cpu | ASAP (no roll-ups) | 2.38 | 2.38 | 21.4 | 1.34e+04 | 97.4 | 0.436 | 4.22 |
| cpu | PerQuery-CostAware | 7.77 | 7.77 | 72.8 | 3.33e+03 | 324 | 0.492 | 10.5 |
| cpu | AutoSketch-Adapted | 2.74e+03 | 2.74e+03 | 5.62e+03 | 1.01e+03 | 10.3 | 212 | 2.74e+03 |
| fargate | ASAP | 0.17 | 2.41 | 16.3 | 2.68e+03 | 51.1 | 0.476 | 0.242 |
| fargate | ASAP (no roll-ups) | 0.17 | 2.41 | 16.3 | 2.68e+03 | 51.1 | 0.475 | 0.242 |
| fargate | PerQuery-CostAware | 0.623 | 7.92 | 67.9 | 3.26e+03 | 269 | 0.499 | 0.732 |
| fargate | AutoSketch-Adapted | 136 | 2.74e+03 | 5.62e+03 | 1.01e+03 | 10.3 | 212 | 136 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.5), (138, 21.8), (206, 14.1), (342, 9.69), (541, 5.83), (813, 4.99), (980, 4.22), (1.3e+03, 4.1), (1.48e+03, 3.83), (2.68e+03, 2.41), (1.34e+04, 2.38)
- cpu, ASAP (no roll-ups): (87.7, 44.5), (138, 21.8), (206, 14.1), (342, 9.69), (541, 5.83), (813, 4.99), (980, 4.22), (1.3e+03, 4.1), (1.48e+03, 3.83), (2.68e+03, 2.41), (1.34e+04, 2.38)
- cpu, PerQuery-CostAware: (87.7, 73.3), (118, 47.1), (164, 32.7), (235, 24.7), (324, 19.6), (452, 15.8), (575, 13.4), (829, 11), (980, 10.5), (1.14e+03, 9.86), (1.63e+03, 8.87), (2.29e+03, 8.36), (3.33e+03, 7.77)
- fargate, ASAP: (87.7, 1.89), (115, 1.14), (163, 0.755), (219, 0.628), (291, 0.499), (386, 0.418), (541, 0.305), (765, 0.272), (896, 0.242), (1.3e+03, 0.234), (1.48e+03, 0.228), (2.68e+03, 0.17)
- fargate, ASAP (no roll-ups): (87.7, 1.89), (115, 1.14), (163, 0.755), (219, 0.628), (291, 0.499), (386, 0.418), (541, 0.305), (765, 0.272), (896, 0.242), (1.3e+03, 0.234), (1.48e+03, 0.228), (2.68e+03, 0.17)
- fargate, PerQuery-CostAware: (87.7, 3.31), (118, 2.22), (164, 1.63), (233, 1.33), (323, 1.09), (452, 0.938), (575, 0.841), (829, 0.747), (896, 0.732), (1.14e+03, 0.701), (1.63e+03, 0.663), (2.29e+03, 0.642), (3.26e+03, 0.623)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 6 | 6 | 2.38 | 2.38 | 0.0% |
| fargate | 0 | 6 | 6 | 0.17 | 0.17 | 0.0% |

## mixed, m = 8

336 RQEs on 48 streams; dropped: 0; sanity violations: 0. AutoSketch: 1200 probes, 80 distinct (metric, config); measured benchmark 1.7e+03 s at N = 1e+08 (paper rate 4.8e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 19.1 | 19.1 | 171 | 1.34e+04 | 97.4 | 3.74 | 33.8 |
| cpu | ASAP (no roll-ups) | 19.1 | 19.1 | 171 | 1.34e+04 | 97.4 | 3.74 | 33.8 |
| cpu | PerQuery-CostAware | 62.2 | 62.2 | 582 | 3.33e+03 | 324 | 4.2 | 83.7 |
| cpu | AutoSketch-Adapted | 2.19e+04 | 2.19e+04 | 4.49e+04 | 1.01e+03 | 10.3 | 1.7e+03 | 2.19e+04 |
| fargate | ASAP | 1.36 | 19.3 | 131 | 2.68e+03 | 51.1 | 4.04 | 1.93 |
| fargate | ASAP (no roll-ups) | 1.36 | 19.3 | 131 | 2.68e+03 | 51.1 | 4.04 | 1.93 |
| fargate | PerQuery-CostAware | 4.98 | 63.4 | 543 | 3.26e+03 | 269 | 4.26 | 5.85 |
| fargate | AutoSketch-Adapted | 1.09e+03 | 2.19e+04 | 4.49e+04 | 1.01e+03 | 10.3 | 1.7e+03 | 1.09e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 356), (138, 174), (206, 113), (342, 77.5), (541, 46.6), (813, 39.9), (980, 33.8), (1.3e+03, 32.8), (1.48e+03, 30.6), (2.68e+03, 19.3), (1.34e+04, 19.1)
- cpu, ASAP (no roll-ups): (87.7, 356), (138, 174), (206, 113), (342, 77.5), (541, 46.6), (813, 39.9), (980, 33.8), (1.3e+03, 32.8), (1.48e+03, 30.6), (2.68e+03, 19.3), (1.34e+04, 19.1)
- cpu, PerQuery-CostAware: (87.7, 586), (118, 377), (164, 261), (235, 197), (324, 157), (452, 127), (575, 107), (829, 88.1), (980, 83.7), (1.14e+03, 78.9), (1.63e+03, 71), (2.29e+03, 66.9), (3.33e+03, 62.2)
- fargate, ASAP: (87.7, 15.1), (115, 9.14), (163, 6.04), (219, 5.02), (291, 4), (386, 3.35), (541, 2.44), (765, 2.18), (896, 1.93), (1.3e+03, 1.87), (1.48e+03, 1.82), (2.68e+03, 1.36)
- fargate, ASAP (no roll-ups): (87.7, 15.1), (115, 9.14), (163, 6.04), (219, 5.02), (291, 4), (386, 3.35), (541, 2.44), (765, 2.18), (896, 1.93), (1.3e+03, 1.87), (1.48e+03, 1.82), (2.68e+03, 1.36)
- fargate, PerQuery-CostAware: (87.7, 26.5), (118, 17.8), (164, 13), (233, 10.6), (323, 8.71), (452, 7.5), (575, 6.73), (829, 5.98), (896, 5.85), (1.14e+03, 5.61), (1.63e+03, 5.3), (2.29e+03, 5.14), (3.26e+03, 4.98)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 48 | 48 | 19.1 | 19.1 | 0.0% |
| fargate | 0 | 48 | 48 | 1.36 | 1.36 | 0.0% |

## mixed, m = 16

672 RQEs on 96 streams; dropped: 0; sanity violations: 0. AutoSketch: 2400 probes, 160 distinct (metric, config); measured benchmark 3.39e+03 s at N = 1e+08 (paper rate 9.6e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 38.1 | 38.1 | 342 | 1.34e+04 | 97.4 | 8.12 | 67.6 |
| cpu | ASAP (no roll-ups) | 38.1 | 38.1 | 342 | 1.34e+04 | 97.4 | 8.13 | 67.6 |
| cpu | PerQuery-CostAware | 124 | 124 | 1.16e+03 | 3.33e+03 | 324 | 8.87 | 167 |
| cpu | AutoSketch-Adapted | 4.38e+04 | 4.38e+04 | 8.99e+04 | 1.01e+03 | 10.3 | 3.39e+03 | 4.38e+04 |
| fargate | ASAP | 2.73 | 38.6 | 261 | 2.68e+03 | 51.1 | 10.6 | 3.86 |
| fargate | ASAP (no roll-ups) | 2.73 | 38.6 | 261 | 2.68e+03 | 51.1 | 10.6 | 3.86 |
| fargate | PerQuery-CostAware | 9.97 | 127 | 1.09e+03 | 3.26e+03 | 269 | 9.01 | 11.7 |
| fargate | AutoSketch-Adapted | 2.17e+03 | 4.38e+04 | 8.99e+04 | 1.01e+03 | 10.3 | 3.39e+03 | 2.17e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 712), (138, 349), (206, 226), (342, 155), (541, 93.2), (813, 79.8), (980, 67.6), (1.3e+03, 65.6), (1.48e+03, 61.3), (2.68e+03, 38.6), (1.34e+04, 38.1)
- cpu, ASAP (no roll-ups): (87.7, 712), (138, 349), (206, 226), (342, 155), (541, 93.2), (813, 79.8), (980, 67.6), (1.3e+03, 65.6), (1.48e+03, 61.3), (2.68e+03, 38.6), (1.34e+04, 38.1)
- cpu, PerQuery-CostAware: (87.7, 1.17e+03), (118, 753), (164, 523), (235, 395), (324, 313), (452, 253), (575, 214), (829, 176), (980, 167), (1.14e+03, 158), (1.63e+03, 142), (2.29e+03, 134), (3.33e+03, 124)
- fargate, ASAP: (87.7, 30.2), (115, 18.3), (163, 12.1), (219, 10), (291, 7.99), (386, 6.69), (541, 4.88), (765, 4.36), (896, 3.86), (1.3e+03, 3.75), (1.48e+03, 3.65), (2.68e+03, 2.73)
- fargate, ASAP (no roll-ups): (87.7, 30.2), (115, 18.3), (163, 12.1), (219, 10), (291, 7.99), (386, 6.69), (541, 4.88), (765, 4.36), (896, 3.86), (1.3e+03, 3.75), (1.48e+03, 3.65), (2.68e+03, 2.73)
- fargate, PerQuery-CostAware: (87.7, 53), (118, 35.6), (164, 26), (233, 21.3), (323, 17.4), (452, 15), (575, 13.5), (829, 12), (896, 11.7), (1.14e+03, 11.2), (1.63e+03, 10.6), (2.29e+03, 10.3), (3.26e+03, 9.97)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 96 | 96 | 38.1 | 38.1 | 0.0% |
| fargate | 0 | 96 | 96 | 2.73 | 2.73 | 0.0% |

## mixed, r = 8

76 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 264 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.47 | 2.47 | 16.3 | 2.68e+03 | 97.4 | 1.01 | 4.25 |
| cpu | ASAP (no roll-ups) | 2.47 | 2.47 | 16.3 | 2.68e+03 | 97.4 | 1.01 | 4.25 |
| cpu | PerQuery-CostAware | 13.6 | 13.6 | 103 | 3.33e+03 | 77.9 | 0.949 | 16.5 |
| cpu | AutoSketch-Adapted | 3.28e+03 | 3.28e+03 | 5.85e+03 | 1.01e+03 | 10.1 | 212 | 3.28e+03 |
| fargate | ASAP | 0.173 | 2.47 | 16.3 | 2.68e+03 | 97.4 | 1.24 | 0.243 |
| fargate | ASAP (no roll-ups) | 0.173 | 2.47 | 16.3 | 2.68e+03 | 97.4 | 1.24 | 0.243 |
| fargate | PerQuery-CostAware | 0.919 | 13.8 | 80.9 | 3.26e+03 | 64.7 | 0.959 | 1.04 |
| fargate | AutoSketch-Adapted | 159 | 3.28e+03 | 5.85e+03 | 1.01e+03 | 10.1 | 212 | 159 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.5), (115, 26.4), (163, 16.9), (219, 13.8), (291, 10.6), (389, 8.6), (541, 5.85), (765, 5.04), (980, 4.25), (1.3e+03, 4.14), (1.48e+03, 3.87), (2.68e+03, 2.47)
- cpu, ASAP (no roll-ups): (87.7, 44.5), (115, 26.4), (163, 16.9), (219, 13.8), (291, 10.6), (389, 8.6), (541, 5.85), (765, 5.04), (980, 4.25), (1.3e+03, 4.14), (1.48e+03, 3.87), (2.68e+03, 2.47)
- cpu, PerQuery-CostAware: (87.7, 88.3), (118, 59.2), (164, 42.1), (235, 32.9), (324, 26.9), (452, 22.9), (575, 19.6), (829, 17), (980, 16.5), (1.14e+03, 15.8), (1.63e+03, 14.7), (2.29e+03, 14.2), (3.33e+03, 13.6)
- fargate, ASAP: (87.7, 1.89), (115, 1.14), (163, 0.755), (219, 0.628), (291, 0.5), (386, 0.419), (541, 0.306), (765, 0.273), (896, 0.243), (1.3e+03, 0.236), (1.48e+03, 0.23), (2.68e+03, 0.173)
- fargate, ASAP (no roll-ups): (87.7, 1.89), (115, 1.14), (163, 0.755), (219, 0.628), (291, 0.5), (386, 0.419), (541, 0.306), (765, 0.273), (896, 0.243), (1.3e+03, 0.236), (1.48e+03, 0.23), (2.68e+03, 0.173)
- fargate, PerQuery-CostAware: (87.7, 3.98), (118, 2.77), (164, 2.07), (233, 1.72), (323, 1.44), (452, 1.29), (575, 1.15), (829, 1.05), (896, 1.04), (1.14e+03, 1), (1.63e+03, 0.959), (2.29e+03, 0.938), (3.26e+03, 0.919)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 6 | 6 | 2.47 | 2.47 | 0.0% |
| fargate | 0 | 6 | 6 | 0.173 | 0.173 | 0.0% |

## multi-grouping

62 RQEs on 3 streams; dropped: 0; sanity violations: 0. AutoSketch: 166 probes, 9 distinct (metric, config); measured benchmark 269 s at N = 1e+08 (paper rate 540 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 0.221 | 0.221 | 1.06 | 557 | 24.6 | 0.412 | 0.324 |
| cpu | ASAP (no roll-ups) | 0.551 | 0.551 | 2.6 | 959 | 19.5 | 0.381 | 0.823 |
| cpu | PerQuery-CostAware | 1.07 | 1.07 | 3.69 | 959 | 19.5 | 0.224 | 1.33 |
| cpu | AutoSketch-Adapted | 10.2 | 10.2 | 3.55 | 490 | 4.9 | 269 | 10.2 |
| fargate | ASAP | 0.0137 | 0.224 | 1.03 | 557 | 24.6 | 0.436 | 0.0187 |
| fargate | ASAP (no roll-ups) | 0.0339 | 0.559 | 2.52 | 959 | 16.2 | 0.386 | 0.0469 |
| fargate | PerQuery-CostAware | 0.0597 | 1.08 | 3.61 | 959 | 16.2 | 0.225 | 0.0701 |
| fargate | AutoSketch-Adapted | 0.43 | 10.2 | 3.55 | 490 | 4.9 | 269 | 0.43 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 0.324), (557, 0.221)
- cpu, ASAP (no roll-ups): (490, 0.823), (557, 0.594), (624, 0.586), (959, 0.551)
- cpu, PerQuery-CostAware: (490, 1.33), (557, 1.17), (624, 1.09), (959, 1.07)
- fargate, ASAP: (490, 0.0187), (557, 0.0137)
- fargate, ASAP (no roll-ups): (490, 0.0469), (557, 0.0356), (959, 0.0339)
- fargate, PerQuery-CostAware: (490, 0.0701), (557, 0.0639), (624, 0.0605), (959, 0.0597)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 5 | 26 | 0.221 | 0.551 | 59.9% |
| fargate | 52 | 5 | 26 | 0.0137 | 0.0339 | 59.7% |

## multi-grouping, m = 8

496 RQEs on 24 streams; dropped: 0; sanity violations: 0. AutoSketch: 1328 probes, 72 distinct (metric, config); measured benchmark 2.15e+03 s at N = 1e+08 (paper rate 4.32e+03 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 1.77 | 1.77 | 8.52 | 557 | 24.6 | 3.4 | 2.59 |
| cpu | ASAP (no roll-ups) | 4.41 | 4.41 | 20.8 | 959 | 19.5 | 3.1 | 6.58 |
| cpu | PerQuery-CostAware | 8.55 | 8.55 | 29.5 | 959 | 19.5 | 1.93 | 10.6 |
| cpu | AutoSketch-Adapted | 81.7 | 81.7 | 28.4 | 490 | 4.9 | 2.15e+03 | 81.7 |
| fargate | ASAP | 0.109 | 1.79 | 8.22 | 557 | 24.6 | 3.63 | 0.15 |
| fargate | ASAP (no roll-ups) | 0.271 | 4.47 | 20.2 | 959 | 16.2 | 3.21 | 0.375 |
| fargate | PerQuery-CostAware | 0.477 | 8.61 | 28.9 | 959 | 16.2 | 1.93 | 0.56 |
| fargate | AutoSketch-Adapted | 3.44 | 81.7 | 28.4 | 490 | 4.9 | 2.15e+03 | 3.44 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 2.59), (557, 1.77)
- cpu, ASAP (no roll-ups): (490, 6.58), (557, 4.76), (624, 4.69), (959, 4.41)
- cpu, PerQuery-CostAware: (490, 10.6), (557, 9.39), (624, 8.71), (959, 8.55)
- fargate, ASAP: (490, 0.15), (557, 0.109)
- fargate, ASAP (no roll-ups): (490, 0.375), (557, 0.285), (959, 0.271)
- fargate, PerQuery-CostAware: (490, 0.56), (557, 0.511), (624, 0.484), (959, 0.477)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 40 | 208 | 1.77 | 4.41 | 59.9% |
| fargate | 416 | 40 | 208 | 0.109 | 0.271 | 59.7% |

## multi-grouping, m = 16

992 RQEs on 48 streams; dropped: 0; sanity violations: 0. AutoSketch: 2656 probes, 144 distinct (metric, config); measured benchmark 4.31e+03 s at N = 1e+08 (paper rate 8.64e+03 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.53 | 3.53 | 17 | 557 | 24.6 | 7.01 | 5.18 |
| cpu | ASAP (no roll-ups) | 8.81 | 8.81 | 41.5 | 959 | 19.5 | 6.3 | 13.2 |
| cpu | PerQuery-CostAware | 17.1 | 17.1 | 59 | 959 | 19.5 | 4.09 | 21.2 |
| cpu | AutoSketch-Adapted | 163 | 163 | 56.8 | 490 | 4.9 | 4.31e+03 | 163 |
| fargate | ASAP | 0.218 | 3.59 | 16.4 | 557 | 24.6 | 9.83 | 0.3 |
| fargate | ASAP (no roll-ups) | 0.542 | 8.94 | 40.4 | 959 | 16.2 | 6.48 | 0.75 |
| fargate | PerQuery-CostAware | 0.955 | 17.2 | 57.8 | 959 | 16.2 | 4.07 | 1.12 |
| fargate | AutoSketch-Adapted | 6.87 | 163 | 56.8 | 490 | 4.9 | 4.31e+03 | 6.87 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 5.18), (557, 3.53)
- cpu, ASAP (no roll-ups): (490, 13.2), (557, 9.51), (624, 9.37), (959, 8.81)
- cpu, PerQuery-CostAware: (490, 21.2), (557, 18.8), (624, 17.4), (959, 17.1)
- fargate, ASAP: (490, 0.3), (557, 0.218)
- fargate, ASAP (no roll-ups): (490, 0.75), (557, 0.57), (959, 0.542)
- fargate, PerQuery-CostAware: (490, 1.12), (557, 1.02), (624, 0.967), (959, 0.955)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 80 | 416 | 3.53 | 8.81 | 59.9% |
| fargate | 832 | 80 | 416 | 0.218 | 0.542 | 59.7% |

## multi-grouping, r = 8

124 RQEs on 3 streams; dropped: 0; sanity violations: 0. AutoSketch: 332 probes, 9 distinct (metric, config); measured benchmark 269 s at N = 1e+08 (paper rate 540 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 0.242 | 0.242 | 1.06 | 557 | 24.6 | 0.917 | 0.344 |
| cpu | ASAP (no roll-ups) | 0.573 | 0.573 | 2.6 | 959 | 19.5 | 0.851 | 0.841 |
| cpu | PerQuery-CostAware | 2.04 | 2.04 | 4.69 | 959 | 19.5 | 0.453 | 2.33 |
| cpu | AutoSketch-Adapted | 12.3 | 12.3 | 4.52 | 490 | 4.9 | 269 | 12.3 |
| fargate | ASAP | 0.0145 | 0.245 | 1.03 | 557 | 24.6 | 1.02 | 0.0196 |
| fargate | ASAP (no roll-ups) | 0.0347 | 0.581 | 2.52 | 959 | 16.2 | 0.875 | 0.0476 |
| fargate | PerQuery-CostAware | 0.104 | 2.05 | 4.62 | 959 | 16.2 | 0.456 | 0.115 |
| fargate | AutoSketch-Adapted | 0.517 | 12.3 | 4.52 | 490 | 4.9 | 269 | 0.517 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 0.344), (557, 0.242)
- cpu, ASAP (no roll-ups): (490, 0.841), (557, 0.613), (624, 0.606), (959, 0.573)
- cpu, PerQuery-CostAware: (490, 2.33), (557, 2.15), (624, 2.06), (959, 2.04)
- fargate, ASAP: (490, 0.0196), (557, 0.0145)
- fargate, ASAP (no roll-ups): (490, 0.0476), (557, 0.0364), (959, 0.0347)
- fargate, PerQuery-CostAware: (490, 0.115), (557, 0.108), (624, 0.104), (959, 0.104)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 5 | 26 | 0.242 | 0.573 | 57.8% |
| fargate | 104 | 5 | 26 | 0.0145 | 0.0347 | 58.2% |
