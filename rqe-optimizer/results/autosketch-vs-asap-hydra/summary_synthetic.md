# Synthetic workload: AutoSketch vs. ASAP, cost by use

Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; Fargate prices, in $/hour). Latency: a plan's query latency (its longest chain, compaction then query, CPU elastic), and the median over its RQEs. AutoSketch's planning time is its search plus its measured benchmark.

## mixed + multi-grouping

104 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 238 probes, 15 distinct (metric, config); measured benchmark 396 s at N = 1e+08 (paper rate 900 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.93 | 2.93 | 17.8 | 1.34e+04 | 46.7 | 2.88 | 4.71 |
| cpu | ASAP (no roll-ups) | 3.26 | 3.26 | 19.2 | 1.34e+04 | 39 | 2.86 | 5.04 |
| cpu | PerQuery-CostAware | 8.74 | 8.74 | 43.3 | 3.33e+03 | 59.1 | 5.03 | 11.1 |
| cpu | AutoSketch-Adapted | 2.71e+03 | 2.71e+03 | 4.01e+03 | 1.01e+03 | 9.45 | 396 | 2.71e+03 |
| fargate | ASAP | 0.177 | 2.96 | 12.7 | 2.68e+03 | 46.7 | 2.92 | 0.243 |
| fargate | ASAP (no roll-ups) | 0.197 | 3.29 | 14.2 | 2.68e+03 | 39 | 2.89 | 0.263 |
| fargate | PerQuery-CostAware | 0.545 | 8.85 | 42 | 2.97e+03 | 59.1 | 5.03 | 0.635 |
| fargate | AutoSketch-Adapted | 128 | 2.71e+03 | 4.01e+03 | 1.01e+03 | 9.45 | 396 | 128 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.55), (658, 6.28), (765, 5.52), (989, 4.71), (1.19e+03, 4.65), (1.48e+03, 4.38), (2.68e+03, 2.96), (1.34e+04, 2.93)
- cpu, ASAP (no roll-ups): (490, 8.05), (658, 6.64), (765, 5.89), (989, 5.04), (1.19e+03, 4.98), (1.48e+03, 4.71), (2.68e+03, 3.29), (1.34e+04, 3.26)
- cpu, PerQuery-CostAware: (490, 16.4), (575, 14.2), (673, 12.9), (765, 12.2), (959, 11.5), (989, 11.1), (1.14e+03, 10.8), (1.34e+03, 10.2), (1.49e+03, 9.8), (2.29e+03, 9.31), (2.68e+03, 8.95), (3.33e+03, 8.74)
- fargate, ASAP: (490, 0.358), (557, 0.31), (592, 0.307), (765, 0.277), (896, 0.246), (989, 0.243), (1.19e+03, 0.24), (1.48e+03, 0.234), (2.29e+03, 0.206), (2.68e+03, 0.177)
- fargate, ASAP (no roll-ups): (490, 0.386), (557, 0.331), (592, 0.329), (765, 0.298), (896, 0.268), (989, 0.263), (1.19e+03, 0.26), (1.48e+03, 0.254), (2.29e+03, 0.226), (2.68e+03, 0.197)
- fargate, PerQuery-CostAware: (490, 0.844), (575, 0.759), (673, 0.703), (765, 0.674), (896, 0.65), (989, 0.635), (1.19e+03, 0.613), (1.49e+03, 0.584), (1.98e+03, 0.578), (2.29e+03, 0.564), (2.97e+03, 0.545)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 12 | 33 | 2.93 | 3.26 | 10.1% |
| fargate | 52 | 12 | 33 | 0.177 | 0.197 | 10.1% |

## mixed + multi-grouping, m = 8

832 RQEs on 72 streams; dropped: 0; sanity violations: 0. AutoSketch: 1904 probes, 120 distinct (metric, config); measured benchmark 3.17e+03 s at N = 1e+08 (paper rate 7.2e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 23.5 | 23.5 | 142 | 1.34e+04 | 46.7 | 8.23 | 37.6 |
| cpu | ASAP (no roll-ups) | 26.1 | 26.1 | 154 | 1.34e+04 | 39 | 8.35 | 40.3 |
| cpu | PerQuery-CostAware | 69.9 | 69.9 | 346 | 3.33e+03 | 59.1 | 16.4 | 89 |
| cpu | AutoSketch-Adapted | 2.17e+04 | 2.17e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 3.17e+03 | 2.17e+04 |
| fargate | ASAP | 1.41 | 23.7 | 102 | 2.68e+03 | 46.7 | 9.29 | 1.94 |
| fargate | ASAP (no roll-ups) | 1.57 | 26.4 | 113 | 2.68e+03 | 39 | 8.62 | 2.1 |
| fargate | PerQuery-CostAware | 4.36 | 70.8 | 336 | 2.97e+03 | 59.1 | 15.1 | 5.08 |
| fargate | AutoSketch-Adapted | 1.02e+03 | 2.17e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 3.17e+03 | 1.02e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 60.4), (658, 50.2), (765, 44.2), (989, 37.6), (1.19e+03, 37.2), (1.48e+03, 35), (2.68e+03, 23.7), (1.34e+04, 23.5)
- cpu, ASAP (no roll-ups): (490, 64.4), (658, 53.1), (765, 47.1), (989, 40.3), (1.19e+03, 39.8), (1.48e+03, 37.7), (2.68e+03, 26.4), (1.34e+04, 26.1)
- cpu, PerQuery-CostAware: (490, 131), (575, 114), (673, 103), (765, 97.2), (959, 91.9), (989, 89), (1.14e+03, 86.2), (1.34e+03, 81.8), (1.49e+03, 78.4), (2.29e+03, 74.5), (2.68e+03, 71.6), (3.33e+03, 69.9)
- fargate, ASAP: (490, 2.87), (557, 2.48), (592, 2.46), (765, 2.21), (896, 1.97), (989, 1.94), (1.19e+03, 1.92), (1.48e+03, 1.87), (2.29e+03, 1.65), (2.68e+03, 1.41)
- fargate, ASAP (no roll-ups): (490, 3.09), (557, 2.65), (592, 2.63), (765, 2.39), (896, 2.14), (989, 2.1), (1.19e+03, 2.08), (1.48e+03, 2.03), (2.29e+03, 1.81), (2.68e+03, 1.57)
- fargate, PerQuery-CostAware: (490, 6.75), (575, 6.07), (673, 5.62), (765, 5.39), (896, 5.2), (989, 5.08), (1.19e+03, 4.91), (1.49e+03, 4.67), (1.98e+03, 4.63), (2.29e+03, 4.51), (2.97e+03, 4.36)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 96 | 264 | 23.5 | 26.1 | 10.1% |
| fargate | 416 | 96 | 264 | 1.41 | 1.57 | 10.1% |

## mixed + multi-grouping, m = 16

1664 RQEs on 144 streams; dropped: 0; sanity violations: 0. AutoSketch: 3808 probes, 240 distinct (metric, config); measured benchmark 6.34e+03 s at N = 1e+08 (paper rate 1.44e+04 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 46.9 | 46.9 | 284 | 1.34e+04 | 46.7 | 17.8 | 75.3 |
| cpu | ASAP (no roll-ups) | 52.2 | 52.2 | 307 | 1.34e+04 | 39 | 17.3 | 80.6 |
| cpu | PerQuery-CostAware | 140 | 140 | 693 | 3.33e+03 | 59.1 | 33 | 178 |
| cpu | AutoSketch-Adapted | 4.34e+04 | 4.34e+04 | 6.41e+04 | 1.01e+03 | 9.45 | 6.34e+03 | 4.34e+04 |
| fargate | ASAP | 2.83 | 47.4 | 204 | 2.68e+03 | 46.7 | 27 | 3.89 |
| fargate | ASAP (no roll-ups) | 3.14 | 52.7 | 227 | 2.68e+03 | 39 | 23 | 4.21 |
| fargate | PerQuery-CostAware | 8.73 | 142 | 672 | 2.97e+03 | 59.1 | 37.4 | 10.2 |
| fargate | AutoSketch-Adapted | 2.04e+03 | 4.34e+04 | 6.41e+04 | 1.01e+03 | 9.45 | 6.34e+03 | 2.04e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 121), (658, 100), (765, 88.4), (989, 75.3), (1.19e+03, 74.4), (1.48e+03, 70.1), (2.68e+03, 47.4), (1.34e+04, 46.9)
- cpu, ASAP (no roll-ups): (490, 129), (658, 106), (765, 94.2), (989, 80.6), (1.19e+03, 79.7), (1.48e+03, 75.4), (2.68e+03, 52.7), (1.34e+04, 52.2)
- cpu, PerQuery-CostAware: (490, 262), (575, 228), (673, 206), (765, 194), (959, 184), (989, 178), (1.14e+03, 172), (1.34e+03, 164), (1.49e+03, 157), (2.29e+03, 149), (2.68e+03, 143), (3.33e+03, 140)
- fargate, ASAP: (490, 5.73), (557, 4.96), (592, 4.91), (765, 4.43), (896, 3.94), (989, 3.89), (1.19e+03, 3.85), (1.48e+03, 3.75), (2.29e+03, 3.29), (2.68e+03, 2.83)
- fargate, ASAP (no roll-ups): (490, 6.18), (557, 5.3), (592, 5.26), (765, 4.77), (896, 4.29), (989, 4.21), (1.19e+03, 4.17), (1.48e+03, 4.06), (2.29e+03, 3.61), (2.68e+03, 3.14)
- fargate, PerQuery-CostAware: (490, 13.5), (575, 12.1), (673, 11.2), (765, 10.8), (896, 10.4), (989, 10.2), (1.19e+03, 9.82), (1.49e+03, 9.35), (1.98e+03, 9.25), (2.29e+03, 9.03), (2.97e+03, 8.73)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 192 | 528 | 46.9 | 52.2 | 10.1% |
| fargate | 832 | 192 | 528 | 2.83 | 3.14 | 10.1% |

## mixed + multi-grouping, r = 8

200 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 454 probes, 15 distinct (metric, config); measured benchmark 396 s at N = 1e+08 (paper rate 900 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.04 | 3.04 | 12.7 | 2.68e+03 | 46.7 | 1.98 | 4.76 |
| cpu | ASAP (no roll-ups) | 3.37 | 3.37 | 14.2 | 2.68e+03 | 39 | 1.94 | 5.09 |
| cpu | PerQuery-CostAware | 15.5 | 15.5 | 51.5 | 3.33e+03 | 34 | 3.57 | 18.1 |
| cpu | AutoSketch-Adapted | 3.25e+03 | 3.25e+03 | 4.17e+03 | 1.01e+03 | 5.33 | 396 | 3.25e+03 |
| fargate | ASAP | 0.18 | 3.04 | 12.7 | 2.68e+03 | 46.7 | 2.15 | 0.245 |
| fargate | ASAP (no roll-ups) | 0.2 | 3.37 | 14.2 | 2.68e+03 | 39 | 2.02 | 0.265 |
| fargate | PerQuery-CostAware | 0.856 | 15.6 | 50.2 | 2.97e+03 | 34 | 3.58 | 0.953 |
| fargate | AutoSketch-Adapted | 150 | 3.25e+03 | 4.17e+03 | 1.01e+03 | 5.33 | 396 | 150 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.59), (557, 6.39), (658, 6.32), (765, 5.57), (896, 4.82), (989, 4.76), (1.19e+03, 4.7), (1.48e+03, 4.44), (2.29e+03, 3.75), (2.68e+03, 3.04)
- cpu, ASAP (no roll-ups): (490, 8.08), (557, 6.76), (658, 6.68), (765, 5.93), (896, 5.18), (989, 5.09), (1.19e+03, 5.03), (1.48e+03, 4.77), (2.29e+03, 4.08), (2.68e+03, 3.37)
- cpu, PerQuery-CostAware: (490, 24.1), (575, 21.4), (673, 19.8), (765, 19.1), (959, 18.4), (989, 18.1), (1.14e+03, 17.7), (1.34e+03, 17), (1.49e+03, 16.6), (2.29e+03, 16.1), (2.68e+03, 15.7), (3.33e+03, 15.5)
- fargate, ASAP: (490, 0.36), (557, 0.311), (592, 0.309), (765, 0.279), (896, 0.248), (989, 0.245), (1.19e+03, 0.243), (1.48e+03, 0.237), (2.29e+03, 0.208), (2.68e+03, 0.18)
- fargate, ASAP (no roll-ups): (490, 0.387), (557, 0.333), (592, 0.33), (765, 0.3), (896, 0.27), (989, 0.265), (1.19e+03, 0.263), (1.48e+03, 0.256), (2.29e+03, 0.228), (2.68e+03, 0.2)
- fargate, PerQuery-CostAware: (490, 1.19), (575, 1.08), (673, 1.02), (765, 0.992), (896, 0.968), (989, 0.953), (1.19e+03, 0.924), (1.49e+03, 0.895), (1.98e+03, 0.889), (2.29e+03, 0.875), (2.97e+03, 0.856)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 12 | 33 | 3.04 | 3.37 | 9.8% |
| fargate | 104 | 12 | 33 | 0.18 | 0.2 | 10.0% |

## mixed

42 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 100 probes, 8 distinct (metric, config); measured benchmark 169 s at N = 1e+08 (paper rate 480 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.71 | 2.71 | 16.7 | 1.34e+04 | 46.7 | 0.823 | 4.49 |
| cpu | ASAP (no roll-ups) | 2.71 | 2.71 | 16.7 | 1.34e+04 | 46.7 | 0.822 | 4.49 |
| cpu | PerQuery-CostAware | 7.67 | 7.67 | 39.8 | 3.33e+03 | 245 | 1.82 | 10.1 |
| cpu | AutoSketch-Adapted | 2.7e+03 | 2.7e+03 | 4e+03 | 1.01e+03 | 9.45 | 169 | 2.7e+03 |
| fargate | ASAP | 0.163 | 2.74 | 11.7 | 2.68e+03 | 46.7 | 0.831 | 0.23 |
| fargate | ASAP (no roll-ups) | 0.163 | 2.74 | 11.7 | 2.68e+03 | 46.7 | 0.831 | 0.23 |
| fargate | PerQuery-CostAware | 0.487 | 7.78 | 38.5 | 2.97e+03 | 245 | 1.83 | 0.576 |
| fargate | AutoSketch-Adapted | 127 | 2.7e+03 | 4e+03 | 1.01e+03 | 9.45 | 169 | 127 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.1), (133, 21.7), (206, 14.1), (342, 9.91), (539, 6.13), (765, 5.3), (989, 4.49), (1.19e+03, 4.43), (1.48e+03, 4.16), (2.68e+03, 2.74), (1.34e+04, 2.71)
- cpu, ASAP (no roll-ups): (87.7, 44.1), (133, 21.7), (206, 14.1), (342, 9.91), (539, 6.13), (765, 5.3), (989, 4.49), (1.19e+03, 4.43), (1.48e+03, 4.16), (2.68e+03, 2.74), (1.34e+04, 2.71)
- cpu, PerQuery-CostAware: (87.7, 69.2), (121, 44.8), (167, 30.7), (235, 23.9), (328, 18.7), (452, 15.4), (592, 12.7), (829, 10.8), (989, 10.1), (1.19e+03, 9.53), (1.49e+03, 8.74), (2.29e+03, 8.24), (3.33e+03, 7.67)
- fargate, ASAP: (87.7, 1.85), (112, 1.11), (163, 0.732), (206, 0.616), (295, 0.48), (394, 0.4), (539, 0.296), (765, 0.263), (989, 0.23), (1.19e+03, 0.227), (1.48e+03, 0.221), (2.68e+03, 0.163)
- fargate, ASAP (no roll-ups): (87.7, 1.85), (112, 1.11), (163, 0.732), (206, 0.616), (295, 0.48), (394, 0.4), (539, 0.296), (765, 0.263), (989, 0.23), (1.19e+03, 0.227), (1.48e+03, 0.221), (2.68e+03, 0.163)
- fargate, PerQuery-CostAware: (87.7, 2.99), (118, 2.02), (163, 1.44), (227, 1.21), (305, 0.936), (394, 0.82), (592, 0.681), (765, 0.614), (989, 0.576), (1.49e+03, 0.525), (1.98e+03, 0.519), (2.97e+03, 0.487)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 7 | 7 | 2.71 | 2.71 | 0.0% |
| fargate | 0 | 7 | 7 | 0.163 | 0.163 | 0.0% |

## mixed, m = 8

336 RQEs on 48 streams; dropped: 0; sanity violations: 0. AutoSketch: 800 probes, 64 distinct (metric, config); measured benchmark 1.36e+03 s at N = 1e+08 (paper rate 3.84e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 21.7 | 21.7 | 134 | 1.34e+04 | 46.7 | 6.73 | 35.9 |
| cpu | ASAP (no roll-ups) | 21.7 | 21.7 | 134 | 1.34e+04 | 46.7 | 6.73 | 35.9 |
| cpu | PerQuery-CostAware | 61.3 | 61.3 | 318 | 3.33e+03 | 245 | 14.4 | 80.5 |
| cpu | AutoSketch-Adapted | 2.16e+04 | 2.16e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 1.36e+03 | 2.16e+04 |
| fargate | ASAP | 1.31 | 22 | 93.7 | 2.68e+03 | 46.7 | 6.83 | 1.84 |
| fargate | ASAP (no roll-ups) | 1.31 | 22 | 93.7 | 2.68e+03 | 46.7 | 6.83 | 1.84 |
| fargate | PerQuery-CostAware | 3.89 | 62.3 | 308 | 2.97e+03 | 245 | 14.4 | 4.61 |
| fargate | AutoSketch-Adapted | 1.02e+03 | 2.16e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 1.36e+03 | 1.02e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 353), (133, 173), (206, 112), (342, 79.3), (539, 49.1), (765, 42.4), (989, 35.9), (1.19e+03, 35.4), (1.48e+03, 33.3), (2.68e+03, 22), (1.34e+04, 21.7)
- cpu, ASAP (no roll-ups): (87.7, 353), (133, 173), (206, 112), (342, 79.3), (539, 49.1), (765, 42.4), (989, 35.9), (1.19e+03, 35.4), (1.48e+03, 33.3), (2.68e+03, 22), (1.34e+04, 21.7)
- cpu, PerQuery-CostAware: (87.7, 553), (121, 358), (167, 246), (235, 191), (328, 149), (452, 123), (592, 102), (829, 86.3), (989, 80.5), (1.19e+03, 76.2), (1.49e+03, 69.9), (2.29e+03, 65.9), (3.33e+03, 61.3)
- fargate, ASAP: (87.7, 14.8), (112, 8.88), (163, 5.86), (206, 4.93), (295, 3.84), (394, 3.2), (539, 2.37), (765, 2.11), (989, 1.84), (1.19e+03, 1.82), (1.48e+03, 1.77), (2.68e+03, 1.31)
- fargate, ASAP (no roll-ups): (87.7, 14.8), (112, 8.88), (163, 5.86), (206, 4.93), (295, 3.84), (394, 3.2), (539, 2.37), (765, 2.11), (989, 1.84), (1.19e+03, 1.82), (1.48e+03, 1.77), (2.68e+03, 1.31)
- fargate, PerQuery-CostAware: (87.7, 23.9), (118, 16.1), (163, 11.5), (227, 9.68), (305, 7.48), (394, 6.56), (592, 5.45), (765, 4.91), (989, 4.61), (1.49e+03, 4.2), (1.98e+03, 4.15), (2.97e+03, 3.89)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 56 | 56 | 21.7 | 21.7 | 0.0% |
| fargate | 0 | 56 | 56 | 1.31 | 1.31 | 0.0% |

## mixed, m = 16

672 RQEs on 96 streams; dropped: 0; sanity violations: 0. AutoSketch: 1600 probes, 128 distinct (metric, config); measured benchmark 2.71e+03 s at N = 1e+08 (paper rate 7.68e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 43.4 | 43.4 | 268 | 1.34e+04 | 46.7 | 13.3 | 71.8 |
| cpu | ASAP (no roll-ups) | 43.4 | 43.4 | 268 | 1.34e+04 | 46.7 | 13.3 | 71.8 |
| cpu | PerQuery-CostAware | 123 | 123 | 636 | 3.33e+03 | 245 | 28.4 | 161 |
| cpu | AutoSketch-Adapted | 4.32e+04 | 4.32e+04 | 6.4e+04 | 1.01e+03 | 9.45 | 2.71e+03 | 4.32e+04 |
| fargate | ASAP | 2.61 | 43.9 | 187 | 2.68e+03 | 46.7 | 13.6 | 3.67 |
| fargate | ASAP (no roll-ups) | 2.61 | 43.9 | 187 | 2.68e+03 | 46.7 | 13.6 | 3.67 |
| fargate | PerQuery-CostAware | 7.78 | 125 | 616 | 2.97e+03 | 245 | 28.4 | 9.22 |
| fargate | AutoSketch-Adapted | 2.04e+03 | 4.32e+04 | 6.4e+04 | 1.01e+03 | 9.45 | 2.71e+03 | 2.04e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 706), (133, 347), (206, 225), (342, 159), (539, 98.1), (765, 84.8), (989, 71.8), (1.19e+03, 70.9), (1.48e+03, 66.6), (2.68e+03, 43.9), (1.34e+04, 43.4)
- cpu, ASAP (no roll-ups): (87.7, 706), (133, 347), (206, 225), (342, 159), (539, 98.1), (765, 84.8), (989, 71.8), (1.19e+03, 70.9), (1.48e+03, 66.6), (2.68e+03, 43.9), (1.34e+04, 43.4)
- cpu, PerQuery-CostAware: (87.7, 1.11e+03), (121, 717), (167, 491), (235, 382), (328, 299), (452, 247), (592, 203), (829, 173), (989, 161), (1.19e+03, 152), (1.49e+03, 140), (2.29e+03, 132), (3.33e+03, 123)
- fargate, ASAP: (87.7, 29.6), (112, 17.8), (163, 11.7), (206, 9.86), (295, 7.69), (394, 6.41), (539, 4.74), (765, 4.21), (989, 3.67), (1.19e+03, 3.63), (1.48e+03, 3.53), (2.68e+03, 2.61)
- fargate, ASAP (no roll-ups): (87.7, 29.6), (112, 17.8), (163, 11.7), (206, 9.86), (295, 7.69), (394, 6.41), (539, 4.74), (765, 4.21), (989, 3.67), (1.19e+03, 3.63), (1.48e+03, 3.53), (2.68e+03, 2.61)
- fargate, PerQuery-CostAware: (87.7, 47.9), (118, 32.3), (163, 23.1), (227, 19.4), (305, 15), (394, 13.1), (592, 10.9), (765, 9.83), (989, 9.22), (1.49e+03, 8.4), (1.98e+03, 8.31), (2.97e+03, 7.78)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 112 | 112 | 43.4 | 43.4 | 0.0% |
| fargate | 0 | 112 | 112 | 2.61 | 2.61 | 0.0% |

## mixed, r = 8

76 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 178 probes, 8 distinct (metric, config); measured benchmark 169 s at N = 1e+08 (paper rate 480 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.8 | 2.8 | 11.7 | 2.68e+03 | 58.6 | 1.53 | 4.52 |
| cpu | ASAP (no roll-ups) | 2.8 | 2.8 | 11.7 | 2.68e+03 | 58.6 | 1.53 | 4.52 |
| cpu | PerQuery-CostAware | 13.5 | 13.5 | 47 | 3.33e+03 | 59.1 | 3.36 | 16 |
| cpu | AutoSketch-Adapted | 3.24e+03 | 3.24e+03 | 4.16e+03 | 1.01e+03 | 9.45 | 169 | 3.24e+03 |
| fargate | ASAP | 0.165 | 2.8 | 11.7 | 2.68e+03 | 58.6 | 1.55 | 0.231 |
| fargate | ASAP (no roll-ups) | 0.165 | 2.8 | 11.7 | 2.68e+03 | 58.6 | 1.55 | 0.231 |
| fargate | PerQuery-CostAware | 0.754 | 13.6 | 45.8 | 2.97e+03 | 59.1 | 3.36 | 0.851 |
| fargate | AutoSketch-Adapted | 150 | 3.24e+03 | 4.16e+03 | 1.01e+03 | 9.45 | 169 | 150 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.1), (112, 26.2), (163, 16.9), (206, 14.1), (295, 10.7), (394, 8.73), (539, 6.15), (765, 5.33), (989, 4.52), (1.19e+03, 4.46), (1.48e+03, 4.2), (2.68e+03, 2.8)
- cpu, ASAP (no roll-ups): (87.7, 44.1), (112, 26.2), (163, 16.9), (206, 14.1), (295, 10.7), (394, 8.73), (539, 6.15), (765, 5.33), (989, 4.52), (1.19e+03, 4.46), (1.48e+03, 4.2), (2.68e+03, 2.8)
- cpu, PerQuery-CostAware: (87.7, 83.8), (121, 56.6), (167, 40), (235, 32), (328, 25.9), (452, 22.5), (592, 18.7), (829, 16.8), (989, 16), (1.19e+03, 15.3), (1.49e+03, 14.5), (2.29e+03, 14), (3.33e+03, 13.5)
- fargate, ASAP: (87.7, 1.85), (112, 1.11), (163, 0.733), (206, 0.616), (295, 0.481), (394, 0.401), (539, 0.297), (765, 0.264), (989, 0.231), (1.19e+03, 0.228), (1.48e+03, 0.222), (2.68e+03, 0.165)
- fargate, ASAP (no roll-ups): (87.7, 1.85), (112, 1.11), (163, 0.733), (206, 0.616), (295, 0.481), (394, 0.401), (539, 0.297), (765, 0.264), (989, 0.231), (1.19e+03, 0.228), (1.48e+03, 0.222), (2.68e+03, 0.165)
- fargate, PerQuery-CostAware: (87.7, 3.62), (118, 2.53), (163, 1.85), (227, 1.59), (305, 1.26), (394, 1.14), (592, 0.955), (765, 0.889), (989, 0.851), (1.49e+03, 0.793), (1.98e+03, 0.787), (2.97e+03, 0.754)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 7 | 7 | 2.8 | 2.8 | 0.0% |
| fargate | 0 | 7 | 7 | 0.165 | 0.165 | 0.0% |

## multi-grouping

62 RQEs on 3 streams; dropped: 0; sanity violations: 0. AutoSketch: 138 probes, 7 distinct (metric, config); measured benchmark 227 s at N = 1e+08 (paper rate 420 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 0.22 | 0.22 | 1.01 | 557 | 24.6 | 0.523 | 0.323 |
| cpu | ASAP (no roll-ups) | 0.55 | 0.55 | 2.47 | 959 | 14.8 | 0.499 | 0.822 |
| cpu | PerQuery-CostAware | 1.07 | 1.07 | 3.51 | 959 | 14.8 | 0.281 | 1.32 |
| cpu | AutoSketch-Adapted | 9.65 | 9.65 | 3.51 | 490 | 4.9 | 227 | 9.65 |
| fargate | ASAP | 0.0134 | 0.22 | 1.01 | 557 | 24.6 | 0.553 | 0.0185 |
| fargate | ASAP (no roll-ups) | 0.0333 | 0.55 | 2.47 | 959 | 14.8 | 0.507 | 0.0463 |
| fargate | PerQuery-CostAware | 0.0589 | 1.07 | 3.51 | 959 | 14.8 | 0.283 | 0.0693 |
| fargate | AutoSketch-Adapted | 0.406 | 9.65 | 3.51 | 490 | 4.9 | 227 | 0.406 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 0.323), (557, 0.22)
- cpu, ASAP (no roll-ups): (490, 0.822), (557, 0.594), (624, 0.585), (959, 0.55)
- cpu, PerQuery-CostAware: (490, 1.32), (557, 1.17), (624, 1.09), (959, 1.07)
- fargate, ASAP: (490, 0.0185), (557, 0.0134)
- fargate, ASAP (no roll-ups): (490, 0.0463), (557, 0.035), (959, 0.0333)
- fargate, PerQuery-CostAware: (490, 0.0693), (557, 0.0631), (624, 0.0597), (959, 0.0589)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 5 | 26 | 0.22 | 0.55 | 60.0% |
| fargate | 52 | 5 | 26 | 0.0134 | 0.0333 | 59.7% |

## multi-grouping, m = 8

496 RQEs on 24 streams; dropped: 0; sanity violations: 0. AutoSketch: 1104 probes, 56 distinct (metric, config); measured benchmark 1.81e+03 s at N = 1e+08 (paper rate 3.36e+03 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 1.76 | 1.76 | 8.12 | 557 | 24.6 | 3.69 | 2.59 |
| cpu | ASAP (no roll-ups) | 4.4 | 4.4 | 19.8 | 959 | 14.8 | 3.48 | 6.58 |
| cpu | PerQuery-CostAware | 8.54 | 8.54 | 28.1 | 959 | 14.8 | 2.15 | 10.6 |
| cpu | AutoSketch-Adapted | 77.2 | 77.2 | 28.1 | 490 | 4.9 | 1.81e+03 | 77.2 |
| fargate | ASAP | 0.107 | 1.76 | 8.12 | 557 | 24.6 | 4.37 | 0.148 |
| fargate | ASAP (no roll-ups) | 0.266 | 4.4 | 19.8 | 959 | 14.8 | 3.54 | 0.371 |
| fargate | PerQuery-CostAware | 0.471 | 8.54 | 28.1 | 959 | 14.8 | 2.16 | 0.554 |
| fargate | AutoSketch-Adapted | 3.25 | 77.2 | 28.1 | 490 | 4.9 | 1.81e+03 | 3.25 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 2.59), (557, 1.76)
- cpu, ASAP (no roll-ups): (490, 6.58), (557, 4.75), (624, 4.68), (959, 4.4)
- cpu, PerQuery-CostAware: (490, 10.6), (557, 9.38), (624, 8.7), (959, 8.54)
- fargate, ASAP: (490, 0.148), (557, 0.107)
- fargate, ASAP (no roll-ups): (490, 0.371), (557, 0.28), (959, 0.266)
- fargate, PerQuery-CostAware: (490, 0.554), (557, 0.505), (624, 0.477), (959, 0.471)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 40 | 208 | 1.76 | 4.4 | 60.0% |
| fargate | 416 | 40 | 208 | 0.107 | 0.266 | 59.7% |

## multi-grouping, m = 16

992 RQEs on 48 streams; dropped: 0; sanity violations: 0. AutoSketch: 2208 probes, 112 distinct (metric, config); measured benchmark 3.62e+03 s at N = 1e+08 (paper rate 6.72e+03 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.52 | 3.52 | 16.2 | 557 | 24.6 | 8.52 | 5.17 |
| cpu | ASAP (no roll-ups) | 8.8 | 8.8 | 39.5 | 959 | 14.8 | 7.96 | 13.2 |
| cpu | PerQuery-CostAware | 17.1 | 17.1 | 56.2 | 959 | 14.8 | 4.96 | 21.2 |
| cpu | AutoSketch-Adapted | 154 | 154 | 56.1 | 490 | 4.9 | 3.62e+03 | 154 |
| fargate | ASAP | 0.215 | 3.52 | 16.2 | 557 | 24.6 | 9.26 | 0.296 |
| fargate | ASAP (no roll-ups) | 0.532 | 8.8 | 39.5 | 959 | 14.8 | 8.23 | 0.741 |
| fargate | PerQuery-CostAware | 0.942 | 17.1 | 56.2 | 959 | 14.8 | 4.98 | 1.11 |
| fargate | AutoSketch-Adapted | 6.5 | 154 | 56.1 | 490 | 4.9 | 3.62e+03 | 6.5 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 5.17), (557, 3.52)
- cpu, ASAP (no roll-ups): (490, 13.2), (557, 9.5), (624, 9.36), (959, 8.8)
- cpu, PerQuery-CostAware: (490, 21.2), (557, 18.8), (624, 17.4), (959, 17.1)
- fargate, ASAP: (490, 0.296), (557, 0.215)
- fargate, ASAP (no roll-ups): (490, 0.741), (557, 0.561), (959, 0.532)
- fargate, PerQuery-CostAware: (490, 1.11), (557, 1.01), (624, 0.955), (959, 0.942)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 80 | 416 | 3.52 | 8.8 | 60.0% |
| fargate | 832 | 80 | 416 | 0.215 | 0.532 | 59.7% |

## multi-grouping, r = 8

124 RQEs on 3 streams; dropped: 0; sanity violations: 0. AutoSketch: 276 probes, 7 distinct (metric, config); measured benchmark 227 s at N = 1e+08 (paper rate 420 s). AutoSketch latency 490 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 0.241 | 0.241 | 1.01 | 557 | 24.6 | 1.22 | 0.343 |
| cpu | ASAP (no roll-ups) | 0.572 | 0.572 | 2.47 | 959 | 14.8 | 1.17 | 0.84 |
| cpu | PerQuery-CostAware | 2.04 | 2.04 | 4.47 | 959 | 14.8 | 0.575 | 2.33 |
| cpu | AutoSketch-Adapted | 11.6 | 11.6 | 4.46 | 490 | 4.9 | 227 | 11.6 |
| fargate | ASAP | 0.0143 | 0.241 | 1.01 | 557 | 24.6 | 1.32 | 0.0193 |
| fargate | ASAP (no roll-ups) | 0.0342 | 0.572 | 2.47 | 959 | 14.8 | 1.18 | 0.0471 |
| fargate | PerQuery-CostAware | 0.103 | 2.04 | 4.47 | 959 | 14.8 | 0.576 | 0.114 |
| fargate | AutoSketch-Adapted | 0.489 | 11.6 | 4.46 | 490 | 4.9 | 227 | 0.489 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 0.343), (557, 0.241)
- cpu, ASAP (no roll-ups): (490, 0.84), (557, 0.612), (624, 0.605), (959, 0.572)
- cpu, PerQuery-CostAware: (490, 2.33), (557, 2.15), (624, 2.06), (959, 2.04)
- fargate, ASAP: (490, 0.0193), (557, 0.0143)
- fargate, ASAP (no roll-ups): (490, 0.0471), (557, 0.0358), (959, 0.0342)
- fargate, PerQuery-CostAware: (490, 0.114), (557, 0.107), (624, 0.103), (959, 0.103)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 5 | 26 | 0.241 | 0.572 | 57.9% |
| fargate | 104 | 5 | 26 | 0.0143 | 0.0342 | 58.2% |
