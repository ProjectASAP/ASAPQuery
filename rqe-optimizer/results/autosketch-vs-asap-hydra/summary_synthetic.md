# Synthetic workload: AutoSketch vs. ASAP, cost by use

Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; Fargate prices, in $/hour). Latency: a plan's query latency (its longest chain, compaction then query, CPU elastic), and the median over its RQEs. AutoSketch's planning time is its search plus its measured benchmark.

## mixed + multi-grouping

112 RQEs on 11 streams; dropped: 0; sanity violations: 0. AutoSketch: 332 probes, 19 distinct (metric, config); measured benchmark 481 s at N = 1e+08 (paper rate 1.14e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.76 | 3.76 | 30 | 1.34e+04 | 51.1 | 1.12 | 5.9 |
| cpu | ASAP (no roll-ups) | 4.09 | 4.09 | 31.5 | 1.34e+04 | 39 | 1.09 | 6.23 |
| cpu | PerQuery-CostAware | 11.6 | 11.6 | 80.7 | 3.33e+03 | 39 | 0.875 | 14.5 |
| cpu | AutoSketch-Adapted | 3.73e+03 | 3.73e+03 | 5.63e+03 | 1.01e+03 | 4.9 | 481 | 3.73e+03 |
| fargate | ASAP | 0.243 | 3.82 | 19.9 | 2.68e+03 | 51.1 | 1.23 | 0.321 |
| fargate | ASAP (no roll-ups) | 0.264 | 4.16 | 21.4 | 2.68e+03 | 39 | 1.17 | 0.341 |
| fargate | PerQuery-CostAware | 0.812 | 11.9 | 74.6 | 3.26e+03 | 39 | 0.886 | 0.923 |
| fargate | AutoSketch-Adapted | 176 | 3.73e+03 | 5.63e+03 | 1.01e+03 | 4.9 | 481 | 176 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 8.99), (649, 7.43), (813, 6.66), (980, 5.9), (1.08e+03, 5.83), (1.48e+03, 5.24), (2.68e+03, 3.82), (1.34e+04, 3.76)
- cpu, ASAP (no roll-ups): (490, 9.49), (649, 7.79), (813, 7.02), (980, 6.23), (1.08e+03, 6.16), (1.48e+03, 5.57), (2.68e+03, 4.15), (1.34e+04, 4.09)
- cpu, PerQuery-CostAware: (490, 20), (575, 17.8), (673, 16.5), (813, 15.6), (980, 14.5), (1.14e+03, 13.9), (1.34e+03, 13.4), (1.63e+03, 12.8), (1.96e+03, 12.7), (2.29e+03, 12.3), (2.68e+03, 11.9), (3.33e+03, 11.6)
- fargate, ASAP: (490, 0.443), (557, 0.384), (649, 0.381), (765, 0.352), (896, 0.321), (1.08e+03, 0.317), (1.3e+03, 0.314), (1.48e+03, 0.301), (2.29e+03, 0.272), (2.68e+03, 0.243)
- fargate, ASAP (no roll-ups): (490, 0.471), (557, 0.406), (649, 0.403), (765, 0.374), (896, 0.343), (959, 0.341), (1.08e+03, 0.337), (1.3e+03, 0.334), (1.48e+03, 0.321), (2.29e+03, 0.293), (2.68e+03, 0.264)
- fargate, PerQuery-CostAware: (490, 1.14), (575, 1.04), (673, 0.994), (813, 0.956), (959, 0.923), (1.14e+03, 0.893), (1.34e+03, 0.87), (1.63e+03, 0.852), (2.29e+03, 0.831), (2.68e+03, 0.817), (3.26e+03, 0.812)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 13 | 34 | 3.76 | 4.09 | 8.1% |
| fargate | 52 | 13 | 34 | 0.243 | 0.264 | 7.7% |

## mixed + multi-grouping, m = 8

896 RQEs on 88 streams; dropped: 0; sanity violations: 0. AutoSketch: 2656 probes, 152 distinct (metric, config); measured benchmark 3.85e+03 s at N = 1e+08 (paper rate 9.12e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 30.1 | 30.1 | 240 | 1.34e+04 | 51.1 | 10.4 | 47.2 |
| cpu | ASAP (no roll-ups) | 32.7 | 32.7 | 252 | 1.34e+04 | 39 | 9.73 | 49.8 |
| cpu | PerQuery-CostAware | 92.7 | 92.7 | 646 | 3.33e+03 | 39 | 7.84 | 116 |
| cpu | AutoSketch-Adapted | 2.98e+04 | 2.98e+04 | 4.5e+04 | 1.01e+03 | 4.9 | 3.85e+03 | 2.98e+04 |
| fargate | ASAP | 1.95 | 30.6 | 159 | 2.68e+03 | 51.1 | 13.2 | 2.57 |
| fargate | ASAP (no roll-ups) | 2.11 | 33.3 | 171 | 2.68e+03 | 39 | 10.7 | 2.73 |
| fargate | PerQuery-CostAware | 6.49 | 94.8 | 596 | 3.26e+03 | 39 | 7.93 | 7.39 |
| fargate | AutoSketch-Adapted | 1.41e+03 | 2.98e+04 | 4.5e+04 | 1.01e+03 | 4.9 | 3.85e+03 | 1.41e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 71.9), (649, 59.4), (813, 53.3), (980, 47.2), (1.08e+03, 46.7), (1.48e+03, 41.9), (2.68e+03, 30.6), (1.34e+04, 30.1)
- cpu, ASAP (no roll-ups): (490, 75.9), (649, 62.3), (813, 56.2), (980, 49.8), (1.08e+03, 49.3), (1.48e+03, 44.5), (2.68e+03, 33.2), (1.34e+04, 32.7)
- cpu, PerQuery-CostAware: (490, 160), (575, 143), (673, 132), (813, 125), (980, 116), (1.14e+03, 111), (1.34e+03, 107), (1.63e+03, 102), (1.96e+03, 102), (2.29e+03, 98.3), (2.68e+03, 95.2), (3.33e+03, 92.7)
- fargate, ASAP: (490, 3.54), (557, 3.07), (649, 3.05), (765, 2.81), (896, 2.57), (1.08e+03, 2.53), (1.3e+03, 2.51), (1.48e+03, 2.41), (2.29e+03, 2.18), (2.68e+03, 1.95)
- fargate, ASAP (no roll-ups): (490, 3.77), (557, 3.25), (649, 3.22), (765, 2.99), (896, 2.74), (959, 2.73), (1.08e+03, 2.69), (1.3e+03, 2.67), (1.48e+03, 2.57), (2.29e+03, 2.34), (2.68e+03, 2.11)
- fargate, PerQuery-CostAware: (490, 9.1), (575, 8.36), (673, 7.96), (813, 7.65), (959, 7.39), (1.14e+03, 7.14), (1.34e+03, 6.96), (1.63e+03, 6.81), (2.29e+03, 6.65), (2.68e+03, 6.53), (3.26e+03, 6.49)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 104 | 272 | 30.1 | 32.7 | 8.1% |
| fargate | 416 | 104 | 272 | 1.95 | 2.11 | 7.7% |

## mixed + multi-grouping, m = 16

1792 RQEs on 176 streams; dropped: 0; sanity violations: 0. AutoSketch: 5312 probes, 304 distinct (metric, config); measured benchmark 7.7e+03 s at N = 1e+08 (paper rate 1.82e+04 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 60.2 | 60.2 | 480 | 1.34e+04 | 51.1 | 22.5 | 94.3 |
| cpu | ASAP (no roll-ups) | 65.4 | 65.4 | 504 | 1.34e+04 | 39 | 21.1 | 99.6 |
| cpu | PerQuery-CostAware | 185 | 185 | 1.29e+03 | 3.33e+03 | 39 | 16.7 | 232 |
| cpu | AutoSketch-Adapted | 5.96e+04 | 5.96e+04 | 9e+04 | 1.01e+03 | 4.9 | 7.7e+03 | 5.96e+04 |
| fargate | ASAP | 3.89 | 61.2 | 318 | 2.68e+03 | 51.1 | 31.3 | 5.13 |
| fargate | ASAP (no roll-ups) | 4.22 | 66.5 | 342 | 2.68e+03 | 39 | 26.9 | 5.46 |
| fargate | PerQuery-CostAware | 13 | 190 | 1.19e+03 | 3.26e+03 | 39 | 17.2 | 14.8 |
| fargate | AutoSketch-Adapted | 2.81e+03 | 5.96e+04 | 9e+04 | 1.01e+03 | 4.9 | 7.7e+03 | 2.81e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 144), (649, 119), (813, 107), (980, 94.3), (1.08e+03, 93.3), (1.48e+03, 83.8), (2.68e+03, 61.1), (1.34e+04, 60.2)
- cpu, ASAP (no roll-ups): (490, 152), (649, 125), (813, 112), (980, 99.6), (1.08e+03, 98.6), (1.48e+03, 89.1), (2.68e+03, 66.4), (1.34e+04, 65.4)
- cpu, PerQuery-CostAware: (490, 320), (575, 285), (673, 265), (813, 250), (980, 232), (1.14e+03, 223), (1.34e+03, 214), (1.63e+03, 205), (1.96e+03, 204), (2.29e+03, 197), (2.68e+03, 190), (3.33e+03, 185)
- fargate, ASAP: (490, 7.09), (557, 6.14), (649, 6.1), (765, 5.63), (896, 5.13), (1.08e+03, 5.07), (1.3e+03, 5.02), (1.48e+03, 4.81), (2.29e+03, 4.36), (2.68e+03, 3.89)
- fargate, ASAP (no roll-ups): (490, 7.54), (557, 6.5), (649, 6.45), (765, 5.98), (896, 5.48), (959, 5.46), (1.08e+03, 5.39), (1.3e+03, 5.34), (1.48e+03, 5.14), (2.29e+03, 4.68), (2.68e+03, 4.22)
- fargate, PerQuery-CostAware: (490, 18.2), (575, 16.7), (673, 15.9), (813, 15.3), (959, 14.8), (1.14e+03, 14.3), (1.34e+03, 13.9), (1.63e+03, 13.6), (2.29e+03, 13.3), (2.68e+03, 13.1), (3.26e+03, 13)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 208 | 544 | 60.2 | 65.4 | 8.1% |
| fargate | 832 | 208 | 544 | 3.89 | 4.22 | 7.7% |

## mixed + multi-grouping, r = 8

216 RQEs on 11 streams; dropped: 0; sanity violations: 0. AutoSketch: 628 probes, 19 distinct (metric, config); measured benchmark 481 s at N = 1e+08 (paper rate 1.14e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.9 | 3.9 | 19.9 | 2.68e+03 | 51.1 | 2.68 | 5.95 |
| cpu | ASAP (no roll-ups) | 4.23 | 4.23 | 21.5 | 2.68e+03 | 39 | 2.57 | 6.28 |
| cpu | PerQuery-CostAware | 20.5 | 20.5 | 112 | 3.33e+03 | 30.6 | 1.73 | 23.7 |
| cpu | AutoSketch-Adapted | 4.47e+03 | 4.47e+03 | 5.86e+03 | 1.01e+03 | 4.9 | 481 | 4.47e+03 |
| fargate | ASAP | 0.247 | 3.91 | 19.9 | 2.68e+03 | 51.1 | 3.3 | 0.323 |
| fargate | ASAP (no roll-ups) | 0.267 | 4.24 | 21.4 | 2.68e+03 | 39 | 3.1 | 0.343 |
| fargate | PerQuery-CostAware | 1.24 | 20.9 | 88.8 | 3.26e+03 | 30.6 | 1.74 | 1.36 |
| fargate | AutoSketch-Adapted | 207 | 4.47e+03 | 5.86e+03 | 1.01e+03 | 4.9 | 481 | 207 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 9.03), (557, 7.54), (649, 7.47), (765, 6.74), (896, 5.96), (980, 5.95), (1.08e+03, 5.89), (1.3e+03, 5.83), (1.48e+03, 5.31), (2.29e+03, 4.61), (2.68e+03, 3.9)
- cpu, ASAP (no roll-ups): (490, 9.53), (557, 7.91), (649, 7.84), (765, 7.1), (896, 6.32), (980, 6.28), (1.08e+03, 6.22), (1.3e+03, 6.16), (1.48e+03, 5.64), (2.29e+03, 4.94), (2.68e+03, 4.23)
- cpu, PerQuery-CostAware: (490, 30.1), (575, 27.1), (673, 25.7), (813, 24.8), (980, 23.7), (1.14e+03, 23), (1.34e+03, 22.3), (1.63e+03, 21.8), (1.96e+03, 21.7), (2.29e+03, 21.2), (2.68e+03, 20.9), (3.33e+03, 20.5)
- fargate, ASAP: (490, 0.444), (557, 0.386), (649, 0.383), (765, 0.354), (896, 0.323), (1.08e+03, 0.319), (1.3e+03, 0.316), (1.48e+03, 0.304), (2.29e+03, 0.276), (2.68e+03, 0.247)
- fargate, ASAP (no roll-ups): (490, 0.472), (557, 0.408), (649, 0.405), (765, 0.376), (896, 0.345), (959, 0.343), (1.08e+03, 0.339), (1.3e+03, 0.336), (1.48e+03, 0.324), (2.29e+03, 0.296), (2.68e+03, 0.267)
- fargate, PerQuery-CostAware: (490, 1.61), (575, 1.49), (673, 1.43), (813, 1.39), (959, 1.36), (1.14e+03, 1.33), (1.34e+03, 1.3), (1.63e+03, 1.28), (2.29e+03, 1.26), (2.68e+03, 1.25), (3.26e+03, 1.24)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 13 | 34 | 3.9 | 4.23 | 7.8% |
| fargate | 104 | 13 | 34 | 0.247 | 0.267 | 7.6% |

## mixed

50 RQEs on 8 streams; dropped: 0; sanity violations: 0. AutoSketch: 166 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.54 | 3.54 | 28.9 | 1.34e+04 | 97.4 | 0.722 | 5.67 |
| cpu | ASAP (no roll-ups) | 3.54 | 3.54 | 28.9 | 1.34e+04 | 97.4 | 0.721 | 5.67 |
| cpu | PerQuery-CostAware | 10.5 | 10.5 | 77 | 3.33e+03 | 97.4 | 0.638 | 13.5 |
| cpu | AutoSketch-Adapted | 3.72e+03 | 3.72e+03 | 5.62e+03 | 1.01e+03 | 1.22 | 212 | 3.72e+03 |
| fargate | ASAP | 0.23 | 3.6 | 18.9 | 2.68e+03 | 51.1 | 0.779 | 0.307 |
| fargate | ASAP (no roll-ups) | 0.23 | 3.6 | 18.9 | 2.68e+03 | 51.1 | 0.779 | 0.307 |
| fargate | PerQuery-CostAware | 0.752 | 10.8 | 70.9 | 3.26e+03 | 97.4 | 0.646 | 0.864 |
| fargate | AutoSketch-Adapted | 176 | 3.72e+03 | 5.62e+03 | 1.01e+03 | 1.22 | 212 | 176 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 47.4), (138, 24.1), (206, 16.2), (342, 11.7), (541, 7.28), (813, 6.44), (980, 5.67), (1.3e+03, 5.55), (1.48e+03, 5.02), (2.68e+03, 3.6), (1.34e+04, 3.54)
- cpu, ASAP (no roll-ups): (87.7, 47.4), (138, 24.1), (206, 16.2), (342, 11.7), (541, 7.28), (813, 6.44), (980, 5.67), (1.3e+03, 5.55), (1.48e+03, 5.02), (2.68e+03, 3.6), (1.34e+04, 3.54)
- cpu, PerQuery-CostAware: (87.7, 78.3), (118, 51.5), (164, 36.6), (235, 28.4), (324, 23.3), (452, 19.2), (575, 16.7), (829, 14), (980, 13.5), (1.14e+03, 12.9), (1.63e+03, 11.7), (2.29e+03, 11.2), (3.33e+03, 10.5)
- fargate, ASAP: (87.7, 2.01), (115, 1.25), (163, 0.847), (219, 0.715), (291, 0.586), (386, 0.494), (541, 0.37), (765, 0.338), (896, 0.307), (1.3e+03, 0.3), (1.48e+03, 0.287), (2.68e+03, 0.23)
- fargate, ASAP (no roll-ups): (87.7, 2.01), (115, 1.25), (163, 0.847), (219, 0.715), (291, 0.586), (386, 0.494), (541, 0.37), (765, 0.338), (896, 0.307), (1.3e+03, 0.3), (1.48e+03, 0.287), (2.68e+03, 0.23)
- fargate, PerQuery-CostAware: (87.7, 3.52), (118, 2.4), (164, 1.79), (233, 1.49), (323, 1.24), (452, 1.08), (575, 0.981), (829, 0.879), (896, 0.864), (1.14e+03, 0.833), (1.63e+03, 0.792), (2.29e+03, 0.771), (3.26e+03, 0.752)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 8 | 8 | 3.54 | 3.54 | 0.0% |
| fargate | 0 | 8 | 8 | 0.23 | 0.23 | 0.0% |

## mixed, m = 8

400 RQEs on 64 streams; dropped: 0; sanity violations: 0. AutoSketch: 1328 probes, 80 distinct (metric, config); measured benchmark 1.7e+03 s at N = 1e+08 (paper rate 4.8e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 28.3 | 28.3 | 231 | 1.34e+04 | 97.4 | 6.34 | 45.4 |
| cpu | ASAP (no roll-ups) | 28.3 | 28.3 | 231 | 1.34e+04 | 97.4 | 6.35 | 45.4 |
| cpu | PerQuery-CostAware | 84.1 | 84.1 | 616 | 3.33e+03 | 97.4 | 5.44 | 108 |
| cpu | AutoSketch-Adapted | 2.97e+04 | 2.97e+04 | 4.5e+04 | 1.01e+03 | 1.22 | 1.7e+03 | 2.97e+04 |
| fargate | ASAP | 1.84 | 28.8 | 151 | 2.68e+03 | 51.1 | 7.45 | 2.46 |
| fargate | ASAP (no roll-ups) | 1.84 | 28.8 | 151 | 2.68e+03 | 51.1 | 7.46 | 2.46 |
| fargate | PerQuery-CostAware | 6.02 | 86.2 | 568 | 3.26e+03 | 97.4 | 5.52 | 6.91 |
| fargate | AutoSketch-Adapted | 1.4e+03 | 2.97e+04 | 4.5e+04 | 1.01e+03 | 1.22 | 1.7e+03 | 1.4e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 379), (138, 193), (206, 129), (342, 93.8), (541, 58.2), (813, 51.5), (980, 45.4), (1.3e+03, 44.4), (1.48e+03, 40.1), (2.68e+03, 28.8), (1.34e+04, 28.3)
- cpu, ASAP (no roll-ups): (87.7, 379), (138, 193), (206, 129), (342, 93.8), (541, 58.2), (813, 51.5), (980, 45.4), (1.3e+03, 44.4), (1.48e+03, 40.1), (2.68e+03, 28.8), (1.34e+04, 28.3)
- cpu, PerQuery-CostAware: (87.7, 626), (118, 412), (164, 293), (235, 227), (324, 186), (452, 154), (575, 133), (829, 112), (980, 108), (1.14e+03, 103), (1.63e+03, 93.8), (2.29e+03, 89.7), (3.33e+03, 84.1)
- fargate, ASAP: (87.7, 16.1), (115, 10), (163, 6.78), (219, 5.72), (291, 4.69), (386, 3.95), (541, 2.96), (765, 2.71), (896, 2.46), (1.3e+03, 2.4), (1.48e+03, 2.3), (2.68e+03, 1.84)
- fargate, ASAP (no roll-ups): (87.7, 16.1), (115, 10), (163, 6.78), (219, 5.72), (291, 4.69), (386, 3.95), (541, 2.96), (765, 2.71), (896, 2.46), (1.3e+03, 2.4), (1.48e+03, 2.3), (2.68e+03, 1.84)
- fargate, PerQuery-CostAware: (87.7, 28.1), (118, 19.2), (164, 14.3), (233, 11.9), (323, 9.95), (452, 8.66), (575, 7.85), (829, 7.03), (896, 6.91), (1.14e+03, 6.66), (1.63e+03, 6.34), (2.29e+03, 6.17), (3.26e+03, 6.02)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 64 | 64 | 28.3 | 28.3 | 0.0% |
| fargate | 0 | 64 | 64 | 1.84 | 1.84 | 0.0% |

## mixed, m = 16

800 RQEs on 128 streams; dropped: 0; sanity violations: 0. AutoSketch: 2656 probes, 160 distinct (metric, config); measured benchmark 3.39e+03 s at N = 1e+08 (paper rate 9.6e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 56.6 | 56.6 | 463 | 1.34e+04 | 97.4 | 13.7 | 90.8 |
| cpu | ASAP (no roll-ups) | 56.6 | 56.6 | 463 | 1.34e+04 | 97.4 | 13.7 | 90.8 |
| cpu | PerQuery-CostAware | 168 | 168 | 1.23e+03 | 3.33e+03 | 97.4 | 11.4 | 215 |
| cpu | AutoSketch-Adapted | 5.95e+04 | 5.95e+04 | 9e+04 | 1.01e+03 | 1.22 | 3.39e+03 | 5.95e+04 |
| fargate | ASAP | 3.68 | 57.6 | 302 | 2.68e+03 | 51.1 | 16.9 | 4.91 |
| fargate | ASAP (no roll-ups) | 3.68 | 57.6 | 302 | 2.68e+03 | 51.1 | 16.8 | 4.91 |
| fargate | PerQuery-CostAware | 12 | 172 | 1.14e+03 | 3.26e+03 | 97.4 | 11.7 | 13.8 |
| fargate | AutoSketch-Adapted | 2.81e+03 | 5.95e+04 | 9e+04 | 1.01e+03 | 1.22 | 3.39e+03 | 2.81e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 758), (138, 386), (206, 259), (342, 188), (541, 116), (813, 103), (980, 90.8), (1.3e+03, 88.8), (1.48e+03, 80.3), (2.68e+03, 57.6), (1.34e+04, 56.6)
- cpu, ASAP (no roll-ups): (87.7, 758), (138, 386), (206, 259), (342, 188), (541, 116), (813, 103), (980, 90.8), (1.3e+03, 88.8), (1.48e+03, 80.3), (2.68e+03, 57.6), (1.34e+04, 56.6)
- cpu, PerQuery-CostAware: (87.7, 1.25e+03), (118, 824), (164, 586), (235, 454), (324, 372), (452, 308), (575, 267), (829, 224), (980, 215), (1.14e+03, 206), (1.63e+03, 188), (2.29e+03, 179), (3.33e+03, 168)
- fargate, ASAP: (87.7, 32.2), (115, 20), (163, 13.6), (219, 11.4), (291, 9.38), (386, 7.91), (541, 5.93), (765, 5.41), (896, 4.91), (1.3e+03, 4.8), (1.48e+03, 4.59), (2.68e+03, 3.68)
- fargate, ASAP (no roll-ups): (87.7, 32.2), (115, 20), (163, 13.6), (219, 11.4), (291, 9.38), (386, 7.91), (541, 5.93), (765, 5.41), (896, 4.91), (1.3e+03, 4.8), (1.48e+03, 4.59), (2.68e+03, 3.68)
- fargate, PerQuery-CostAware: (87.7, 56.3), (118, 38.5), (164, 28.7), (233, 23.8), (323, 19.9), (452, 17.3), (575, 15.7), (829, 14.1), (896, 13.8), (1.14e+03, 13.3), (1.63e+03, 12.7), (2.29e+03, 12.3), (3.26e+03, 12)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 128 | 128 | 56.6 | 56.6 | 0.0% |
| fargate | 0 | 128 | 128 | 3.68 | 3.68 | 0.0% |

## mixed, r = 8

92 RQEs on 8 streams; dropped: 0; sanity violations: 0. AutoSketch: 296 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.66 | 3.66 | 18.9 | 2.68e+03 | 58.6 | 1.64 | 5.71 |
| cpu | ASAP (no roll-ups) | 3.66 | 3.66 | 18.9 | 2.68e+03 | 58.6 | 1.64 | 5.71 |
| cpu | PerQuery-CostAware | 18.5 | 18.5 | 107 | 3.33e+03 | 61.5 | 1.23 | 21.6 |
| cpu | AutoSketch-Adapted | 4.46e+03 | 4.46e+03 | 5.86e+03 | 1.01e+03 | 1.21 | 212 | 4.46e+03 |
| fargate | ASAP | 0.232 | 3.66 | 18.9 | 2.68e+03 | 58.6 | 2 | 0.308 |
| fargate | ASAP (no roll-ups) | 0.232 | 3.66 | 18.9 | 2.68e+03 | 58.6 | 2.02 | 0.308 |
| fargate | PerQuery-CostAware | 1.14 | 18.8 | 84.2 | 3.26e+03 | 51.1 | 1.24 | 1.26 |
| fargate | AutoSketch-Adapted | 207 | 4.46e+03 | 5.86e+03 | 1.01e+03 | 1.21 | 212 | 207 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 47.4), (115, 29), (163, 19.1), (219, 15.9), (291, 12.7), (389, 10.3), (541, 7.3), (765, 6.5), (980, 5.71), (1.3e+03, 5.59), (1.48e+03, 5.06), (2.68e+03, 3.66)
- cpu, ASAP (no roll-ups): (87.7, 47.4), (115, 29), (163, 19.1), (219, 15.9), (291, 12.7), (389, 10.3), (541, 7.3), (765, 6.5), (980, 5.71), (1.3e+03, 5.59), (1.48e+03, 5.06), (2.68e+03, 3.66)
- cpu, PerQuery-CostAware: (87.7, 95.7), (118, 66), (164, 48.4), (235, 38.9), (324, 32.9), (452, 28.7), (575, 25), (829, 22.2), (980, 21.6), (1.14e+03, 21), (1.63e+03, 19.7), (2.29e+03, 19.2), (3.33e+03, 18.5)
- fargate, ASAP: (87.7, 2.01), (115, 1.25), (163, 0.848), (219, 0.715), (291, 0.587), (386, 0.495), (541, 0.371), (765, 0.339), (896, 0.308), (1.3e+03, 0.301), (1.48e+03, 0.289), (2.68e+03, 0.232)
- fargate, ASAP (no roll-ups): (87.7, 2.01), (115, 1.25), (163, 0.848), (219, 0.715), (291, 0.587), (386, 0.495), (541, 0.371), (765, 0.339), (896, 0.308), (1.3e+03, 0.301), (1.48e+03, 0.289), (2.68e+03, 0.232)
- fargate, PerQuery-CostAware: (87.7, 4.28), (118, 3.05), (164, 2.33), (233, 1.97), (323, 1.69), (452, 1.52), (575, 1.38), (829, 1.27), (896, 1.26), (1.14e+03, 1.23), (1.63e+03, 1.18), (2.29e+03, 1.16), (3.26e+03, 1.14)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 8 | 8 | 3.66 | 3.66 | 0.0% |
| fargate | 0 | 8 | 8 | 0.232 | 0.232 | 0.0% |

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
