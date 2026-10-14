# Synthetic workload: AutoSketch vs. ASAP, cost by use

Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; Fargate prices, in $/hour). Latency: a plan's query latency (its longest chain, compaction then query, CPU elastic), and the median over its RQEs. AutoSketch's planning time is its search plus its measured benchmark.

## mixed + multi-grouping

104 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 238 probes, 15 distinct (metric, config); measured benchmark 396 s at N = 1e+08 (paper rate 900 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.57 | 2.57 | 17.9 | 1.34e+04 | 46.7 | 2.09 | 4.34 |
| cpu | ASAP (no roll-ups) | 2.9 | 2.9 | 19.3 | 1.34e+04 | 39 | 2.03 | 4.67 |
| cpu | PerQuery-CostAware | 8.74 | 8.74 | 43.3 | 3.33e+03 | 59.1 | 2.23 | 11.1 |
| cpu | AutoSketch-Adapted | 2.71e+03 | 2.71e+03 | 4.01e+03 | 1.01e+03 | 9.45 | 396 | 2.71e+03 |
| fargate | ASAP | 0.162 | 2.6 | 12.8 | 2.68e+03 | 46.7 | 2.39 | 0.229 |
| fargate | ASAP (no roll-ups) | 0.182 | 2.93 | 14.3 | 2.68e+03 | 39 | 2.19 | 0.249 |
| fargate | PerQuery-CostAware | 0.545 | 8.85 | 42 | 2.97e+03 | 59.1 | 2.24 | 0.635 |
| fargate | AutoSketch-Adapted | 128 | 2.71e+03 | 4.01e+03 | 1.01e+03 | 9.45 | 396 | 128 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.19), (658, 5.91), (765, 5.16), (989, 4.34), (1.19e+03, 4.29), (1.48e+03, 4.02), (2.68e+03, 2.6), (1.34e+04, 2.57)
- cpu, ASAP (no roll-ups): (490, 7.69), (658, 6.28), (765, 5.52), (989, 4.67), (1.19e+03, 4.62), (1.48e+03, 4.35), (2.68e+03, 2.93), (1.34e+04, 2.9)
- cpu, PerQuery-CostAware: (490, 16.4), (575, 14.2), (673, 12.9), (765, 12.2), (959, 11.5), (989, 11.1), (1.14e+03, 10.8), (1.34e+03, 10.2), (1.49e+03, 9.8), (2.29e+03, 9.31), (2.68e+03, 8.95), (3.33e+03, 8.74)
- fargate, ASAP: (490, 0.344), (557, 0.296), (592, 0.293), (765, 0.262), (896, 0.232), (989, 0.229), (1.19e+03, 0.226), (1.48e+03, 0.22), (2.29e+03, 0.192), (2.68e+03, 0.162)
- fargate, ASAP (no roll-ups): (490, 0.372), (557, 0.317), (592, 0.315), (765, 0.284), (896, 0.254), (989, 0.249), (1.19e+03, 0.246), (1.48e+03, 0.24), (2.29e+03, 0.211), (2.68e+03, 0.182)
- fargate, PerQuery-CostAware: (490, 0.844), (575, 0.759), (673, 0.703), (765, 0.674), (896, 0.65), (989, 0.635), (1.19e+03, 0.613), (1.49e+03, 0.584), (1.98e+03, 0.578), (2.29e+03, 0.564), (2.97e+03, 0.545)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 52 | 11 | 32 | 2.57 | 2.9 | 11.4% |
| fargate | 52 | 11 | 32 | 0.162 | 0.182 | 10.9% |

## mixed + multi-grouping, m = 8

832 RQEs on 72 streams; dropped: 0; sanity violations: 0. AutoSketch: 1904 probes, 120 distinct (metric, config); measured benchmark 3.17e+03 s at N = 1e+08 (paper rate 7.2e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 20.6 | 20.6 | 143 | 1.34e+04 | 46.7 | 18.3 | 34.7 |
| cpu | ASAP (no roll-ups) | 23.2 | 23.2 | 155 | 1.34e+04 | 39 | 17.5 | 37.4 |
| cpu | PerQuery-CostAware | 69.9 | 69.9 | 346 | 3.33e+03 | 59.1 | 19.7 | 89 |
| cpu | AutoSketch-Adapted | 2.17e+04 | 2.17e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 3.17e+03 | 2.17e+04 |
| fargate | ASAP | 1.3 | 20.8 | 103 | 2.68e+03 | 46.7 | 22.7 | 1.83 |
| fargate | ASAP (no roll-ups) | 1.46 | 23.5 | 114 | 2.68e+03 | 39 | 18.8 | 1.99 |
| fargate | PerQuery-CostAware | 4.36 | 70.8 | 336 | 2.97e+03 | 59.1 | 19.7 | 5.08 |
| fargate | AutoSketch-Adapted | 1.02e+03 | 2.17e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 3.17e+03 | 1.02e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 57.5), (658, 47.3), (765, 41.3), (989, 34.7), (1.19e+03, 34.3), (1.48e+03, 32.1), (2.68e+03, 20.8), (1.34e+04, 20.6)
- cpu, ASAP (no roll-ups): (490, 61.5), (658, 50.2), (765, 44.2), (989, 37.4), (1.19e+03, 36.9), (1.48e+03, 34.8), (2.68e+03, 23.5), (1.34e+04, 23.2)
- cpu, PerQuery-CostAware: (490, 131), (575, 114), (673, 103), (765, 97.2), (959, 91.9), (989, 89), (1.14e+03, 86.2), (1.34e+03, 81.8), (1.49e+03, 78.4), (2.29e+03, 74.5), (2.68e+03, 71.6), (3.33e+03, 69.9)
- fargate, ASAP: (490, 2.75), (557, 2.36), (592, 2.34), (765, 2.1), (896, 1.86), (989, 1.83), (1.19e+03, 1.81), (1.48e+03, 1.76), (2.29e+03, 1.53), (2.68e+03, 1.3)
- fargate, ASAP (no roll-ups): (490, 2.97), (557, 2.54), (592, 2.52), (765, 2.27), (896, 2.03), (989, 1.99), (1.19e+03, 1.97), (1.48e+03, 1.92), (2.29e+03, 1.69), (2.68e+03, 1.46)
- fargate, PerQuery-CostAware: (490, 6.75), (575, 6.07), (673, 5.62), (765, 5.39), (896, 5.2), (989, 5.08), (1.19e+03, 4.91), (1.49e+03, 4.67), (1.98e+03, 4.63), (2.29e+03, 4.51), (2.97e+03, 4.36)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 416 | 88 | 256 | 20.6 | 23.2 | 11.4% |
| fargate | 416 | 88 | 256 | 1.3 | 1.46 | 10.9% |

## mixed + multi-grouping, m = 16

1664 RQEs on 144 streams; dropped: 0; sanity violations: 0. AutoSketch: 3808 probes, 240 distinct (metric, config); measured benchmark 6.34e+03 s at N = 1e+08 (paper rate 1.44e+04 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 41.1 | 41.1 | 286 | 1.34e+04 | 46.7 | 16.1 | 69.5 |
| cpu | ASAP (no roll-ups) | 46.4 | 46.4 | 309 | 1.34e+04 | 39 | 15.1 | 74.8 |
| cpu | PerQuery-CostAware | 140 | 140 | 693 | 3.33e+03 | 59.1 | 17.1 | 178 |
| cpu | AutoSketch-Adapted | 4.34e+04 | 4.34e+04 | 6.41e+04 | 1.01e+03 | 9.45 | 6.34e+03 | 4.34e+04 |
| fargate | ASAP | 2.6 | 41.6 | 205 | 2.68e+03 | 46.7 | 22.2 | 3.66 |
| fargate | ASAP (no roll-ups) | 2.92 | 46.9 | 229 | 2.68e+03 | 39 | 18.5 | 3.98 |
| fargate | PerQuery-CostAware | 8.73 | 142 | 672 | 2.97e+03 | 59.1 | 17.2 | 10.2 |
| fargate | AutoSketch-Adapted | 2.04e+03 | 4.34e+04 | 6.41e+04 | 1.01e+03 | 9.45 | 6.34e+03 | 2.04e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 115), (658, 94.6), (765, 82.5), (989, 69.5), (1.19e+03, 68.6), (1.48e+03, 64.3), (2.68e+03, 41.6), (1.34e+04, 41.1)
- cpu, ASAP (no roll-ups): (490, 123), (658, 100), (765, 88.4), (989, 74.8), (1.19e+03, 73.9), (1.48e+03, 69.6), (2.68e+03, 46.9), (1.34e+04, 46.4)
- cpu, PerQuery-CostAware: (490, 262), (575, 228), (673, 206), (765, 194), (959, 184), (989, 178), (1.14e+03, 172), (1.34e+03, 164), (1.49e+03, 157), (2.29e+03, 149), (2.68e+03, 143), (3.33e+03, 140)
- fargate, ASAP: (490, 5.5), (557, 4.73), (592, 4.69), (765, 4.2), (896, 3.71), (989, 3.66), (1.19e+03, 3.62), (1.48e+03, 3.52), (2.29e+03, 3.06), (2.68e+03, 2.6)
- fargate, ASAP (no roll-ups): (490, 5.95), (557, 5.07), (592, 5.03), (765, 4.55), (896, 4.06), (989, 3.98), (1.19e+03, 3.94), (1.48e+03, 3.84), (2.29e+03, 3.38), (2.68e+03, 2.92)
- fargate, PerQuery-CostAware: (490, 13.5), (575, 12.1), (673, 11.2), (765, 10.8), (896, 10.4), (989, 10.2), (1.19e+03, 9.82), (1.49e+03, 9.35), (1.98e+03, 9.25), (2.29e+03, 9.03), (2.97e+03, 8.73)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 832 | 176 | 512 | 41.1 | 46.4 | 11.4% |
| fargate | 832 | 176 | 512 | 2.6 | 2.92 | 10.9% |

## mixed + multi-grouping, r = 8

200 RQEs on 9 streams; dropped: 0; sanity violations: 0. AutoSketch: 454 probes, 15 distinct (metric, config); measured benchmark 396 s at N = 1e+08 (paper rate 900 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 490 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.68 | 2.68 | 12.8 | 2.68e+03 | 46.7 | 2.28 | 4.4 |
| cpu | ASAP (no roll-ups) | 3.01 | 3.01 | 14.3 | 2.68e+03 | 39 | 2.18 | 4.73 |
| cpu | PerQuery-CostAware | 15.5 | 15.5 | 51.5 | 3.33e+03 | 34 | 2.13 | 18.1 |
| cpu | AutoSketch-Adapted | 3.25e+03 | 3.25e+03 | 4.17e+03 | 1.01e+03 | 5.33 | 396 | 3.25e+03 |
| fargate | ASAP | 0.165 | 2.68 | 12.8 | 2.68e+03 | 46.7 | 2.53 | 0.231 |
| fargate | ASAP (no roll-ups) | 0.185 | 3.01 | 14.3 | 2.68e+03 | 39 | 2.39 | 0.251 |
| fargate | PerQuery-CostAware | 0.856 | 15.6 | 50.2 | 2.97e+03 | 34 | 2.14 | 0.953 |
| fargate | AutoSketch-Adapted | 150 | 3.25e+03 | 4.17e+03 | 1.01e+03 | 5.33 | 396 | 150 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (490, 7.22), (557, 6.03), (658, 5.96), (765, 5.2), (896, 4.46), (989, 4.4), (1.19e+03, 4.34), (1.48e+03, 4.08), (2.29e+03, 3.39), (2.68e+03, 2.68)
- cpu, ASAP (no roll-ups): (490, 7.72), (557, 6.4), (658, 6.32), (765, 5.57), (896, 4.82), (989, 4.73), (1.19e+03, 4.67), (1.48e+03, 4.41), (2.29e+03, 3.72), (2.68e+03, 3.01)
- cpu, PerQuery-CostAware: (490, 24.1), (575, 21.4), (673, 19.8), (765, 19.1), (959, 18.4), (989, 18.1), (1.14e+03, 17.7), (1.34e+03, 17), (1.49e+03, 16.6), (2.29e+03, 16.1), (2.68e+03, 15.7), (3.33e+03, 15.5)
- fargate, ASAP: (490, 0.345), (557, 0.297), (592, 0.295), (765, 0.264), (896, 0.234), (989, 0.231), (1.19e+03, 0.228), (1.48e+03, 0.222), (2.29e+03, 0.194), (2.68e+03, 0.165)
- fargate, ASAP (no roll-ups): (490, 0.373), (557, 0.319), (592, 0.316), (765, 0.286), (896, 0.256), (989, 0.251), (1.19e+03, 0.248), (1.48e+03, 0.242), (2.29e+03, 0.214), (2.68e+03, 0.185)
- fargate, PerQuery-CostAware: (490, 1.19), (575, 1.08), (673, 1.02), (765, 0.992), (896, 0.968), (989, 0.953), (1.19e+03, 0.924), (1.49e+03, 0.895), (1.98e+03, 0.889), (2.29e+03, 0.875), (2.97e+03, 0.856)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 104 | 11 | 32 | 2.68 | 3.01 | 11.0% |
| fargate | 104 | 11 | 32 | 0.165 | 0.185 | 10.7% |

## mixed

42 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 100 probes, 8 distinct (metric, config); measured benchmark 169 s at N = 1e+08 (paper rate 480 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.35 | 2.35 | 16.8 | 1.34e+04 | 97.4 | 0.991 | 4.12 |
| cpu | ASAP (no roll-ups) | 2.35 | 2.35 | 16.8 | 1.34e+04 | 97.4 | 0.993 | 4.12 |
| cpu | PerQuery-CostAware | 7.67 | 7.67 | 39.8 | 3.33e+03 | 245 | 1.65 | 10.1 |
| cpu | AutoSketch-Adapted | 2.7e+03 | 2.7e+03 | 4e+03 | 1.01e+03 | 9.45 | 169 | 2.7e+03 |
| fargate | ASAP | 0.149 | 2.38 | 11.8 | 2.68e+03 | 46.7 | 1.06 | 0.215 |
| fargate | ASAP (no roll-ups) | 0.149 | 2.38 | 11.8 | 2.68e+03 | 46.7 | 1.06 | 0.215 |
| fargate | PerQuery-CostAware | 0.487 | 7.78 | 38.5 | 2.97e+03 | 245 | 1.65 | 0.576 |
| fargate | AutoSketch-Adapted | 127 | 2.7e+03 | 4e+03 | 1.01e+03 | 9.45 | 169 | 127 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.1), (137, 21.3), (206, 13.7), (342, 9.55), (539, 5.77), (765, 4.94), (989, 4.12), (1.19e+03, 4.07), (1.48e+03, 3.8), (2.68e+03, 2.38), (1.34e+04, 2.35)
- cpu, ASAP (no roll-ups): (87.7, 44.1), (137, 21.3), (206, 13.7), (342, 9.55), (539, 5.77), (765, 4.94), (989, 4.12), (1.19e+03, 4.07), (1.48e+03, 3.8), (2.68e+03, 2.38), (1.34e+04, 2.35)
- cpu, PerQuery-CostAware: (87.7, 69.2), (121, 44.8), (167, 30.7), (235, 23.9), (328, 18.7), (452, 15.4), (592, 12.7), (829, 10.8), (989, 10.1), (1.19e+03, 9.53), (1.49e+03, 8.74), (2.29e+03, 8.24), (3.33e+03, 7.67)
- fargate, ASAP: (87.7, 1.85), (112, 1.11), (163, 0.718), (206, 0.602), (295, 0.466), (394, 0.386), (539, 0.282), (765, 0.249), (989, 0.215), (1.19e+03, 0.213), (1.48e+03, 0.206), (2.68e+03, 0.149)
- fargate, ASAP (no roll-ups): (87.7, 1.85), (112, 1.11), (163, 0.718), (206, 0.602), (295, 0.466), (394, 0.386), (539, 0.282), (765, 0.249), (989, 0.215), (1.19e+03, 0.213), (1.48e+03, 0.206), (2.68e+03, 0.149)
- fargate, PerQuery-CostAware: (87.7, 2.99), (118, 2.02), (163, 1.44), (227, 1.21), (305, 0.936), (394, 0.82), (592, 0.681), (765, 0.614), (989, 0.576), (1.49e+03, 0.525), (1.98e+03, 0.519), (2.97e+03, 0.487)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 6 | 6 | 2.35 | 2.35 | 0.0% |
| fargate | 0 | 6 | 6 | 0.149 | 0.149 | 0.0% |

## mixed, m = 8

336 RQEs on 48 streams; dropped: 0; sanity violations: 0. AutoSketch: 800 probes, 64 distinct (metric, config); measured benchmark 1.36e+03 s at N = 1e+08 (paper rate 3.84e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 18.8 | 18.8 | 135 | 1.34e+04 | 97.4 | 8.6 | 33 |
| cpu | ASAP (no roll-ups) | 18.8 | 18.8 | 135 | 1.34e+04 | 97.4 | 8.6 | 33 |
| cpu | PerQuery-CostAware | 61.3 | 61.3 | 318 | 3.33e+03 | 245 | 14 | 80.5 |
| cpu | AutoSketch-Adapted | 2.16e+04 | 2.16e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 1.36e+03 | 2.16e+04 |
| fargate | ASAP | 1.19 | 19.1 | 94.5 | 2.68e+03 | 46.7 | 9.01 | 1.72 |
| fargate | ASAP (no roll-ups) | 1.19 | 19.1 | 94.5 | 2.68e+03 | 46.7 | 9.04 | 1.72 |
| fargate | PerQuery-CostAware | 3.89 | 62.3 | 308 | 2.97e+03 | 245 | 14.1 | 4.61 |
| fargate | AutoSketch-Adapted | 1.02e+03 | 2.16e+04 | 3.2e+04 | 1.01e+03 | 9.45 | 1.36e+03 | 1.02e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 353), (137, 170), (206, 110), (342, 76.4), (539, 46.2), (765, 39.5), (989, 33), (1.19e+03, 32.5), (1.48e+03, 30.4), (2.68e+03, 19.1), (1.34e+04, 18.8)
- cpu, ASAP (no roll-ups): (87.7, 353), (137, 170), (206, 110), (342, 76.4), (539, 46.2), (765, 39.5), (989, 33), (1.19e+03, 32.5), (1.48e+03, 30.4), (2.68e+03, 19.1), (1.34e+04, 18.8)
- cpu, PerQuery-CostAware: (87.7, 553), (121, 358), (167, 246), (235, 191), (328, 149), (452, 123), (592, 102), (829, 86.3), (989, 80.5), (1.19e+03, 76.2), (1.49e+03, 69.9), (2.29e+03, 65.9), (3.33e+03, 61.3)
- fargate, ASAP: (87.7, 14.8), (112, 8.88), (163, 5.74), (206, 4.82), (295, 3.73), (394, 3.09), (539, 2.26), (765, 1.99), (989, 1.72), (1.19e+03, 1.7), (1.48e+03, 1.65), (2.68e+03, 1.19)
- fargate, ASAP (no roll-ups): (87.7, 14.8), (112, 8.88), (163, 5.74), (206, 4.82), (295, 3.73), (394, 3.09), (539, 2.26), (765, 1.99), (989, 1.72), (1.19e+03, 1.7), (1.48e+03, 1.65), (2.68e+03, 1.19)
- fargate, PerQuery-CostAware: (87.7, 23.9), (118, 16.1), (163, 11.5), (227, 9.68), (305, 7.48), (394, 6.56), (592, 5.45), (765, 4.91), (989, 4.61), (1.49e+03, 4.2), (1.98e+03, 4.15), (2.97e+03, 3.89)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 48 | 48 | 18.8 | 18.8 | 0.0% |
| fargate | 0 | 48 | 48 | 1.19 | 1.19 | 0.0% |

## mixed, m = 16

672 RQEs on 96 streams; dropped: 0; sanity violations: 0. AutoSketch: 1600 probes, 128 distinct (metric, config); measured benchmark 2.71e+03 s at N = 1e+08 (paper rate 7.68e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 37.6 | 37.6 | 270 | 1.34e+04 | 97.4 | 7.36 | 66 |
| cpu | ASAP (no roll-ups) | 37.6 | 37.6 | 270 | 1.34e+04 | 97.4 | 7.36 | 66 |
| cpu | PerQuery-CostAware | 123 | 123 | 636 | 3.33e+03 | 245 | 12 | 161 |
| cpu | AutoSketch-Adapted | 4.32e+04 | 4.32e+04 | 6.4e+04 | 1.01e+03 | 9.45 | 2.71e+03 | 4.32e+04 |
| fargate | ASAP | 2.38 | 38.1 | 189 | 2.68e+03 | 46.7 | 8.21 | 3.45 |
| fargate | ASAP (no roll-ups) | 2.38 | 38.1 | 189 | 2.68e+03 | 46.7 | 8.26 | 3.45 |
| fargate | PerQuery-CostAware | 7.78 | 125 | 616 | 2.97e+03 | 245 | 12.2 | 9.22 |
| fargate | AutoSketch-Adapted | 2.04e+03 | 4.32e+04 | 6.4e+04 | 1.01e+03 | 9.45 | 2.71e+03 | 2.04e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 706), (137, 341), (206, 219), (342, 153), (539, 92.3), (765, 79), (989, 66), (1.19e+03, 65), (1.48e+03, 60.8), (2.68e+03, 38.1), (1.34e+04, 37.6)
- cpu, ASAP (no roll-ups): (87.7, 706), (137, 341), (206, 219), (342, 153), (539, 92.3), (765, 79), (989, 66), (1.19e+03, 65), (1.48e+03, 60.8), (2.68e+03, 38.1), (1.34e+04, 37.6)
- cpu, PerQuery-CostAware: (87.7, 1.11e+03), (121, 717), (167, 491), (235, 382), (328, 299), (452, 247), (592, 203), (829, 173), (989, 161), (1.19e+03, 152), (1.49e+03, 140), (2.29e+03, 132), (3.33e+03, 123)
- fargate, ASAP: (87.7, 29.6), (112, 17.8), (163, 11.5), (206, 9.63), (295, 7.46), (394, 6.18), (539, 4.51), (765, 3.99), (989, 3.45), (1.19e+03, 3.4), (1.48e+03, 3.3), (2.68e+03, 2.38)
- fargate, ASAP (no roll-ups): (87.7, 29.6), (112, 17.8), (163, 11.5), (206, 9.63), (295, 7.46), (394, 6.18), (539, 4.51), (765, 3.99), (989, 3.45), (1.19e+03, 3.4), (1.48e+03, 3.3), (2.68e+03, 2.38)
- fargate, PerQuery-CostAware: (87.7, 47.9), (118, 32.3), (163, 23.1), (227, 19.4), (305, 15), (394, 13.1), (592, 10.9), (765, 9.83), (989, 9.22), (1.49e+03, 8.4), (1.98e+03, 8.31), (2.97e+03, 7.78)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 96 | 96 | 37.6 | 37.6 | 0.0% |
| fargate | 0 | 96 | 96 | 2.38 | 2.38 | 0.0% |

## mixed, r = 8

76 RQEs on 6 streams; dropped: 0; sanity violations: 0. AutoSketch: 178 probes, 8 distinct (metric, config); measured benchmark 169 s at N = 1e+08 (paper rate 480 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 87.7 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 2.43 | 2.43 | 11.8 | 2.68e+03 | 97.4 | 1.06 | 4.16 |
| cpu | ASAP (no roll-ups) | 2.43 | 2.43 | 11.8 | 2.68e+03 | 97.4 | 1.06 | 4.16 |
| cpu | PerQuery-CostAware | 13.5 | 13.5 | 47 | 3.33e+03 | 59.1 | 1.51 | 16 |
| cpu | AutoSketch-Adapted | 3.24e+03 | 3.24e+03 | 4.16e+03 | 1.01e+03 | 9.45 | 169 | 3.24e+03 |
| fargate | ASAP | 0.151 | 2.43 | 11.8 | 2.68e+03 | 97.4 | 1.23 | 0.217 |
| fargate | ASAP (no roll-ups) | 0.151 | 2.43 | 11.8 | 2.68e+03 | 97.4 | 1.23 | 0.217 |
| fargate | PerQuery-CostAware | 0.754 | 13.6 | 45.8 | 2.97e+03 | 59.1 | 1.52 | 0.851 |
| fargate | AutoSketch-Adapted | 150 | 3.24e+03 | 4.16e+03 | 1.01e+03 | 9.45 | 169 | 150 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (87.7, 44.1), (112, 26.2), (163, 16.5), (206, 13.7), (295, 10.4), (394, 8.37), (539, 5.79), (765, 4.96), (989, 4.16), (1.19e+03, 4.1), (1.48e+03, 3.84), (2.68e+03, 2.43)
- cpu, ASAP (no roll-ups): (87.7, 44.1), (112, 26.2), (163, 16.5), (206, 13.7), (295, 10.4), (394, 8.37), (539, 5.79), (765, 4.96), (989, 4.16), (1.19e+03, 4.1), (1.48e+03, 3.84), (2.68e+03, 2.43)
- cpu, PerQuery-CostAware: (87.7, 83.8), (121, 56.6), (167, 40), (235, 32), (328, 25.9), (452, 22.5), (592, 18.7), (829, 16.8), (989, 16), (1.19e+03, 15.3), (1.49e+03, 14.5), (2.29e+03, 14), (3.33e+03, 13.5)
- fargate, ASAP: (87.7, 1.85), (112, 1.11), (163, 0.718), (206, 0.602), (295, 0.467), (394, 0.387), (539, 0.283), (765, 0.25), (989, 0.217), (1.19e+03, 0.214), (1.48e+03, 0.208), (2.68e+03, 0.151)
- fargate, ASAP (no roll-ups): (87.7, 1.85), (112, 1.11), (163, 0.718), (206, 0.602), (295, 0.467), (394, 0.387), (539, 0.283), (765, 0.25), (989, 0.217), (1.19e+03, 0.214), (1.48e+03, 0.208), (2.68e+03, 0.151)
- fargate, PerQuery-CostAware: (87.7, 3.62), (118, 2.53), (163, 1.85), (227, 1.59), (305, 1.26), (394, 1.14), (592, 0.955), (765, 0.889), (989, 0.851), (1.49e+03, 0.793), (1.98e+03, 0.787), (2.97e+03, 0.754)

Roll-up ablation (ASAP vs. ASAP without roll-ups):

| weights | RQEs rolled up | deployments with | deployments without | cost with | cost without | saving |
|---|---|---|---|---|---|---|
| cpu | 0 | 6 | 6 | 2.43 | 2.43 | 0.0% |
| fargate | 0 | 6 | 6 | 0.151 | 0.151 | 0.0% |

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
