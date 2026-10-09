# Synthetic mixed set: AutoSketch vs. ASAP, cost by use

Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; Fargate prices, in $/hour). Latency: a plan's query latency (its longest chain, compaction then query, CPU elastic), and the median over its RQEs. AutoSketch's planning time is its search plus its measured benchmark.

## mixed

50 RQEs on 8 streams; dropped: 0; sanity violations: 0. AutoSketch: 166 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 92 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.32 | 3.32 | 42.4 | 1.21e+04 | 103 | 0.758 | 5.12 |
| cpu | PerQuery-CostAware | 9.55 | 9.55 | 161 | 3.84e+03 | 103 | 0.65 | 12 |
| cpu | AutoSketch-Adapted | 3.41e+03 | 3.41e+03 | 5.65e+03 | 1.01e+03 | 1.13 | 212 | 3.41e+03 |
| fargate | ASAP | 0.22 | 3.35 | 19 | 2.94e+03 | 79.7 | 0.867 | 0.284 |
| fargate | PerQuery-CostAware | 0.72 | 9.97 | 71.1 | 2.97e+03 | 103 | 0.658 | 0.813 |
| fargate | AutoSketch-Adapted | 163 | 3.41e+03 | 5.65e+03 | 1.01e+03 | 1.13 | 212 | 163 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (92, 41.6), (141, 21.8), (215, 14.3), (334, 10.4), (508, 7.25), (805, 5.82), (982, 5.12), (1.26e+03, 4.76), (1.53e+03, 4.59), (2.94e+03, 3.32), (1.21e+04, 3.32)
- cpu, PerQuery-CostAware: (92, 66.6), (129, 45.4), (178, 32.2), (252, 25.3), (334, 20.5), (495, 16.3), (636, 14.6), (982, 12), (1.34e+03, 11.2), (1.92e+03, 10.5), (2.56e+03, 10.1), (3.84e+03, 9.55)
- fargate, ASAP: (92, 1.83), (120, 1.13), (170, 0.795), (208, 0.66), (314, 0.52), (405, 0.449), (592, 0.343), (805, 0.316), (989, 0.284), (1.34e+03, 0.272), (1.98e+03, 0.271), (2.94e+03, 0.22)
- fargate, PerQuery-CostAware: (92, 3.25), (125, 2.3), (170, 1.71), (208, 1.49), (314, 1.2), (405, 1.07), (606, 0.924), (805, 0.844), (989, 0.813), (1.48e+03, 0.759), (1.98e+03, 0.751), (2.97e+03, 0.72)

## mixed, m = 8

400 RQEs on 64 streams; dropped: 0; sanity violations: 0. AutoSketch: 1328 probes, 80 distinct (metric, config); measured benchmark 1.7e+03 s at N = 1e+08 (paper rate 4.8e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 92 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 26.5 | 26.5 | 340 | 1.21e+04 | 103 | 6.82 | 41 |
| cpu | PerQuery-CostAware | 76.4 | 76.4 | 1.29e+03 | 3.84e+03 | 103 | 5.55 | 96.2 |
| cpu | AutoSketch-Adapted | 2.73e+04 | 2.73e+04 | 4.52e+04 | 1.01e+03 | 1.13 | 1.7e+03 | 2.73e+04 |
| fargate | ASAP | 1.76 | 26.8 | 152 | 2.94e+03 | 79.7 | 7.94 | 2.27 |
| fargate | PerQuery-CostAware | 5.76 | 79.8 | 569 | 2.97e+03 | 103 | 5.64 | 6.5 |
| fargate | AutoSketch-Adapted | 1.31e+03 | 2.73e+04 | 4.52e+04 | 1.01e+03 | 1.13 | 1.7e+03 | 1.31e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (92, 333), (141, 174), (215, 115), (334, 83.1), (508, 58), (805, 46.5), (982, 41), (1.26e+03, 38.1), (1.53e+03, 36.7), (2.94e+03, 26.6), (1.21e+04, 26.5)
- cpu, PerQuery-CostAware: (92, 533), (129, 363), (178, 258), (252, 202), (334, 164), (495, 131), (636, 116), (982, 96.2), (1.34e+03, 89.6), (1.92e+03, 84.2), (2.56e+03, 80.8), (3.84e+03, 76.4)
- fargate, ASAP: (92, 14.6), (120, 9.05), (170, 6.36), (208, 5.28), (314, 4.16), (405, 3.59), (592, 2.75), (805, 2.53), (989, 2.27), (1.34e+03, 2.17), (1.98e+03, 2.17), (2.94e+03, 1.76)
- fargate, PerQuery-CostAware: (92, 26), (125, 18.4), (170, 13.7), (208, 11.9), (314, 9.56), (405, 8.53), (606, 7.39), (805, 6.75), (989, 6.5), (1.48e+03, 6.07), (1.98e+03, 6.01), (2.97e+03, 5.76)

## mixed, m = 16

800 RQEs on 128 streams; dropped: 0; sanity violations: 0. AutoSketch: 2656 probes, 160 distinct (metric, config); measured benchmark 3.39e+03 s at N = 1e+08 (paper rate 9.6e+03 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 92 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 53.1 | 53.1 | 679 | 1.21e+04 | 103 | 14.7 | 81.9 |
| cpu | PerQuery-CostAware | 153 | 153 | 2.58e+03 | 3.84e+03 | 103 | 11.7 | 192 |
| cpu | AutoSketch-Adapted | 5.46e+04 | 5.46e+04 | 9.03e+04 | 1.01e+03 | 1.13 | 3.39e+03 | 5.46e+04 |
| fargate | ASAP | 3.52 | 53.6 | 303 | 2.94e+03 | 79.7 | 18 | 4.54 |
| fargate | PerQuery-CostAware | 11.5 | 160 | 1.14e+03 | 2.97e+03 | 103 | 12 | 13 |
| fargate | AutoSketch-Adapted | 2.61e+03 | 5.46e+04 | 9.03e+04 | 1.01e+03 | 1.13 | 3.39e+03 | 2.61e+03 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (92, 666), (141, 348), (215, 229), (334, 166), (508, 116), (805, 93.1), (982, 81.9), (1.26e+03, 76.1), (1.53e+03, 73.4), (2.94e+03, 53.2), (1.21e+04, 53.1)
- cpu, PerQuery-CostAware: (92, 1.07e+03), (129, 726), (178, 516), (252, 405), (334, 327), (495, 261), (636, 233), (982, 192), (1.34e+03, 179), (1.92e+03, 168), (2.56e+03, 162), (3.84e+03, 153)
- fargate, ASAP: (92, 29.3), (120, 18.1), (170, 12.7), (208, 10.6), (314, 8.32), (405, 7.19), (592, 5.49), (805, 5.05), (989, 4.54), (1.34e+03, 4.35), (1.98e+03, 4.34), (2.94e+03, 3.52)
- fargate, PerQuery-CostAware: (92, 52), (125, 36.7), (170, 27.4), (208, 23.8), (314, 19.1), (405, 17.1), (606, 14.8), (805, 13.5), (989, 13), (1.48e+03, 12.1), (1.98e+03, 12), (2.97e+03, 11.5)

## mixed, r = 8

92 RQEs on 8 streams; dropped: 0; sanity violations: 0. AutoSketch: 296 probes, 10 distinct (metric, config); measured benchmark 212 s at N = 1e+08 (paper rate 600 s). AutoSketch latency 1.01e+03 ms; tightest feasible bound 92 ms.

| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | median latency (ms) | planning (s) | cost at AutoSketch's latency |
|---|---|---|---|---|---|---|---|---|
| cpu | ASAP | 3.39 | 3.39 | 32.4 | 2.94e+03 | 59.9 | 1.68 | 5.15 |
| cpu | PerQuery-CostAware | 16.7 | 16.7 | 191 | 3.84e+03 | 59.9 | 1.14 | 19.3 |
| cpu | AutoSketch-Adapted | 4.09e+03 | 4.09e+03 | 5.88e+03 | 1.01e+03 | 1.13 | 212 | 4.09e+03 |
| fargate | ASAP | 0.223 | 3.42 | 19 | 2.94e+03 | 53.2 | 2 | 0.285 |
| fargate | PerQuery-CostAware | 1.08 | 17.2 | 85.1 | 2.97e+03 | 59 | 1.15 | 1.18 |
| fargate | AutoSketch-Adapted | 192 | 4.09e+03 | 5.88e+03 | 1.01e+03 | 1.13 | 212 | 192 |

Frontier points (latency ms, cost), by weights:

- cpu, ASAP: (92, 41.6), (120, 25.7), (172, 17.6), (215, 14.3), (316, 10.9), (423, 9.23), (591, 6.58), (805, 5.85), (982, 5.15), (1.53e+03, 4.64), (2.94e+03, 3.39)
- cpu, PerQuery-CostAware: (92, 81.7), (129, 57.8), (178, 42.8), (252, 34.6), (334, 29.3), (495, 24.2), (636, 22), (982, 19.3), (1.34e+03, 18.4), (1.92e+03, 17.6), (2.56e+03, 17.2), (3.84e+03, 16.7)
- fargate, ASAP: (92, 1.83), (120, 1.13), (170, 0.795), (208, 0.66), (314, 0.52), (405, 0.45), (592, 0.344), (805, 0.317), (989, 0.285), (1.34e+03, 0.273), (2.94e+03, 0.223)
- fargate, PerQuery-CostAware: (92, 3.98), (125, 2.9), (170, 2.21), (208, 1.96), (314, 1.61), (405, 1.48), (606, 1.29), (805, 1.21), (989, 1.18), (1.48e+03, 1.11), (1.98e+03, 1.11), (2.97e+03, 1.08)
