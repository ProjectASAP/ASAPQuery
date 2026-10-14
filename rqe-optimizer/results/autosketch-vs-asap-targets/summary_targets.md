# Accuracy target sweep

Cheapest plan of each method (no latency bound). Cost by use: CPU only in vCPU, Fargate in $/hour. `dropped`: RQEs ASAP could not serve at the target.

## mixed + multi-grouping

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 112 | 0 | 3.76 (13.36 s, 13, 52) | 4.09 | 11.59 | 3726 (1015 ms) |
| 90% | fargate | 112 | 0 | 0.2434 (2.68 s, 13, 52) | 0.2636 | 0.8119 | 175.9 (1015 ms) |
| 95% | cpu | 112 | 0 | 3.76 (13.36 s, 13, 52) | 4.09 | 11.59 | 3726 (1015 ms) |
| 95% | fargate | 112 | 0 | 0.2434 (2.68 s, 13, 52) | 0.2636 | 0.8119 | 175.9 (1015 ms) |
| 99% | cpu | 112 | 0 | 4.473 (2.68 s, 16, 42) | 4.824 | 12.34 | 3767 (1569 ms) |
| 99% | fargate | 112 | 0 | 0.3646 (2.68 s, 14, 51) | 0.4108 | 1.313 | 193.9 (1569 ms) |

## mixed + multi-grouping, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 896 | 0 | 30.08 (13.36 s, 104, 416) | 32.72 | 92.68 | 2.981e+04 (1015 ms) |
| 90% | fargate | 896 | 0 | 1.947 (2.68 s, 104, 416) | 2.108 | 6.495 | 1407 (1015 ms) |
| 95% | cpu | 896 | 0 | 30.08 (13.36 s, 104, 416) | 32.72 | 92.68 | 2.981e+04 (1015 ms) |
| 95% | fargate | 896 | 0 | 1.947 (2.68 s, 104, 416) | 2.108 | 6.495 | 1407 (1015 ms) |
| 99% | cpu | 896 | 0 | 35.78 (2.68 s, 128, 336) | 38.59 | 98.72 | 3.014e+04 (1569 ms) |
| 99% | fargate | 896 | 0 | 2.917 (2.68 s, 112, 408) | 3.287 | 10.51 | 1551 (1569 ms) |

## mixed + multi-grouping, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 1792 | 0 | 60.15 (13.36 s, 208, 832) | 65.43 | 185.4 | 5.962e+04 (1015 ms) |
| 90% | fargate | 1792 | 0 | 3.894 (2.68 s, 208, 832) | 4.217 | 12.99 | 2815 (1015 ms) |
| 95% | cpu | 1792 | 0 | 60.15 (13.36 s, 208, 832) | 65.43 | 185.4 | 5.962e+04 (1015 ms) |
| 95% | fargate | 1792 | 0 | 3.894 (2.68 s, 208, 832) | 4.217 | 12.99 | 2815 (1015 ms) |
| 99% | cpu | 1792 | 0 | 71.57 (2.68 s, 256, 672) | 77.19 | 197.4 | 6.028e+04 (1569 ms) |
| 99% | fargate | 1792 | 0 | 5.834 (2.68 s, 224, 816) | 6.573 | 21.01 | 3103 (1569 ms) |

## mixed + multi-grouping, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 216 | 0 | 3.904 (2.68 s, 13, 104) | 4.234 | 20.54 | 4468 (1015 ms) |
| 90% | fargate | 216 | 0 | 0.2467 (2.68 s, 13, 104) | 0.267 | 1.241 | 207 (1015 ms) |
| 95% | cpu | 216 | 0 | 3.904 (2.68 s, 13, 104) | 4.234 | 20.54 | 4468 (1015 ms) |
| 95% | fargate | 216 | 0 | 0.2467 (2.68 s, 13, 104) | 0.267 | 1.241 | 207 (1015 ms) |
| 99% | cpu | 216 | 0 | 4.613 (2.68 s, 16, 84) | 4.985 | 21.39 | 4518 (1569 ms) |
| 99% | fargate | 216 | 0 | 0.3705 (2.68 s, 15, 92) | 0.4174 | 1.842 | 226 (1569 ms) |

## mixed

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 50 | 0 | 3.539 (13.36 s, 8, 0) | 3.539 | 10.52 | 3716 (1015 ms) |
| 90% | fargate | 50 | 0 | 0.2297 (2.68 s, 8, 0) | 0.2297 | 0.7522 | 175.5 (1015 ms) |
| 95% | cpu | 50 | 0 | 3.539 (13.36 s, 8, 0) | 3.539 | 10.52 | 3716 (1015 ms) |
| 95% | fargate | 50 | 0 | 0.2297 (2.68 s, 8, 0) | 0.2297 | 0.7522 | 175.5 (1015 ms) |
| 99% | cpu | 50 | 0 | 3.846 (2.68 s, 8, 0) | 3.846 | 10.85 | 3757 (1015 ms) |
| 99% | fargate | 50 | 0 | 0.2998 (2.68 s, 8, 0) | 0.2998 | 1.154 | 193.4 (1015 ms) |

## mixed, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 400 | 0 | 28.31 (13.36 s, 64, 0) | 28.31 | 84.13 | 2.973e+04 (1015 ms) |
| 90% | fargate | 400 | 0 | 1.838 (2.68 s, 64, 0) | 1.838 | 6.018 | 1404 (1015 ms) |
| 95% | cpu | 400 | 0 | 28.31 (13.36 s, 64, 0) | 28.31 | 84.13 | 2.973e+04 (1015 ms) |
| 95% | fargate | 400 | 0 | 1.838 (2.68 s, 64, 0) | 1.838 | 6.018 | 1404 (1015 ms) |
| 99% | cpu | 400 | 0 | 30.77 (2.68 s, 64, 0) | 30.77 | 86.81 | 3.006e+04 (1015 ms) |
| 99% | fargate | 400 | 0 | 2.399 (2.68 s, 64, 0) | 2.399 | 9.228 | 1547 (1015 ms) |

## mixed, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 800 | 0 | 56.62 (13.36 s, 128, 0) | 56.62 | 168.3 | 5.945e+04 (1015 ms) |
| 90% | fargate | 800 | 0 | 3.675 (2.68 s, 128, 0) | 3.675 | 12.04 | 2808 (1015 ms) |
| 95% | cpu | 800 | 0 | 56.62 (13.36 s, 128, 0) | 56.62 | 168.3 | 5.945e+04 (1015 ms) |
| 95% | fargate | 800 | 0 | 3.675 (2.68 s, 128, 0) | 3.675 | 12.04 | 2808 (1015 ms) |
| 99% | cpu | 800 | 0 | 61.53 (2.68 s, 128, 0) | 61.53 | 173.6 | 6.012e+04 (1015 ms) |
| 99% | fargate | 800 | 0 | 4.797 (2.68 s, 128, 0) | 4.797 | 18.46 | 3095 (1015 ms) |

## mixed, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 92 | 0 | 3.662 (2.68 s, 8, 0) | 3.662 | 18.5 | 4456 (1015 ms) |
| 90% | fargate | 92 | 0 | 0.2322 (2.68 s, 8, 0) | 0.2322 | 1.137 | 206.5 (1015 ms) |
| 95% | cpu | 92 | 0 | 3.662 (2.68 s, 8, 0) | 3.662 | 18.5 | 4456 (1015 ms) |
| 95% | fargate | 92 | 0 | 0.2322 (2.68 s, 8, 0) | 0.2322 | 1.137 | 206.5 (1015 ms) |
| 99% | cpu | 92 | 0 | 3.914 (2.68 s, 8, 0) | 3.914 | 18.87 | 4506 (1015 ms) |
| 99% | fargate | 92 | 0 | 0.3026 (2.68 s, 8, 0) | 0.3026 | 1.613 | 225.4 (1015 ms) |

## multi-grouping

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 62 | 0 | 0.2207 (0.56 s, 5, 52) | 0.5508 | 1.069 | 10.22 (490 ms) |
| 90% | fargate | 62 | 0 | 0.01365 (0.56 s, 5, 52) | 0.03385 | 0.05967 | 0.4296 (490 ms) |
| 95% | cpu | 62 | 0 | 0.2207 (0.56 s, 5, 52) | 0.5508 | 1.069 | 10.22 (490 ms) |
| 95% | fargate | 62 | 0 | 0.01365 (0.56 s, 5, 52) | 0.03385 | 0.05967 | 0.4296 (490 ms) |
| 99% | cpu | 62 | 0 | 0.6273 (1.84 s, 8, 42) | 0.9785 | 1.489 | 9.94 (1569 ms) |
| 99% | fargate | 62 | 0 | 0.0648 (1.84 s, 6, 51) | 0.111 | 0.1598 | 0.5018 (1569 ms) |

## multi-grouping, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 496 | 0 | 1.766 (0.56 s, 40, 416) | 4.406 | 8.548 | 81.74 (490 ms) |
| 90% | fargate | 496 | 0 | 0.1092 (0.56 s, 40, 416) | 0.2708 | 0.4773 | 3.437 (490 ms) |
| 95% | cpu | 496 | 0 | 1.766 (0.56 s, 40, 416) | 4.406 | 8.548 | 81.74 (490 ms) |
| 95% | fargate | 496 | 0 | 0.1092 (0.56 s, 40, 416) | 0.2708 | 0.4773 | 3.437 (490 ms) |
| 99% | cpu | 496 | 0 | 5.018 (1.84 s, 64, 336) | 7.828 | 11.91 | 79.52 (1569 ms) |
| 99% | fargate | 496 | 0 | 0.5184 (1.84 s, 48, 408) | 0.8879 | 1.279 | 4.015 (1569 ms) |

## multi-grouping, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 992 | 0 | 3.532 (0.56 s, 80, 832) | 8.813 | 17.1 | 163.5 (490 ms) |
| 90% | fargate | 992 | 0 | 0.2184 (0.56 s, 80, 832) | 0.5416 | 0.9546 | 6.874 (490 ms) |
| 95% | cpu | 992 | 0 | 3.532 (0.56 s, 80, 832) | 8.813 | 17.1 | 163.5 (490 ms) |
| 95% | fargate | 992 | 0 | 0.2184 (0.56 s, 80, 832) | 0.5416 | 0.9546 | 6.874 (490 ms) |
| 99% | cpu | 992 | 0 | 10.04 (1.84 s, 128, 672) | 15.66 | 23.82 | 159 (1569 ms) |
| 99% | fargate | 992 | 0 | 1.037 (1.84 s, 96, 816) | 1.776 | 2.557 | 8.029 (1569 ms) |

## multi-grouping, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 124 | 0 | 0.2419 (0.56 s, 5, 104) | 0.5728 | 2.042 | 12.26 (490 ms) |
| 90% | fargate | 124 | 0 | 0.01451 (0.56 s, 5, 104) | 0.03474 | 0.1035 | 0.5167 (490 ms) |
| 95% | cpu | 124 | 0 | 0.2419 (0.56 s, 5, 104) | 0.5728 | 2.042 | 12.26 (490 ms) |
| 95% | fargate | 124 | 0 | 0.01451 (0.56 s, 5, 104) | 0.03474 | 0.1035 | 0.5167 (490 ms) |
| 99% | cpu | 124 | 0 | 0.6992 (1.84 s, 8, 84) | 1.071 | 2.515 | 11.93 (1569 ms) |
| 99% | fargate | 124 | 0 | 0.06787 (1.84 s, 7, 92) | 0.1148 | 0.2285 | 0.6095 (1569 ms) |
