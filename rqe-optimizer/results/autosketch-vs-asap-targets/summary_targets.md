# Accuracy target sweep

Cheapest plan of each method (no latency bound). Cost by use: CPU only in vCPU, Fargate in $/hour. `dropped`: RQEs ASAP could not serve at the target.

## mixed + multi-grouping

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 104 | 0 | 2.571 (13.36 s, 11, 52) | 2.901 | 8.736 | 2711 (1015 ms) |
| 90% | fargate | 104 | 0 | 0.1624 (2.68 s, 11, 52) | 0.1823 | 0.5454 | 127.6 (1015 ms) |
| 95% | cpu | 104 | 0 | 2.571 (13.36 s, 11, 52) | 2.901 | 8.736 | 2711 (1015 ms) |
| 95% | fargate | 104 | 0 | 0.1624 (2.68 s, 11, 52) | 0.1823 | 0.5454 | 127.6 (1015 ms) |
| 99% | cpu | 104 | 0 | 3.169 (2.68 s, 14, 42) | 3.52 | 9.442 | 2751 (1569 ms) |
| 99% | fargate | 104 | 0 | 0.3003 (2.68 s, 12, 51) | 0.3465 | 1.178 | 152.7 (1569 ms) |

## mixed + multi-grouping, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 832 | 0 | 20.57 (13.36 s, 88, 416) | 23.21 | 69.89 | 2.169e+04 (1015 ms) |
| 90% | fargate | 832 | 0 | 1.3 (2.68 s, 88, 416) | 1.458 | 4.363 | 1021 (1015 ms) |
| 95% | cpu | 832 | 0 | 20.57 (13.36 s, 88, 416) | 23.21 | 69.89 | 2.169e+04 (1015 ms) |
| 95% | fargate | 832 | 0 | 1.3 (2.68 s, 88, 416) | 1.458 | 4.363 | 1021 (1015 ms) |
| 99% | cpu | 832 | 0 | 25.35 (2.68 s, 112, 336) | 28.16 | 75.54 | 2.201e+04 (1569 ms) |
| 99% | fargate | 832 | 0 | 2.403 (2.68 s, 96, 408) | 2.772 | 9.424 | 1222 (1569 ms) |

## mixed + multi-grouping, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 1664 | 0 | 41.14 (13.36 s, 176, 832) | 46.42 | 139.8 | 4.338e+04 (1015 ms) |
| 90% | fargate | 1664 | 0 | 2.599 (2.68 s, 176, 832) | 2.917 | 8.727 | 2042 (1015 ms) |
| 95% | cpu | 1664 | 0 | 41.14 (13.36 s, 176, 832) | 46.42 | 139.8 | 4.338e+04 (1015 ms) |
| 95% | fargate | 1664 | 0 | 2.599 (2.68 s, 176, 832) | 2.917 | 8.727 | 2042 (1015 ms) |
| 99% | cpu | 1664 | 0 | 50.7 (2.68 s, 224, 672) | 56.32 | 151.1 | 4.402e+04 (1569 ms) |
| 99% | fargate | 1664 | 0 | 4.805 (2.68 s, 192, 816) | 5.544 | 18.85 | 2444 (1569 ms) |

## mixed + multi-grouping, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 200 | 0 | 2.676 (2.68 s, 11, 104) | 3.007 | 15.51 | 3250 (1015 ms) |
| 90% | fargate | 200 | 0 | 0.1655 (2.68 s, 11, 104) | 0.1854 | 0.8563 | 150.2 (1015 ms) |
| 95% | cpu | 200 | 0 | 2.676 (2.68 s, 11, 104) | 3.007 | 15.51 | 3250 (1015 ms) |
| 95% | fargate | 200 | 0 | 0.1655 (2.68 s, 11, 104) | 0.1854 | 0.8563 | 150.2 (1015 ms) |
| 99% | cpu | 200 | 0 | 3.303 (2.68 s, 14, 84) | 3.675 | 16.3 | 3299 (1569 ms) |
| 99% | fargate | 200 | 0 | 0.3059 (2.68 s, 13, 92) | 0.3528 | 1.616 | 176.6 (1569 ms) |

## mixed

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 42 | 0 | 2.351 (13.36 s, 6, 0) | 2.351 | 7.668 | 2701 (1015 ms) |
| 90% | fargate | 42 | 0 | 0.149 (2.68 s, 6, 0) | 0.149 | 0.4866 | 127.2 (1015 ms) |
| 95% | cpu | 42 | 0 | 2.351 (13.36 s, 6, 0) | 2.351 | 7.668 | 2701 (1015 ms) |
| 95% | fargate | 42 | 0 | 0.149 (2.68 s, 6, 0) | 0.149 | 0.4866 | 127.2 (1015 ms) |
| 99% | cpu | 42 | 0 | 2.542 (2.68 s, 6, 0) | 2.542 | 7.953 | 2741 (1015 ms) |
| 99% | fargate | 42 | 0 | 0.2355 (2.68 s, 6, 0) | 0.2355 | 1.018 | 152.2 (1015 ms) |

## mixed, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 336 | 0 | 18.81 (13.36 s, 48, 0) | 18.81 | 61.35 | 2.161e+04 (1015 ms) |
| 90% | fargate | 336 | 0 | 1.192 (2.68 s, 48, 0) | 1.192 | 3.892 | 1018 (1015 ms) |
| 95% | cpu | 336 | 0 | 18.81 (13.36 s, 48, 0) | 18.81 | 61.35 | 2.161e+04 (1015 ms) |
| 95% | fargate | 336 | 0 | 1.192 (2.68 s, 48, 0) | 1.192 | 3.892 | 1018 (1015 ms) |
| 99% | cpu | 336 | 0 | 20.33 (2.68 s, 48, 0) | 20.33 | 63.63 | 2.193e+04 (1015 ms) |
| 99% | fargate | 336 | 0 | 1.884 (2.68 s, 48, 0) | 1.884 | 8.146 | 1218 (1015 ms) |

## mixed, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 672 | 0 | 37.62 (13.36 s, 96, 0) | 37.62 | 122.7 | 4.322e+04 (1015 ms) |
| 90% | fargate | 672 | 0 | 2.384 (2.68 s, 96, 0) | 2.384 | 7.785 | 2035 (1015 ms) |
| 95% | cpu | 672 | 0 | 37.62 (13.36 s, 96, 0) | 37.62 | 122.7 | 4.322e+04 (1015 ms) |
| 95% | fargate | 672 | 0 | 2.384 (2.68 s, 96, 0) | 2.384 | 7.785 | 2035 (1015 ms) |
| 99% | cpu | 672 | 0 | 40.67 (2.68 s, 96, 0) | 40.67 | 127.3 | 4.386e+04 (1015 ms) |
| 99% | fargate | 672 | 0 | 3.768 (2.68 s, 96, 0) | 3.768 | 16.29 | 2436 (1015 ms) |

## mixed, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 76 | 0 | 2.435 (2.68 s, 6, 0) | 2.435 | 13.47 | 3239 (1015 ms) |
| 90% | fargate | 76 | 0 | 0.1512 (2.68 s, 6, 0) | 0.1512 | 0.7538 | 149.7 (1015 ms) |
| 95% | cpu | 76 | 0 | 2.435 (2.68 s, 6, 0) | 2.435 | 13.47 | 3239 (1015 ms) |
| 95% | fargate | 76 | 0 | 0.1512 (2.68 s, 6, 0) | 0.1512 | 0.7538 | 149.7 (1015 ms) |
| 99% | cpu | 76 | 0 | 2.603 (2.68 s, 6, 0) | 2.603 | 13.78 | 3287 (1015 ms) |
| 99% | fargate | 76 | 0 | 0.238 (2.68 s, 6, 0) | 0.238 | 1.387 | 176 (1015 ms) |

## multi-grouping

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 62 | 0 | 0.2199 (0.56 s, 5, 52) | 0.5503 | 1.068 | 9.648 (490 ms) |
| 90% | fargate | 62 | 0 | 0.01342 (0.56 s, 5, 52) | 0.03328 | 0.05887 | 0.4064 (490 ms) |
| 95% | cpu | 62 | 0 | 0.2199 (0.56 s, 5, 52) | 0.5503 | 1.068 | 9.648 (490 ms) |
| 95% | fargate | 62 | 0 | 0.01342 (0.56 s, 5, 52) | 0.03328 | 0.05887 | 0.4064 (490 ms) |
| 99% | cpu | 62 | 0 | 0.6273 (1.84 s, 8, 42) | 0.9785 | 1.489 | 9.94 (1569 ms) |
| 99% | fargate | 62 | 0 | 0.0648 (1.84 s, 6, 51) | 0.111 | 0.1598 | 0.5018 (1569 ms) |

## multi-grouping, 8 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 496 | 0 | 1.759 (0.56 s, 40, 416) | 4.402 | 8.542 | 77.18 (490 ms) |
| 90% | fargate | 496 | 0 | 0.1074 (0.56 s, 40, 416) | 0.2662 | 0.4709 | 3.251 (490 ms) |
| 95% | cpu | 496 | 0 | 1.759 (0.56 s, 40, 416) | 4.402 | 8.542 | 77.18 (490 ms) |
| 95% | fargate | 496 | 0 | 0.1074 (0.56 s, 40, 416) | 0.2662 | 0.4709 | 3.251 (490 ms) |
| 99% | cpu | 496 | 0 | 5.018 (1.84 s, 64, 336) | 7.828 | 11.91 | 79.52 (1569 ms) |
| 99% | fargate | 496 | 0 | 0.5184 (1.84 s, 48, 408) | 0.8879 | 1.279 | 4.015 (1569 ms) |

## multi-grouping, 16 metrics

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 992 | 0 | 3.519 (0.56 s, 80, 832) | 8.804 | 17.08 | 154.4 (490 ms) |
| 90% | fargate | 992 | 0 | 0.2148 (0.56 s, 80, 832) | 0.5324 | 0.9419 | 6.502 (490 ms) |
| 95% | cpu | 992 | 0 | 3.519 (0.56 s, 80, 832) | 8.804 | 17.08 | 154.4 (490 ms) |
| 95% | fargate | 992 | 0 | 0.2148 (0.56 s, 80, 832) | 0.5324 | 0.9419 | 6.502 (490 ms) |
| 99% | cpu | 992 | 0 | 10.04 (1.84 s, 128, 672) | 15.66 | 23.82 | 159 (1569 ms) |
| 99% | fargate | 992 | 0 | 1.037 (1.84 s, 96, 816) | 1.776 | 2.557 | 8.029 (1569 ms) |

## multi-grouping, 8 users

| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) | ASAP no roll-ups | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|---|---|
| 90% | cpu | 124 | 0 | 0.241 (0.56 s, 5, 104) | 0.5722 | 2.04 | 11.58 (490 ms) |
| 90% | fargate | 124 | 0 | 0.01428 (0.56 s, 5, 104) | 0.03417 | 0.1025 | 0.4888 (490 ms) |
| 95% | cpu | 124 | 0 | 0.241 (0.56 s, 5, 104) | 0.5722 | 2.04 | 11.58 (490 ms) |
| 95% | fargate | 124 | 0 | 0.01428 (0.56 s, 5, 104) | 0.03417 | 0.1025 | 0.4888 (490 ms) |
| 99% | cpu | 124 | 0 | 0.6992 (1.84 s, 8, 84) | 1.071 | 2.515 | 11.93 (1569 ms) |
| 99% | fargate | 124 | 0 | 0.06787 (1.84 s, 7, 92) | 0.1148 | 0.2285 | 0.6095 (1569 ms) |
