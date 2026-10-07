# Per-cell paired deltas (candidate/base − 1)

Paired medians from clean pairs; the interval is the descriptive bootstrap median interval. Superseded tables are the first port (d30d286) with specialized subset callbacks.

### full-5

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 5 | -0.06% | -0.51…+0.77% | -0.51…+0.77% | pass |
| wave | 100 | 0 | 5 | -0.79% | -1.94…-0.11% | -1.94…-0.11% | pass |
| inbox | 100 | 0 | 5 | +0.07% | -0.76…+0.29% | -0.76…+0.29% | pass |
| private | 100 | 0 | 5 | -0.12% | -1.21…+0.81% | -1.21…+0.81% | pass |
| private-remote | 100 | 0 | 5 | -0.02% | -1.00…+0.96% | -1.00…+0.96% | pass |
| tree | 100 | 0 | 5 | +0.87% | -1.62…+1.14% | -1.62…+1.14% | pass |
| tree-wait | 100 | 0 | 5 | -0.19% | -2.45…+4.93% | -2.45…+4.93% | pass |
| tree-cont | 100 | 0 | 5 | -0.41% | -1.10…+0.97% | -1.10…+0.97% | pass |
| tree-pipelined | 100 | 0 | 5 | -0.80% | -2.45…+0.92% | -2.45…+0.92% | pass |
| dtor | 100 | 0 | 5 | +0.26% | +0.11…+1.06% | +0.11…+1.06% | pass |
| subset | 1 | 0 | 5 | +0.69% | -0.42…+2.13% | -0.42…+2.13% | pass |
| subset | 10 | 0 | 5 | -1.71% | -1.84…+1.20% | -1.84…+1.20% | pass |
| subset | 100 | 0 | 5 | +3.31% | +0.09…+5.14% | +0.09…+5.14% | pass |
| subset-whole | 100 | 0 | 5 | +17.52% | +2.42…+20.02% | +2.42…+20.02% | pass |
| subset-whole-unreserved | 100 | 0 | 5 | +16.26% | +14.57…+18.90% | +14.57…+18.90% | pass |
| tree-cancel | 100 | 0 | 5 | +0.80% | +0.11…+1.06% | +0.11…+1.06% | pass |
| private-tracked | 100 | 0 | 5 | -0.36% | -4.44…-0.11% | -4.44…-0.11% | inconclusive |
| actor | 100 | 6 | 5 | -0.08% | -0.48…+11.02% | -0.48…+11.02% | pass |
| wave | 100 | 6 | 5 | +0.07% | -0.21…+0.46% | -0.21…+0.46% | pass |
| inbox | 100 | 6 | 5 | +0.06% | -0.03…+0.61% | -0.03…+0.61% | pass |
| private | 100 | 6 | 5 | -0.95% | -1.88…-0.42% | -1.88…-0.42% | pass |
| private-remote | 100 | 6 | 5 | +0.07% | -0.13…+0.18% | -0.13…+0.18% | pass |
| tree | 100 | 6 | 5 | -0.24% | -1.23…+0.21% | -1.23…+0.21% | pass |
| tree-wait | 100 | 6 | 5 | -0.49% | -1.06…+0.19% | -1.06…+0.19% | pass |
| tree-cont | 100 | 6 | 5 | -0.28% | -0.47…+0.31% | -0.47…+0.31% | pass |
| tree-pipelined | 100 | 6 | 5 | -0.35% | -0.75…+0.20% | -0.75…+0.20% | pass |
| dtor | 100 | 6 | 5 | -0.15% | -0.19…+0.09% | -0.19…+0.09% | pass |
| subset | 1 | 6 | 5 | -0.12% | -0.36…+0.14% | -0.36…+0.14% | pass |
| subset | 10 | 6 | 5 | -0.16% | -1.83…+2.60% | -1.83…+2.60% | pass |
| subset | 100 | 6 | 5 | -0.92% | -4.08…+1.29% | -4.08…+1.29% | inconclusive |
| subset-whole | 100 | 6 | 5 | +1.71% | -0.31…+1.83% | -0.31…+1.83% | pass |
| subset-whole-unreserved | 100 | 6 | 5 | +1.66% | +1.33…+1.69% | +1.33…+1.69% | pass |
| tree-cancel | 100 | 6 | 5 | -0.30% | -1.06…+0.02% | -1.06…+0.02% | pass |
| private-tracked | 100 | 6 | 5 | -0.33% | -2.40…+0.24% | -2.40…+0.24% | pass |

### boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree-wait | 100 | 0 | 20 | +0.40% | -0.14…+2.19% | -2.44…+4.54% | pass |
| tree-pipelined | 100 | 0 | 20 | +1.42% | +1.05…+1.71% | +0.10…+14.94% | pass |
| private-tracked | 100 | 0 | 20 | -1.27% | -1.51…-1.09% | -2.67…+3.74% | pass |

### boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| subset | 100 | 6 | 20 | +0.32% | -0.82…+1.28% | -4.51…+4.85% | pass |
| private-tracked | 100 | 6 | 20 | -0.38% | -0.51…-0.11% | -0.74…+0.18% | pass |

### control-boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree-wait | 100 | 0 | 20 | -0.30% | -1.46…+0.68% | -10.59…+12.42% | pass |
| tree-pipelined | 100 | 0 | 20 | -0.19% | -0.36…+0.15% | -5.42…+0.24% | pass |
| private-tracked | 100 | 0 | 20 | +0.27% | -0.05…+0.48% | -0.46…+1.66% | pass |

### control-boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| subset | 100 | 6 | 20 | +0.79% | -0.46…+2.80% | -8.59…+5.59% | pass |
| private-tracked | 100 | 6 | 20 | +0.16% | -0.18…+0.32% | -9.93…+0.94% | pass |

### superseded-d30d286-full-5

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 5 | +0.51% | -0.06…+0.82% | -0.06…+0.82% | pass |
| wave | 100 | 0 | 5 | +0.61% | +0.06…+1.03% | +0.06…+1.03% | pass |
| inbox | 100 | 0 | 5 | +0.29% | -0.87…+0.93% | -0.87…+0.93% | pass |
| private | 100 | 0 | 5 | -0.09% | -2.01…+0.57% | -2.01…+0.57% | pass |
| private-remote | 100 | 0 | 5 | -0.39% | -1.49…+1.82% | -1.49…+1.82% | pass |
| tree | 100 | 0 | 5 | +1.97% | -0.98…+5.65% | -0.98…+5.65% | pass |
| tree-wait | 100 | 0 | 5 | +1.14% | +0.45…+1.99% | +0.45…+1.99% | pass |
| tree-cont | 100 | 0 | 5 | -0.53% | -1.35…+2.02% | -1.35…+2.02% | pass |
| tree-pipelined | 100 | 0 | 5 | +0.14% | -4.17…+2.35% | -4.17…+2.35% | inconclusive |
| dtor | 100 | 0 | 5 | +0.39% | -2.33…+1.39% | -2.33…+1.39% | pass |
| subset | 1 | 0 | 5 | +8.00% | +6.77…+14.22% | +6.77…+14.22% | pass |
| subset | 10 | 0 | 5 | +11.83% | +11.43…+17.67% | +11.43…+17.67% | pass |
| subset | 100 | 0 | 5 | +12.54% | +8.30…+12.67% | +8.30…+12.67% | pass |
| subset-whole | 100 | 0 | 5 | +17.96% | +16.91…+20.29% | +16.91…+20.29% | pass |
| subset-whole-unreserved | 100 | 0 | 5 | +18.18% | +16.88…+18.82% | +16.88…+18.82% | pass |
| tree-cancel | 100 | 0 | 5 | +1.67% | +0.95…+10.12% | +0.95…+10.12% | pass |
| private-tracked | 100 | 0 | 5 | -0.02% | -0.11…+0.92% | -0.11…+0.92% | pass |
| actor | 100 | 6 | 5 | +0.10% | -0.44…+0.35% | -0.44…+0.35% | pass |
| wave | 100 | 6 | 5 | -0.14% | -0.44…+0.10% | -0.44…+0.10% | pass |
| inbox | 100 | 6 | 5 | -0.11% | -0.25…+0.36% | -0.25…+0.36% | pass |
| private | 100 | 6 | 5 | -0.17% | -0.44…+0.38% | -0.44…+0.38% | pass |
| private-remote | 100 | 6 | 5 | -0.00% | -0.67…+0.17% | -0.67…+0.17% | pass |
| tree | 100 | 6 | 5 | -0.28% | -0.69…+0.34% | -0.69…+0.34% | pass |
| tree-wait | 100 | 6 | 5 | -0.55% | -0.65…-0.13% | -0.65…-0.13% | pass |
| tree-cont | 100 | 6 | 5 | +0.01% | -2.84…+0.47% | -2.84…+0.47% | pass |
| tree-pipelined | 100 | 6 | 5 | -0.33% | -2.64…+0.88% | -2.64…+0.88% | pass |
| dtor | 100 | 6 | 5 | +0.05% | -0.47…+1.42% | -0.47…+1.42% | pass |
| subset | 1 | 6 | 5 | +0.05% | -0.20…+0.12% | -0.20…+0.12% | pass |
| subset | 10 | 6 | 5 | +1.17% | -0.38…+1.36% | -0.38…+1.36% | pass |
| subset | 100 | 6 | 5 | -6.77% | -7.04…-2.01% | -7.04…-2.01% | inconclusive |
| subset-whole | 100 | 6 | 5 | +0.13% | -1.93…+1.88% | -1.93…+1.88% | pass |
| subset-whole-unreserved | 100 | 6 | 5 | +1.66% | +1.44…+1.93% | +1.44…+1.93% | pass |
| tree-cancel | 100 | 6 | 5 | -0.42% | -0.85…+0.80% | -0.85…+0.80% | pass |
| private-tracked | 100 | 6 | 5 | -0.04% | -0.08…+0.58% | -0.08…+0.58% | pass |

### superseded-d30d286-boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| subset | 100 | 6 | 20 | -3.68% | -5.01…-3.19% | -8.40…+1.63% | regression |
| tree-cont | 100 | 6 | 20 | +0.91% | +0.73…+1.25% | +0.02…+1.74% | pass |
| tree-pipelined | 100 | 6 | 20 | +0.87% | +0.65…+1.31% | -1.89…+1.54% | pass |

