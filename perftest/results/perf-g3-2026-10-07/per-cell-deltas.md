# Per-cell paired deltas (candidate/base − 1)

Candidate is the gated P1d commit `1091472` (not landed); base is `daad3d9`. Paired medians from clean pairs; the interval is the descriptive bootstrap median interval. Control runs compare two independent, byte-identical base builds.

### full-5

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 5 | -3.02% | -3.62…-2.92% | -3.62…-2.92% | inconclusive |
| wave | 100 | 0 | 5 | -1.07% | -1.80…-0.66% | -1.80…-0.66% | pass |
| inbox | 100 | 0 | 5 | +0.15% | -0.02…+0.67% | -0.02…+0.67% | pass |
| private | 100 | 0 | 5 | -1.05% | -2.66…-0.33% | -2.66…-0.33% | pass |
| private-remote | 100 | 0 | 5 | +0.24% | -0.47…+0.90% | -0.47…+0.90% | pass |
| tree | 100 | 0 | 5 | -5.05% | -6.98…-2.46% | -6.98…-2.46% | inconclusive |
| tree-wait | 100 | 0 | 5 | -4.20% | -8.54…-2.92% | -8.54…-2.92% | inconclusive |
| tree-cont | 100 | 0 | 5 | -0.39% | -0.76…+1.98% | -0.76…+1.98% | pass |
| tree-pipelined | 100 | 0 | 5 | +0.16% | -1.62…+0.78% | -1.62…+0.78% | pass |
| dtor | 100 | 0 | 5 | -0.48% | -1.69…-0.28% | -1.69…-0.28% | pass |
| subset | 1 | 0 | 5 | -1.15% | -2.35…+11.20% | -2.35…+11.20% | pass |
| subset | 10 | 0 | 5 | -2.85% | -4.32…-2.13% | -4.32…-2.13% | inconclusive |
| subset | 100 | 0 | 5 | -2.49% | -5.35…-0.21% | -5.35…-0.21% | inconclusive |
| subset-whole | 100 | 0 | 5 | -9.29% | -19.76…+1.73% | -19.76…+1.73% | inconclusive |
| subset-whole-unreserved | 100 | 0 | 5 | -10.03% | -12.92…+1.72% | -12.92…+1.72% | inconclusive |
| tree-cancel | 100 | 0 | 5 | -1.18% | -4.18…+0.82% | -4.18…+0.82% | inconclusive |
| private-tracked | 100 | 0 | 5 | -0.30% | -3.63…+0.10% | -3.63…+0.10% | inconclusive |
| actor | 100 | 6 | 5 | -0.27% | -0.70…+0.25% | -0.70…+0.25% | pass |
| wave | 100 | 6 | 5 | +0.06% | -0.53…+0.35% | -0.53…+0.35% | pass |
| inbox | 100 | 6 | 5 | -0.31% | -1.01…+0.11% | -1.01…+0.11% | pass |
| private | 100 | 6 | 5 | -1.08% | -1.59…-0.20% | -1.59…-0.20% | pass |
| private-remote | 100 | 6 | 5 | +0.51% | +0.39…+0.53% | +0.39…+0.53% | pass |
| tree | 100 | 6 | 5 | -1.05% | -1.98…-0.60% | -1.98…-0.60% | pass |
| tree-wait | 100 | 6 | 5 | -1.56% | -1.64…-0.78% | -1.64…-0.78% | pass |
| tree-cont | 100 | 6 | 5 | -1.33% | -1.41…-0.94% | -1.41…-0.94% | pass |
| tree-pipelined | 100 | 6 | 5 | +0.09% | -0.73…+0.16% | -0.73…+0.16% | pass |
| dtor | 100 | 6 | 5 | -0.20% | -0.50…+0.15% | -0.50…+0.15% | pass |
| subset | 1 | 6 | 5 | -0.84% | -1.21…-0.75% | -1.21…-0.75% | pass |
| subset | 10 | 6 | 5 | -0.07% | -0.20…+3.80% | -0.20…+3.80% | pass |
| subset | 100 | 6 | 5 | -0.35% | -7.00…+2.95% | -7.00…+2.95% | inconclusive |
| subset-whole | 100 | 6 | 5 | +0.23% | +0.15…+0.27% | +0.15…+0.27% | pass |
| subset-whole-unreserved | 100 | 6 | 5 | +0.25% | +0.13…+0.66% | +0.13…+0.66% | pass |
| tree-cancel | 100 | 6 | 5 | +0.19% | -1.04…+0.45% | -1.04…+0.45% | pass |
| private-tracked | 100 | 6 | 5 | +0.04% | -0.28…+0.21% | -0.28…+0.21% | pass |

### boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 20 | -3.04% | -3.37…-2.42% | -3.72…-1.61% | inconclusive |
| private | 100 | 0 | 20 | -1.06% | -1.38…-0.64% | -4.19…+0.51% | pass |
| tree | 100 | 0 | 20 | -4.09% | -5.37…-2.95% | -8.70…+3.53% | inconclusive |
| tree-wait | 100 | 0 | 20 | -3.60% | -6.62…-2.41% | -12.45…-0.62% | inconclusive |
| subset | 1 | 0 | 20 | -13.34% | -15.21…-12.67% | -18.55…-7.88% | regression |
| subset | 10 | 0 | 20 | -15.64% | -16.28…-15.22% | -20.53…-7.56% | regression |
| subset | 100 | 0 | 20 | -13.91% | -14.74…-13.23% | -19.06…-9.92% | regression |
| subset-whole | 100 | 0 | 20 | -10.38% | -10.76…-10.17% | -12.51…+2.47% | regression |
| subset-whole-unreserved | 100 | 0 | 20 | -10.08% | -10.77…-9.64% | -20.97…+2.04% | regression |
| tree-cancel | 100 | 0 | 20 | -1.37% | -1.65…-1.11% | -3.24…+1.01% | pass |
| private-tracked | 100 | 0 | 20 | -0.54% | -0.86…-0.35% | -1.39…+0.46% | pass |

### boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree | 100 | 6 | 20 | -1.87% | -2.11…-1.09% | -2.82…+0.46% | pass |
| subset | 100 | 6 | 20 | -0.57% | -1.37…-0.19% | -6.50…+4.19% | pass |

### control-boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 20 | +0.58% | +0.30…+0.70% | -0.62…+1.16% | pass |
| private | 100 | 0 | 20 | -0.51% | -0.73…-0.13% | -1.54…+2.69% | pass |
| tree | 100 | 0 | 20 | -0.50% | -1.43…+0.55% | -4.38…+3.71% | pass |
| tree-wait | 100 | 0 | 20 | +0.95% | -1.76…+1.63% | -9.74…+10.96% | pass |
| subset | 1 | 0 | 20 | -3.06% | -3.54…-2.30% | -8.08…+0.91% | inconclusive |
| subset | 10 | 0 | 20 | -1.52% | -2.02…-0.62% | -8.12…+3.56% | pass |
| subset | 100 | 0 | 20 | -0.12% | -0.45…+0.28% | -3.81…+3.72% | pass |
| subset-whole | 100 | 0 | 20 | -0.06% | -0.26…+0.31% | -12.56…+14.50% | pass |
| subset-whole-unreserved | 100 | 0 | 20 | +0.03% | -0.56…+0.44% | -12.62…+13.85% | pass |
| tree-cancel | 100 | 0 | 20 | +0.74% | +0.12…+1.24% | -0.88…+4.78% | pass |
| private-tracked | 100 | 0 | 20 | -0.20% | -0.56…-0.05% | -1.06…+0.69% | pass |

### control-boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree | 100 | 6 | 20 | +0.52% | +0.33…+0.77% | -0.59…+1.27% | pass |
| subset | 100 | 6 | 20 | +0.57% | -0.48…+2.06% | -3.56…+8.36% | pass |

