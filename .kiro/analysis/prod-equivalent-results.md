# Prod-Equivalent Load Test: r7a.24xlarge (96 cores, 48 readers)

**Date:** 2026-05-03
**Instance:** r7a.24xlarge (96 vCPU, 768 GiB) — matches Vertex gamma/prod
**Dataset:** 311K nodes (EBS snapshot)
**Branch:** phase2-multi-process

## Results vs c5.4xlarge (16 cores, 8 readers)

| Test | c5.4xlarge | r7a.24xlarge | Scaling |
|------|-----------|-------------|---------|
| parallel-reads QPS | ~900 | 1169 | 1.3x |
| parallel-reads p50 | 190ms | 48ms | **4x better** |
| throughput speedup | 3.78x | 3.68x | same |
| mixed-load ops/sec | 749 | 1633 | **2.2x** |
| mixed-load read p99 | 147ms | 23ms | **6.4x better** |
| read-after-write | 100%, 364ms | 100%, 365ms | identical |

## Conclusions

1. Latency scales better than throughput with more cores
2. 48 readers are underutilized by 64 test clients — need higher concurrency to saturate
3. Architectural efficiency (3.7x speedup) is stable across hardware
4. Read-after-write consistency is hardware-independent
