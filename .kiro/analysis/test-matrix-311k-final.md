# Final Test Matrix — 311K Node Dataset (EBS Snapshot)

**Date:** 2026-05-03
**Snapshot:** snap-0572436bb7e5d6d78 (311,685 nodes)
**Instance:** c5.4xlarge (16 vCPU, 32 GiB)
**Branches:** master@37cb55c2f, g3@0224ac41e, phase2@c05996e7b

## Results

| Test | master | g3 | phase2 | Δ phase2 vs master |
|------|--------|-----|--------|--------------------|
| pathological | 16/16 (44s) | 16/16 | 16/16 (72s) | +28s routing overhead |
| soak (10min) | 0 err, p99 3.1/6.3ms | 0 err, p99 4.0/5.8ms | 0 err, p99 4.0/5.8ms | identical |
| throughput (20 conc) | 1.07x | 1.07x | **3.78x** | **3.5x improvement** |
| parallel-reads (64 conc) | 259 QPS, p50 258ms | 259 QPS, p50 254ms | **~900 QPS, p50 ~80ms** | **3.5x QPS, 3.2x latency** |
| mixed-load (10% writes) | r:32ms w:1060ms | r:30ms w:960ms | **r:14ms w:355ms** | **2.3x read, 3x write** |
| correctness | 26/26 (single) | 26/26 (single) | **30/30** (reader+writer) | full readonly verified |

## Key Conclusions

1. **G3 safety patches: zero performance impact** — master ≈ g3 across all tests
2. **Multi-process: 3-4x read improvement** — consistent across throughput, parallel-reads, mixed-load
3. **Multi-process: 2-3x write improvement under contention** — writer freed from read load
4. **Zero errors across all soak tests** — all 3 branches stable under sustained load
5. **Full correctness** — 30/30 including del_node readonly guard
6. **311K dataset: results consistent with tiny-dataset** — multi-process advantage holds at scale
