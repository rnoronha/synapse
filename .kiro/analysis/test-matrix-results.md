# Test Matrix Results — Final Run (Fixed Assertions)

**Date:** 2026-05-03
**Artifacts:** master@37cb55c2f, g3@0224ac41e, phase2@1b85ac5cd
**Instance type:** c5.4xlarge (16 vCPU, 32 GiB)

## Matrix

| Test | master | g3 | phase2 |
|------|--------|-----|--------|
| **pathological** | ⚠️ 14/16 (2 vacuous) | ⚠️ 13/16 (2 vacuous+1 prop) | ⚠️ 14/16 (2 vacuous) |
| **soak (10min)** | ✅ 0 errors, p99 3.4/5.9ms | ✅ 0 errors, p99 3.4/5.9ms | ✅ 0 errors, p99 3.2/4.3ms |
| **throughput** | ✅ 1.07x | ✅ 1.06x | ✅ 4.22x |
| **correctness** | ✅ 26/26 | ✅ 26/26 | ❌ 29/30 (del_node deploy issue) |
| **recovery** | ✅ N/A (correct skip) | ✅ N/A (correct skip) | ⚠️ 7/10 (kill mechanism) |
| **parallel-reads (64)** | ❌ script bug (fixed) | ❌ script bug (fixed) | ✅ 831 QPS, p99 65ms |
| **mixed-load (10% writes)** | ✅ 0 errors, r:27ms w:1008ms | ✅ 0 errors, r:27ms w:997ms | ✅ 0 errors, r:14ms w:351ms |

## Key Findings

### G3 patches have zero performance impact
- Throughput: 1.07x vs 1.06x (within noise)
- Soak latency: identical (p99 3.4ms reads, 5.9ms writes)
- Mixed load: identical (r:27ms, w:997ms vs r:27ms, w:1008ms)

### Multi-process delivers 2-4x improvement
- Throughput: 4.22x speedup (vs 1.07x single-process)
- Mixed load reads: 14ms vs 27ms (1.9x)
- Mixed load writes: 351ms vs 1008ms (2.8x)
- Soak p99: 3.2ms vs 3.4ms reads, 4.3ms vs 5.9ms writes

### Known issues
1. **Pathological tests 6-7 vacuous** — yield budget tests seed data but interval/geo queries return 0 nodes (broken seed data across all branches)
2. **del_node readonly guard** — fix exists in source but doesn't deploy to EC2 (pip install issue)
3. **Recovery kill mechanism** — PID lookup unreliable, SIGKILL doesn't always work within 5s timeout
4. **Parallel-reads on single-process** — deploy-and-test.sh passed concurrency=0 (fixed)
