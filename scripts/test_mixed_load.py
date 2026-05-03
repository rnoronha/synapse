#!/usr/bin/env python3.11
"""
Mixed read/write concurrent load benchmark for Synapse Cortex.

Sustains --concurrency simultaneous operations for --duration seconds,
with each operation randomly chosen as a read or write based on --write-pct.
Reports per-type latency percentiles and optional JSON output.
"""
import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time

log = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

READ_QUERIES = [
    'inet:fqdn | limit 10',
    'inet:ipv4 | limit 10',
    'inet:fqdn:zone=com | limit 5',
]

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


def _percentile(data, pct):
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[-1]
    return data[f] + (k - f) * (data[c] - data[f])


def _make_write_query():
    ts = int(time.time())
    r = random.randint(0, 0xFFFFFF)
    if random.random() < 0.5:
        return f'[inet:fqdn=mixed-{ts}-{r:06x}.test.com]'
    a, bc = divmod(r, 256 * 256)
    b, c = divmod(bc, 256)
    return f'[inet:ipv4=10.{a % 256}.{b}.{c}]'


class Stats:
    __slots__ = ('read_lats', 'write_lats', 'errors', '_lock')

    def __init__(self):
        self.read_lats: list[float] = []
        self.write_lats: list[float] = []
        self.errors = 0
        self._lock = asyncio.Lock()

    async def record(self, kind, latency):
        async with self._lock:
            (self.read_lats if kind == 'read' else self.write_lats).append(latency)

    async def record_error(self):
        async with self._lock:
            self.errors += 1

    async def snapshot(self):
        async with self._lock:
            rl, wl = sorted(self.read_lats), sorted(self.write_lats)
            self.read_lats.clear()
            self.write_lats.clear()
            return rl, wl


async def _seed_nodes(prox, count=500):
    """Seed inet:fqdn nodes if fewer than `count` exist."""
    existing = 0
    async for mesg in prox.storm('inet:fqdn | count'):
        if mesg[0] == 'print' and mesg[1].get('mesg', '').isdigit():
            existing = int(mesg[1]['mesg'])

    if existing >= count:
        print(f'  inet:fqdn: {existing} exist (>= {count}), skipping')
        return

    need = count - existing
    print(f'  inet:fqdn: seeding {need} nodes...', end=' ', flush=True)
    for start in range(0, need, 50):
        batch = ' '.join(
            f'[inet:fqdn=seed{i}.bench.mixed.com]'
            for i in range(existing + start, min(existing + start + 50, existing + need))
        )
        async for _ in prox.storm(batch):
            pass
    print('done')


async def _run_op(prox, sem, stats, write_pct):
    """Run a single random read or write operation under the semaphore."""
    async with sem:
        is_write = random.randint(1, 100) <= write_pct
        query = _make_write_query() if is_write else random.choice(READ_QUERIES)
        kind = 'write' if is_write else 'read'
        t0 = time.monotonic()
        try:
            async for _ in prox.storm(query):
                pass
            await stats.record(kind, time.monotonic() - t0)
        except Exception:
            log.warning('Op %s failed: %s', kind, query, exc_info=True)
            await stats.record_error()


def _fmt_lat(v):
    return f'{v * 1000:.1f}ms'


def _print_status(elapsed, total, reads, writes, rl, wl, errors):
    rp50 = _fmt_lat(_percentile(rl, 50)) if rl else 'n/a'
    wp50 = _fmt_lat(_percentile(wl, 50)) if wl else 'n/a'
    print(
        f'[{elapsed:>6.0f}s] total={total:<8} reads={reads:<8} writes={writes:<8} '
        f'read_p50={rp50:<10} write_p50={wp50:<10} errors={errors}',
        flush=True,
    )


async def main():
    parser = argparse.ArgumentParser(
        description='Mixed read/write concurrent load benchmark for Synapse Cortex.',
        epilog=(
            'Example runs:\n'
            '  %(prog)s tcp://host:port/cortex --write-pct 0   # pure reads\n'
            '  %(prog)s tcp://host:port/cortex --write-pct 10  # 90/10\n'
            '  %(prog)s tcp://host:port/cortex --write-pct 25  # 75/25\n'
            '  %(prog)s tcp://host:port/cortex --write-pct 50  # 50/50\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('url', help='Telepath URL (e.g. tcp://host:port/cortex)')
    parser.add_argument('--concurrency', type=int, default=64,
                        help='Max concurrent operations (default: 64)')
    parser.add_argument('--write-pct', type=int, default=10, choices=range(0, 101),
                        metavar='0-100',
                        help='Percentage of operations that are writes (default: 10)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Test duration in seconds (default: 60)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    parser.add_argument('--max-error-pct', type=float, default=1.0,
                        help='Max error rate %% before FAIL (default: 1.0)')
    parser.add_argument('--max-p99-ms', type=float, default=None,
                        help='Max p99 latency (ms) before FAIL (default: no limit)')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    sem = asyncio.Semaphore(args.concurrency)
    stats = Stats()
    all_read_lats: list[float] = []
    all_write_lats: list[float] = []
    total_reads = 0
    total_writes = 0

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Concurrency: {args.concurrency}  Write%: {args.write_pct}  '
              f'Duration: {args.duration}s')

        print('\nSeeding data:')
        await _seed_nodes(prox)
        print()

        t_start = time.monotonic()
        next_status = t_start + 10
        pending: set[asyncio.Task] = set()

        while not _shutdown.is_set():
            elapsed = time.monotonic() - t_start
            if elapsed >= args.duration:
                break

            # Status update every 10s
            if time.monotonic() >= next_status:
                rl, wl = await stats.snapshot()
                all_read_lats.extend(rl)
                all_write_lats.extend(wl)
                total_reads += len(rl)
                total_writes += len(wl)
                _print_status(elapsed, total_reads + total_writes,
                              total_reads, total_writes, rl, wl, stats.errors)
                next_status = time.monotonic() + 10

            task = asyncio.create_task(_run_op(prox, sem, stats, args.write_pct))
            pending.add(task)
            task.add_done_callback(pending.discard)

            # Yield to let tasks run; throttle task creation when at capacity
            if len(pending) >= args.concurrency:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED)

        # Drain remaining tasks
        if pending:
            await asyncio.wait(pending)

        elapsed = time.monotonic() - t_start

    # Flush remaining latencies
    rl, wl = await stats.snapshot()
    all_read_lats.extend(rl)
    all_write_lats.extend(wl)
    total_reads += len(rl)
    total_writes += len(wl)

    all_read_lats.sort()
    all_write_lats.sort()
    total_ops = total_reads + total_writes
    ops_sec = total_ops / elapsed if elapsed > 0 else 0

    def _lat_stats(lats):
        if not lats:
            return {}
        return {
            'count': len(lats),
            'p50_ms': round(_percentile(lats, 50) * 1000, 2),
            'p99_ms': round(_percentile(lats, 99) * 1000, 2),
            'p999_ms': round(_percentile(lats, 99.9) * 1000, 2),
            'min_ms': round(lats[0] * 1000, 2),
            'max_ms': round(lats[-1] * 1000, 2),
        }

    report = {
        'params': {
            'url': args.url,
            'concurrency': args.concurrency,
            'write_pct': args.write_pct,
            'duration': args.duration,
        },
        'elapsed_s': round(elapsed, 1),
        'total_ops': total_ops,
        'ops_per_sec': round(ops_sec, 1),
        'reads': total_reads,
        'writes': total_writes,
        'errors': stats.errors,
        'read_latency': _lat_stats(all_read_lats),
        'write_latency': _lat_stats(all_write_lats),
    }

    # Final summary
    print('\n' + '=' * 60)
    print('MIXED LOAD BENCHMARK COMPLETE')
    print('=' * 60)
    print(f'Duration:  {elapsed:.1f}s')
    print(f'Total ops: {total_ops}  ({ops_sec:.1f} ops/sec)')
    print(f'Reads:     {total_reads}  Writes: {total_writes}  Errors: {stats.errors}')
    for label, key in (('Read', 'read_latency'), ('Write', 'write_latency')):
        lat = report[key]
        if not lat:
            continue
        print(f'\n{label} latency ({lat["count"]} ops):')
        print(f'  p50={lat["p50_ms"]:.1f}ms  p99={lat["p99_ms"]:.1f}ms  '
              f'p999={lat["p999_ms"]:.1f}ms  '
              f'min={lat["min_ms"]:.1f}ms  max={lat["max_ms"]:.1f}ms')

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nJSON written to {args.output}')

    # --- Pass/fail evaluation ---
    failures: list[str] = []

    # Vacuous pass: no ops completed at all
    if total_ops == 0:
        failures.append('FAIL: 0 operations completed (vacuous pass)')

    # Error rate threshold
    if total_ops > 0:
        error_pct = (stats.errors / total_ops) * 100
        if error_pct > args.max_error_pct:
            failures.append(
                f'FAIL: error rate {error_pct:.1f}% exceeds --max-error-pct {args.max_error_pct}%'
            )

    # Latency threshold
    if args.max_p99_ms is not None:
        for label, lats in (('read', all_read_lats), ('write', all_write_lats)):
            if lats:
                p99 = _percentile(lats, 99) * 1000
                if p99 > args.max_p99_ms:
                    failures.append(
                        f'FAIL: {label} p99 {p99:.1f}ms exceeds --max-p99-ms {args.max_p99_ms}ms'
                    )

    if failures:
        print('\n' + '\n'.join(failures))
        print('\nRESULT: FAIL')
        return 1

    print('\nRESULT: PASS')
    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
    sys.exit(asyncio.run(main()))
