#!/usr/bin/env python3.11
"""
Heavy soak test for multi-process Cortex.

Sustains concurrent read and write workloads at target TPS for hours,
tracking per-operation latency and reporting p50/p99/p999 percentiles.
"""
import argparse
import asyncio
import json
import logging
import os
import random
import signal
import statistics
import sys
import time

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

READ_QUERIES = [
    'inet:fqdn | limit 10',
    'inet:fqdn:zone=com | limit 10',
    'inet:ipv4 | limit 10',
    'inet:url | limit 5',
    'file:bytes | limit 5',
    'inet:fqdn#soak | limit 10',
]

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


def _percentile(data, pct):
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100)
    f, c = int(k), int(k) + 1
    if c >= len(data):
        return data[-1]
    return data[f] + (k - f) * (data[c] - data[f])


def _make_write_query(idx):
    ts = int(time.time())
    kind = idx % 4
    if kind == 0:
        return f'[inet:fqdn=soak-{ts}-{idx}.test.com]'
    if kind == 1:
        a, rem = divmod(idx, 256 * 256)
        b, c = divmod(rem, 256)
        return f'[inet:ipv4=10.{a % 256}.{b % 256}.{c % 256}]'
    if kind == 2:
        return f'[inet:url="https://soak-{ts}.test.com/path/{idx}"]'
    randhex = random.randbytes(32).hex()
    return f'[file:bytes=sha256:{randhex}]'


class Stats:
    __slots__ = ('read_lats', 'write_lats', 'read_errs', 'write_errs',
                 'total_reads', 'total_writes', 'empty_reads', 'empty_writes', '_lock')

    def __init__(self):
        self.read_lats: list[float] = []
        self.write_lats: list[float] = []
        self.read_errs = 0
        self.write_errs = 0
        self.total_reads = 0
        self.total_writes = 0
        self.empty_reads = 0
        self.empty_writes = 0
        self._lock = asyncio.Lock()

    async def record_read(self, latency, count):
        async with self._lock:
            self.read_lats.append(latency)
            self.total_reads += 1
            if count == 0:
                self.empty_reads += 1

    async def record_write(self, latency, count):
        async with self._lock:
            self.write_lats.append(latency)
            self.total_writes += 1
            if count == 0:
                self.empty_writes += 1

    async def record_read_err(self):
        async with self._lock:
            self.read_errs += 1
            self.total_reads += 1

    async def record_write_err(self):
        async with self._lock:
            self.write_errs += 1
            self.total_writes += 1

    async def snapshot_and_reset(self):
        async with self._lock:
            rl, wl = sorted(self.read_lats), sorted(self.write_lats)
            self.read_lats.clear()
            self.write_lats.clear()
            return rl, wl

    def summary(self):
        return {
            'total_reads': self.total_reads,
            'total_writes': self.total_writes,
            'read_errors': self.read_errs,
            'write_errors': self.write_errs,
            'empty_reads': self.empty_reads,
            'empty_writes': self.empty_writes,
        }


async def _read_loop(prox, tps, stats):
    interval = 1.0 / tps if tps > 0 else 1.0
    idx = 0
    while not _shutdown.is_set():
        query = READ_QUERIES[idx % len(READ_QUERIES)]
        idx += 1
        t0 = time.monotonic()
        try:
            count = 0
            async for _ in prox.storm(query):
                count += 1
            await stats.record_read(time.monotonic() - t0, count)
        except Exception as e:
            logger.warning('read error on query %r: %s', query, e)
            await stats.record_read_err()
        elapsed = time.monotonic() - t0
        sleep = interval - elapsed
        if sleep > 0:
            await asyncio.sleep(sleep)


async def _write_loop(prox, tps, stats):
    interval = 1.0 / tps if tps > 0 else 1.0
    idx = 0
    while not _shutdown.is_set():
        query = _make_write_query(idx)
        idx += 1
        t0 = time.monotonic()
        try:
            count = 0
            async for _ in prox.storm(query):
                count += 1
            await stats.record_write(time.monotonic() - t0, count)
        except Exception as e:
            logger.warning('write error on query %r: %s', query, e)
            await stats.record_write_err()
        elapsed = time.monotonic() - t0
        sleep = interval - elapsed
        if sleep > 0:
            await asyncio.sleep(sleep)


def _fmt_lat(v):
    return f'{v * 1000:.1f}ms'


def _print_status(elapsed, stats, rl, wl):
    rp50 = _fmt_lat(_percentile(rl, 50)) if rl else 'n/a'
    rp99 = _fmt_lat(_percentile(rl, 99)) if rl else 'n/a'
    rp999 = _fmt_lat(_percentile(rl, 99.9)) if rl else 'n/a'
    wp50 = _fmt_lat(_percentile(wl, 50)) if wl else 'n/a'
    wp99 = _fmt_lat(_percentile(wl, 99)) if wl else 'n/a'
    wp999 = _fmt_lat(_percentile(wl, 99.9)) if wl else 'n/a'
    s = stats.summary()
    print(
        f'[{elapsed:>7.0f}s] '
        f'reads={s["total_reads"]:>8} errs={s["read_errors"]:>5} '
        f'p50={rp50:>9} p99={rp99:>9} p999={rp999:>9} | '
        f'writes={s["total_writes"]:>8} errs={s["write_errors"]:>5} '
        f'p50={wp50:>9} p99={wp99:>9} p999={wp999:>9}',
        flush=True,
    )


async def _reporter(stats, duration, t_start, all_read_lats, all_write_lats):
    while not _shutdown.is_set():
        await asyncio.sleep(60)
        rl, wl = await stats.snapshot_and_reset()
        all_read_lats.extend(rl)
        all_write_lats.extend(wl)
        elapsed = time.monotonic() - t_start
        _print_status(elapsed, stats, rl, wl)
        if elapsed >= duration:
            _shutdown.set()
            break


def _build_final_report(params, stats, all_read_lats, all_write_lats, elapsed):
    all_read_lats.sort()
    all_write_lats.sort()

    def _lat_stats(lats):
        if not lats:
            return {}
        return {
            'count': len(lats),
            'min_ms': round(lats[0] * 1000, 2),
            'p50_ms': round(_percentile(lats, 50) * 1000, 2),
            'p99_ms': round(_percentile(lats, 99) * 1000, 2),
            'p999_ms': round(_percentile(lats, 99.9) * 1000, 2),
            'max_ms': round(lats[-1] * 1000, 2),
            'mean_ms': round(statistics.mean(lats) * 1000, 2),
        }

    s = stats.summary()
    actual_read_tps = len(all_read_lats) / elapsed if elapsed > 0 else 0
    actual_write_tps = len(all_write_lats) / elapsed if elapsed > 0 else 0

    return {
        'params': params,
        'elapsed_s': round(elapsed, 1),
        **s,
        'actual_read_tps': round(actual_read_tps, 2),
        'actual_write_tps': round(actual_write_tps, 2),
        'read_latency': _lat_stats(all_read_lats),
        'write_latency': _lat_stats(all_write_lats),
    }


def _evaluate(report, args):
    '''Check pass/fail criteria. Returns list of failure messages.'''
    failures = []
    total_ops = report['total_reads'] + report['total_writes']
    total_errs = report['read_errors'] + report['write_errors']

    # Error rate check
    if total_ops > 0:
        err_pct = (total_errs / total_ops) * 100
        if err_pct > args.max_error_pct:
            failures.append(f'error rate {err_pct:.2f}% exceeds {args.max_error_pct}%')
    elif total_ops == 0:
        failures.append('zero operations completed — test did not run')

    # p99 latency check
    for label, key in (('read', 'read_latency'), ('write', 'write_latency')):
        lat = report.get(key, {})
        p99 = lat.get('p99_ms', 0)
        if p99 > args.max_p99_ms:
            failures.append(f'{label} p99 {p99:.1f}ms exceeds {args.max_p99_ms}ms')

    # Actual TPS vs target check
    elapsed = report['elapsed_s']
    if elapsed > 0:
        target_read = args.read_tps
        target_write = args.write_tps
        for label, actual, target in (('read', report['actual_read_tps'], target_read),
                                      ('write', report['actual_write_tps'], target_write)):
            if target > 0:
                achieved_pct = (actual / target) * 100
                if achieved_pct < args.min_tps_pct:
                    failures.append(f'{label} TPS {actual:.1f} is {achieved_pct:.0f}% of target {target} (min {args.min_tps_pct}%)')

    # Vacuous pass detection
    if report['empty_reads'] > 0:
        empty_pct = (report['empty_reads'] / max(report['total_reads'], 1)) * 100
        if empty_pct > 50:
            failures.append(f'SUSPICIOUS: {empty_pct:.0f}% of reads returned 0 results ({report["empty_reads"]}/{report["total_reads"]})')
    if report['empty_writes'] > 0:
        empty_pct = (report['empty_writes'] / max(report['total_writes'], 1)) * 100
        if empty_pct > 10:
            failures.append(f'SUSPICIOUS: {empty_pct:.0f}% of writes returned 0 results ({report["empty_writes"]}/{report["total_writes"]})')

    return failures


def _print_final(report):
    print('\n' + '=' * 60)
    print('SOAK TEST COMPLETE')
    print('=' * 60)
    print(f'Duration: {report["elapsed_s"]:.1f}s')
    print(f'Total reads:  {report["total_reads"]:>10}  errors: {report["read_errors"]}  empty: {report["empty_reads"]}')
    print(f'Total writes: {report["total_writes"]:>10}  errors: {report["write_errors"]}  empty: {report["empty_writes"]}')
    print(f'Actual TPS:   read={report["actual_read_tps"]:.1f}  write={report["actual_write_tps"]:.1f}')
    for label, key in (('Read', 'read_latency'), ('Write', 'write_latency')):
        lat = report.get(key, {})
        if not lat:
            continue
        print(f'\n{label} latency ({lat["count"]} ops):')
        print(f'  min={lat["min_ms"]:.1f}ms  p50={lat["p50_ms"]:.1f}ms  '
              f'p99={lat["p99_ms"]:.1f}ms  p999={lat["p999_ms"]:.1f}ms  '
              f'max={lat["max_ms"]:.1f}ms  mean={lat["mean_ms"]:.1f}ms')


async def main():
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(
        description='Heavy soak test for multi-process Cortex.')
    parser.add_argument('url', help='Telepath URL (e.g. tcp://host:port/cortex)')
    parser.add_argument('--duration', type=int, default=10800,
                        help='Test duration in seconds (default: 10800 = 3h)')
    parser.add_argument('--read-tps', type=int, default=100,
                        help='Target read operations per second (default: 100)')
    parser.add_argument('--write-tps', type=int, default=30,
                        help='Target write operations per second (default: 30)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    parser.add_argument('--warmup', type=int, default=60,
                        help='Warmup period in seconds excluded from stats (default: 60)')
    parser.add_argument('--max-error-pct', type=float, default=1.0,
                        help='Max error percentage before FAIL (default: 1.0)')
    parser.add_argument('--max-p99-ms', type=float, default=5000.0,
                        help='Max p99 latency in ms before FAIL (default: 5000)')
    parser.add_argument('--min-tps-pct', type=float, default=50.0,
                        help='Min achieved TPS as pct of target before FAIL (default: 50)')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    params = {
        'url': args.url,
        'duration': args.duration,
        'read_tps': args.read_tps,
        'write_tps': args.write_tps,
        'warmup': args.warmup,
        'max_error_pct': args.max_error_pct,
        'max_p99_ms': args.max_p99_ms,
        'min_tps_pct': args.min_tps_pct,
    }

    stats = Stats()
    all_read_lats: list[float] = []
    all_write_lats: list[float] = []

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Duration: {args.duration}s  Read TPS: {args.read_tps}  Write TPS: {args.write_tps}')
        print(f'Warmup: {args.warmup}s  Thresholds: error<{args.max_error_pct}% p99<{args.max_p99_ms}ms tps>{args.min_tps_pct}%')
        print(f'Status updates every 60s. Ctrl+C for early stop.\n')

        t_start = time.monotonic()

        # Warmup: run workloads but discard stats
        if args.warmup > 0:
            warmup_stats = Stats()
            warmup_tasks = [
                asyncio.create_task(_read_loop(prox, args.read_tps, warmup_stats)),
                asyncio.create_task(_write_loop(prox, args.write_tps, warmup_stats)),
            ]
            try:
                await asyncio.wait_for(_shutdown.wait(), timeout=args.warmup)
            except asyncio.TimeoutError:
                pass
            for t in warmup_tasks:
                t.cancel()
            await asyncio.gather(*warmup_tasks, return_exceptions=True)
            ws = warmup_stats.summary()
            print(f'Warmup complete: {ws["total_reads"]} reads, {ws["total_writes"]} writes (discarded)\n')
            if _shutdown.is_set():
                print('Shutdown during warmup.')
                return 1

        t_measure = time.monotonic()

        tasks = [
            asyncio.create_task(_read_loop(prox, args.read_tps, stats)),
            asyncio.create_task(_write_loop(prox, args.write_tps, stats)),
            asyncio.create_task(_reporter(stats, args.duration, t_measure,
                                          all_read_lats, all_write_lats)),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - t_measure

    # Flush any remaining latencies not yet collected by reporter
    rl, wl = await stats.snapshot_and_reset()
    all_read_lats.extend(rl)
    all_write_lats.extend(wl)

    report = _build_final_report(params, stats, all_read_lats, all_write_lats, elapsed)
    _print_final(report)

    # --- Pass/fail evaluation ---
    failures = _evaluate(report, args)

    if args.output:
        report['failures'] = failures
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nJSON written to {args.output}')

    if failures:
        print(f'\nFAIL — {len(failures)} check(s) failed:')
        for msg in failures:
            print(f'  ✗ {msg}')
        return 1

    print('\nPASS — all checks passed.')
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
