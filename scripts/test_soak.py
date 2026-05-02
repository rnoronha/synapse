#!/usr/bin/env python3.11
"""
Heavy soak test for multi-process Cortex.

Sustains concurrent read and write workloads at target TPS for hours,
tracking per-operation latency and reporting p50/p99/p999 percentiles.
"""
import argparse
import asyncio
import json
import os
import random
import signal
import statistics
import sys
import time

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
                 'total_reads', 'total_writes', '_lock')

    def __init__(self):
        self.read_lats: list[float] = []
        self.write_lats: list[float] = []
        self.read_errs = 0
        self.write_errs = 0
        self.total_reads = 0
        self.total_writes = 0
        self._lock = asyncio.Lock()

    async def record_read(self, latency):
        async with self._lock:
            self.read_lats.append(latency)
            self.total_reads += 1

    async def record_write(self, latency):
        async with self._lock:
            self.write_lats.append(latency)
            self.total_writes += 1

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
        }


async def _read_loop(prox, tps, stats):
    interval = 1.0 / tps if tps > 0 else 1.0
    idx = 0
    while not _shutdown.is_set():
        query = READ_QUERIES[idx % len(READ_QUERIES)]
        idx += 1
        t0 = time.monotonic()
        try:
            async for _ in prox.storm(query):
                pass
            await stats.record_read(time.monotonic() - t0)
        except Exception:
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
            async for _ in prox.storm(query):
                pass
            await stats.record_write(time.monotonic() - t0)
        except Exception:
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

    return {
        'params': params,
        'elapsed_s': round(elapsed, 1),
        **stats.summary(),
        'read_latency': _lat_stats(all_read_lats),
        'write_latency': _lat_stats(all_write_lats),
    }


def _print_final(report):
    print('\n' + '=' * 60)
    print('SOAK TEST COMPLETE')
    print('=' * 60)
    print(f'Duration: {report["elapsed_s"]:.1f}s')
    print(f'Total reads:  {report["total_reads"]:>10}  errors: {report["read_errors"]}')
    print(f'Total writes: {report["total_writes"]:>10}  errors: {report["write_errors"]}')
    for label, key in (('Read', 'read_latency'), ('Write', 'write_latency')):
        lat = report.get(key, {})
        if not lat:
            continue
        print(f'\n{label} latency ({lat["count"]} ops):')
        print(f'  min={lat["min_ms"]:.1f}ms  p50={lat["p50_ms"]:.1f}ms  '
              f'p99={lat["p99_ms"]:.1f}ms  p999={lat["p999_ms"]:.1f}ms  '
              f'max={lat["max_ms"]:.1f}ms  mean={lat["mean_ms"]:.1f}ms')


async def main():
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
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    params = {
        'url': args.url,
        'duration': args.duration,
        'read_tps': args.read_tps,
        'write_tps': args.write_tps,
    }

    stats = Stats()
    all_read_lats: list[float] = []
    all_write_lats: list[float] = []

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Duration: {args.duration}s  Read TPS: {args.read_tps}  Write TPS: {args.write_tps}')
        print(f'Status updates every 60s. Ctrl+C for early stop.\n')

        t_start = time.monotonic()

        tasks = [
            asyncio.create_task(_read_loop(prox, args.read_tps, stats)),
            asyncio.create_task(_write_loop(prox, args.write_tps, stats)),
            asyncio.create_task(_reporter(stats, args.duration, t_start,
                                          all_read_lats, all_write_lats)),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - t_start

    # Flush any remaining latencies not yet collected by reporter
    rl, wl = await stats.snapshot_and_reset()
    all_read_lats.extend(rl)
    all_write_lats.extend(wl)

    report = _build_final_report(params, stats, all_read_lats, all_write_lats, elapsed)
    _print_final(report)

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nJSON written to {args.output}')

    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
