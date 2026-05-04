#!/usr/bin/env python3.11
"""
Read-after-write consistency characterization test for multi-process Cortex.

Readers refresh their LMDB read transaction every READONLY_REFRESH_PERIOD (5s),
so writes routed to a reader may not be visible for up to 5s. Writes routed to
the writer process are always immediately visible.

This script measures staleness distribution across load conditions.
"""
import asyncio
import argparse
import json
import os
import signal
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

READONLY_REFRESH_PERIOD = 5.0
READ_TIMEOUT = READONLY_REFRESH_PERIOD + 2.0
BACKOFF_DELAYS = [0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 5.0]

# Pass/fail thresholds
MIN_CONSISTENCY_RATE = 0.999
MAX_P99_STALENESS = READONLY_REFRESH_PERIOD + 2.0
MAX_PERMANENT_MISSES = 0

CONDITIONS = [
    ('low',       1,  None),
    ('medium',   10,  None),
    ('high',     50,  None),
    ('sustained', 10, None),
]

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


async def _storm_has_node(prox, query):
    """Check if a storm query returns a node. Raises on connection/server errors."""
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            return True
        if mesg[0] == 'err':
            raise RuntimeError(f'Storm error: {mesg[1]}')
    return False


async def _write_and_read(prox, fqdn):
    """Write a node, read it back with retry. Return (found, staleness_s)."""
    await prox.callStorm(f'[ inet:fqdn={fqdn} ]')
    t0 = time.monotonic()

    if await _storm_has_node(prox, f'inet:fqdn={fqdn}'):
        return True, 0.0

    for delay in BACKOFF_DELAYS:
        if _shutdown.is_set():
            return False, time.monotonic() - t0
        await asyncio.sleep(delay)
        if await _storm_has_node(prox, f'inet:fqdn={fqdn}'):
            return True, time.monotonic() - t0

    return False, time.monotonic() - t0


async def _run_condition(url, name, rate, duration, warmup=0):
    """Run one condition, yield (found, staleness) per iteration."""
    interval = 1.0 / rate
    i = 0
    ts = int(time.time())

    async with await s_telepath.openurl(url) as prox:
        if warmup > 0:
            print(f'\nWarmup: {warmup} iterations (not measured)...')
            for wi in range(warmup):
                if _shutdown.is_set():
                    break
                fqdn = f'raw-warmup-{ts}-{wi}.test.com'
                await _write_and_read(prox, fqdn)
            print('Warmup complete.\n')

        deadline = time.monotonic() + duration
        while time.monotonic() < deadline and not _shutdown.is_set():
            fqdn = f'raw-{name}-{ts}-{i}.test.com'
            iter_start = time.monotonic()

            found, staleness = await _write_and_read(prox, fqdn)
            yield found, staleness

            i += 1
            elapsed = time.monotonic() - iter_start
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)


def _compute_stats(results):
    total = len(results)
    if total == 0:
        return {'total': 0, 'found': 0, 'missed': 0, 'consistency_rate': 0,
                'miss_rate': 0, 'staleness_p50': 0, 'staleness_p99': 0,
                'staleness_max': 0}

    found = sum(1 for f, _ in results if f)
    missed = total - found
    stale = sorted(s for f, s in results if f and s > 0)

    def pct(vals, p):
        if not vals:
            return 0.0
        k = (len(vals) - 1) * p / 100
        lo = int(k)
        hi = min(lo + 1, len(vals) - 1)
        return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)

    return {
        'total': total,
        'found': found,
        'missed': missed,
        'consistency_rate': found / total,
        'miss_rate': missed / total,
        'staleness_p50': pct(stale, 50),
        'staleness_p99': pct(stale, 99),
        'staleness_max': max((s for _, s in results), default=0),
    }


def _print_table(all_stats):
    hdr = f'{"Condition":<12} {"Total":>7} {"Found":>7} {"Missed":>7} {"Consist%":>9} {"p50(s)":>8} {"p99(s)":>8} {"max(s)":>8}'
    print('\n' + hdr)
    print('-' * len(hdr))
    for name, st in all_stats.items():
        print(f'{name:<12} {st["total"]:>7} {st["found"]:>7} {st["missed"]:>7} '
              f'{st["consistency_rate"]:>8.2%} {st["staleness_p50"]:>8.3f} '
              f'{st["staleness_p99"]:>8.3f} {st["staleness_max"]:>8.3f}')


async def main():
    parser = argparse.ArgumentParser(
        description='Read-after-write consistency characterization test for multi-process Cortex.')
    parser.add_argument('url', help='Telepath URL of the Cortex (e.g. tcp://host:port/cortex)')
    parser.add_argument('--duration', type=int, default=300,
                        help='Seconds per condition (default: 300, sustained always 1h)')
    parser.add_argument('--warmup', type=int, default=10,
                        help='Number of unmeasured warmup iterations (default: 10)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    all_stats = {}
    all_raw = {}

    first_condition = True
    for name, rate, override_dur in CONDITIONS:
        duration = override_dur or args.duration
        print(f'\n=== Condition: {name} — {rate}/s for {duration}s ===')

        results = []
        errors = 0
        warmup = args.warmup if first_condition else 0
        first_condition = False
        try:
            async for found, staleness in _run_condition(args.url, name, rate, duration, warmup=warmup):
                results.append((found, staleness))
                if len(results) % 100 == 0:
                    print(f'  {len(results)} iterations...', end='\r')
        except Exception as exc:
            errors += 1
            print(f'  Error (condition aborted): {exc}')

        stats = _compute_stats(results)
        stats['errors'] = errors
        all_stats[name] = stats
        all_raw[name] = results
        print(f'  Done: {stats["total"]} iterations, '
              f'{stats["consistency_rate"]:.2%} consistent, '
              f'p99 staleness {stats["staleness_p99"]:.3f}s')

        if _shutdown.is_set():
            print('\nInterrupted — printing partial results.')
            break

    _print_table(all_stats)

    # --- Pass/fail verdict ---
    passed = True
    for name, st in all_stats.items():
        if st['total'] == 0:
            print(f'FAIL: {name} — 0 results (vacuous pass)')
            passed = False
        if st['errors'] > 0:
            print(f'FAIL: {name} — {st["errors"]} error(s)')
            passed = False
        if st['missed'] > MAX_PERMANENT_MISSES:
            print(f'FAIL: {name} — {st["missed"]} permanent miss(es) (max {MAX_PERMANENT_MISSES})')
            passed = False
        if st['total'] > 0 and st['consistency_rate'] < MIN_CONSISTENCY_RATE:
            print(f'FAIL: {name} — consistency {st["consistency_rate"]:.2%} < {MIN_CONSISTENCY_RATE:.2%}')
            passed = False
        if st['staleness_p99'] > MAX_P99_STALENESS:
            print(f'FAIL: {name} — p99 staleness {st["staleness_p99"]:.3f}s > {MAX_P99_STALENESS:.1f}s')
            passed = False

    output = args.output or 'raw_results.json'
    report = {
        'conditions': {
            name: {**st, 'raw': [(f, round(s, 6)) for f, s in all_raw.get(name, [])]}
            for name, st in all_stats.items()
        },
        'params': {'duration': args.duration, 'url': args.url},
    }
    with open(output, 'w') as f:
        json.dump(report, f, indent=2)
    print(f'\nResults written to {output}')

    if passed:
        print('\nVERDICT: PASS')
    else:
        print('\nVERDICT: FAIL')
    sys.exit(0 if passed else 1)


if __name__ == '__main__':
    asyncio.run(main())
