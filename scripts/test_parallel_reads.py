#!/usr/bin/env python3.11
"""
High-concurrency parallel read benchmark for Synapse Cortex.

Fires --concurrency queries per iteration from a rotating query set,
measuring wall time, queries/sec, and per-query latency percentiles
(p50/p99/p999/max).
"""
import argparse
import asyncio
import json
import os
import random
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

QUERIES = [
    'inet:fqdn | limit 50',
    'inet:ipv4 | limit 50',
    'inet:url | limit 20',
    'inet:fqdn:zone=com | limit 30',
    'inet:fqdn~=host | limit 30',
]

SEED_SPEC = [
    ('inet:fqdn', 500, 'seed{i}.bench.parallel.com'),
    ('inet:ipv4', 300, '10.{a}.{b}.{c}'),
    ('inet:url', 200, 'http://seed{i}.bench.parallel.com/path{i}'),
]

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


def _percentile(sorted_data, pct):
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (pct / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


async def _count_storm(prox, query):
    count = 0
    errors = 0
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            count += 1
        elif mesg[0] == 'err':
            errors += 1
    return count, errors


async def _seed_nodes(prox):
    total_errors = 0
    for form, target, template in SEED_SPEC:
        existing, errs = await _count_storm(prox, f'{form} | limit {target}')
        total_errors += errs
        if existing >= target:
            print(f'  {form}: {existing} exist (>= {target}), skipping')
            continue

        need = target - existing
        print(f'  {form}: seeding {need} nodes...', end=' ', flush=True)

        batch = []
        for i in range(existing, existing + need):
            if form == 'inet:ipv4':
                a, rem = divmod(i, 256 * 256)
                b, c = divmod(rem, 256)
                val = template.format(a=a, b=b, c=c)
            else:
                val = template.format(i=i)
            batch.append(f'[ {form}={val} ]')

        for start in range(0, len(batch), 50):
            chunk = ' '.join(batch[start:start + 50])
            async for mesg in prox.storm(chunk):
                if mesg[0] == 'err':
                    total_errors += 1

        print('done')
    return total_errors


async def _timed_query(prox, query):
    t0 = time.monotonic()
    count, errors = await _count_storm(prox, query)
    elapsed = time.monotonic() - t0
    return elapsed, count, errors


async def _run_iteration(prox, concurrency):
    queries = [random.choice(QUERIES) for _ in range(concurrency)]
    t0 = time.monotonic()
    results = await asyncio.gather(*[_timed_query(prox, q) for q in queries])
    wall = time.monotonic() - t0
    latencies = [r[0] for r in results]
    total_results = sum(r[1] for r in results)
    total_errors = sum(r[2] for r in results)
    return wall, latencies, total_results, total_errors


def _print_table(rows):
    hdr = (f'{"Iter":<6} {"Wall(s)":>8} {"QPS":>10} '
           f'{"p50(ms)":>9} {"p99(ms)":>9} {"p999(ms)":>10} {"max(ms)":>9}')
    print('\n' + hdr)
    print('-' * len(hdr))
    for r in rows:
        print(f'{r["iteration"]:<6} {r["wall_time"]:>8.3f} {r["qps"]:>10.1f} '
              f'{r["p50_ms"]:>9.2f} {r["p99_ms"]:>9.2f} {r["p999_ms"]:>10.2f} {r["max_ms"]:>9.2f}')


def _print_averages(rows):
    n = len(rows)
    avgs = {k: sum(r[k] for r in rows) / n
            for k in ('wall_time', 'qps', 'p50_ms', 'p99_ms', 'p999_ms', 'max_ms')}
    print(f'{"Avg":<6} {avgs["wall_time"]:>8.3f} {avgs["qps"]:>10.1f} '
          f'{avgs["p50_ms"]:>9.2f} {avgs["p99_ms"]:>9.2f} {avgs["p999_ms"]:>10.2f} {avgs["max_ms"]:>9.2f}')


def _compute_stats(wall, latencies, iteration, concurrency):
    s = sorted(latencies)
    return {
        'iteration': iteration,
        'wall_time': round(wall, 6),
        'qps': round(concurrency / wall, 2) if wall > 0 else 0,
        'p50_ms': round(_percentile(s, 50) * 1000, 3),
        'p99_ms': round(_percentile(s, 99) * 1000, 3),
        'p999_ms': round(_percentile(s, 99.9) * 1000, 3),
        'max_ms': round(max(s) * 1000, 3) if s else 0,
    }


async def main():
    parser = argparse.ArgumentParser(
        description='High-concurrency parallel read benchmark for Synapse Cortex.')
    parser.add_argument('url', help='Telepath URL (e.g. tcp://host:port/cortex)')
    parser.add_argument('--concurrency', type=int, default=64,
                        help='Number of concurrent queries per iteration (default: 64)')
    parser.add_argument('--iterations', type=int, default=5,
                        help='Number of measured iterations (default: 5)')
    parser.add_argument('--warmup', type=int, default=2,
                        help='Number of unmeasured warmup iterations (default: 2)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    results = []
    raw_latencies = {}
    total_errors = 0
    total_results = 0

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Concurrency: {args.concurrency}, Iterations: {args.iterations}, '
              f'Warmup: {args.warmup}')
        print(f'Query rotation: {len(QUERIES)} queries\n')

        print('Seeding data (1000 nodes):')
        seed_errors = await _seed_nodes(prox)
        total_errors += seed_errors
        if seed_errors:
            print(f'  WARNING: {seed_errors} errors during seeding')

        print(f'\nWarmup: {args.warmup} iterations (not measured)...')
        for i in range(args.warmup):
            if _shutdown.is_set():
                break
            await _run_iteration(prox, args.concurrency)
        print('Warmup complete.\n')

        for it in range(1, args.iterations + 1):
            if _shutdown.is_set():
                print('\nInterrupted — printing partial results.')
                break

            wall, latencies, iter_results, iter_errors = await _run_iteration(prox, args.concurrency)
            total_errors += iter_errors
            total_results += iter_results
            stats = _compute_stats(wall, latencies, it, args.concurrency)
            results.append(stats)
            raw_latencies[it] = [round(l, 6) for l in latencies]

    if not results:
        print('No results collected.')
        return 1

    _print_table(results)
    _print_averages(results)

    # Failure detection
    passed = True

    if total_errors > 0:
        print(f'\nFAIL: {total_errors} query errors detected')
        passed = False

    if total_results == 0:
        print(f'\nFAIL: 0 total results across all queries (vacuous pass)')
        passed = False

    if passed:
        print('\nPASS')

    if args.output:
        report = {
            'params': {
                'url': args.url,
                'concurrency': args.concurrency,
                'iterations': args.iterations,
                'warmup': args.warmup,
                'queries': QUERIES,
            },
            'results': results,
            'raw_latencies': raw_latencies,
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nJSON written to {args.output}')

    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
