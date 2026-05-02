#!/usr/bin/env python3.11
"""
Concurrent read throughput benchmark for Synapse Cortex.

Measures sequential vs concurrent query performance across multiple runs,
reporting speedup ratios and optional JSON output.
"""
import argparse
import asyncio
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

QUERIES = [
    'inet:fqdn | limit 50',
    'inet:ipv4 | limit 50',
    'inet:url | limit 20',
]

SEED_SPEC = [
    ('inet:fqdn', 500, 'seed{i}.bench.throughput.com'),
    ('inet:ipv4', 300, '10.{a}.{b}.{c}'),
    ('inet:url', 200, 'http://seed{i}.bench.throughput.com/path{i}'),
]

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


async def _count_storm(prox, query):
    count = 0
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            count += 1
    return count


async def _seed_nodes(prox):
    """Seed test data, skipping forms that already have enough nodes."""
    for form, target, template in SEED_SPEC:
        existing = await _count_storm(prox, f'{form} | count')
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

        # Batch in chunks of 50 to avoid oversized queries
        for start in range(0, len(batch), 50):
            chunk = ' '.join(batch[start:start + 50])
            async for _ in prox.storm(chunk):
                pass

        print('done')


async def _run_sequential(prox, queries):
    t0 = time.monotonic()
    for q in queries:
        await _count_storm(prox, q)
    return time.monotonic() - t0


async def _run_concurrent(prox, queries):
    t0 = time.monotonic()
    await asyncio.gather(*[_count_storm(prox, q) for q in queries])
    return time.monotonic() - t0


def _build_query_rotation(concurrency):
    """Build a list of `concurrency` queries cycling through QUERIES."""
    return [QUERIES[i % len(QUERIES)] for i in range(concurrency)]


def _print_table(runs):
    hdr = f'{"Run":<6} {"Sequential(s)":>14} {"Concurrent(s)":>14} {"Speedup":>8}'
    print('\n' + hdr)
    print('-' * len(hdr))
    for r in runs:
        print(f'{r["run"]:<6} {r["sequential_time"]:>14.3f} {r["concurrent_time"]:>14.3f} {r["speedup"]:>8.2f}x')


def _print_averages(runs):
    n = len(runs)
    avg_seq = sum(r['sequential_time'] for r in runs) / n
    avg_con = sum(r['concurrent_time'] for r in runs) / n
    avg_spd = sum(r['speedup'] for r in runs) / n
    print(f'\n{"Avg":<6} {avg_seq:>14.3f} {avg_con:>14.3f} {avg_spd:>8.2f}x')


async def main():
    parser = argparse.ArgumentParser(
        description='Concurrent read throughput benchmark for Synapse Cortex.')
    parser.add_argument('url', help='Telepath URL (e.g. tcp://host:port/cortex)')
    parser.add_argument('--concurrency', type=int, default=20,
                        help='Number of concurrent queries (default: 20)')
    parser.add_argument('--runs', type=int, default=3,
                        help='Number of benchmark runs (default: 3)')
    parser.add_argument('--warmup', type=int, default=5,
                        help='Number of warmup queries before measuring (default: 5)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    queries = _build_query_rotation(args.concurrency)
    results = []

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Concurrency: {args.concurrency}, Runs: {args.runs}')
        print(f'Query rotation: {len(QUERIES)} queries x {args.concurrency} slots\n')

        print('Seeding data (1000 nodes):')
        await _seed_nodes(prox)

        # Warmup: run unmeasured queries to warm the LMDB page cache
        print(f'\nWarmup: {args.warmup} iterations (not measured)...')
        warmup_t0 = time.monotonic()
        for i in range(args.warmup):
            if _shutdown.is_set():
                break
            await _run_sequential(prox, queries)
            await _run_concurrent(prox, queries)
        warmup_time = round(time.monotonic() - warmup_t0, 6)
        print(f'Warmup complete in {warmup_time:.3f}s\n')

        for run_num in range(1, args.runs + 1):
            if _shutdown.is_set():
                print('\nInterrupted — printing partial results.')
                break

            seq_time = await _run_sequential(prox, queries)
            if _shutdown.is_set():
                break

            con_time = await _run_concurrent(prox, queries)
            speedup = seq_time / con_time if con_time > 0 else 0

            results.append({
                'run': run_num,
                'sequential_time': round(seq_time, 6),
                'concurrent_time': round(con_time, 6),
                'speedup': round(speedup, 4),
            })

    if not results:
        print('No results collected.')
        return 1

    _print_table(results)
    _print_averages(results)

    if args.output:
        report = {
            'params': {
                'url': args.url,
                'concurrency': args.concurrency,
                'runs': args.runs,
                'warmup': args.warmup,
                'queries': QUERIES,
            },
            'warmup_time': warmup_time,
            'results': results,
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nJSON written to {args.output}')

    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
