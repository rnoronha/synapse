#!/usr/bin/env python3.11
"""
Pathological input tests for Synapse Cortex.

Exercises 16 categories of worst-case inputs: regex backtracking,
exponentiation, broad lifts, compression, yield budgets, type
normalization, list operations, hashing, and concurrent blocking.

Seeds its own data if the Cortex is empty.  Per-test timeout.
PASS/FAIL per test with optional JSON output.
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

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


# ---------------------------------------------------------------
# helpers
# ---------------------------------------------------------------

async def _count_storm(prox, query, timeout=None):
    """Run a storm query, return node count.  Raises on timeout."""
    count = 0

    async def _inner():
        nonlocal count
        async for mesg in prox.storm(query):
            if mesg[0] == 'node':
                count += 1

    if timeout:
        await asyncio.wait_for(_inner(), timeout=timeout)
    else:
        await _inner()
    return count


async def _call_storm(prox, query, opts=None, timeout=None):
    """callStorm with optional timeout.  Returns the result."""
    coro = prox.callStorm(query, opts=opts)
    if timeout:
        return await asyncio.wait_for(coro, timeout=timeout)
    return await coro


async def _collect_storm(prox, query, timeout=None):
    """Collect all storm messages, return list."""
    msgs = []

    async def _inner():
        async for mesg in prox.storm(query):
            msgs.append(mesg)

    if timeout:
        await asyncio.wait_for(_inner(), timeout=timeout)
    else:
        await _inner()
    return msgs


# ---------------------------------------------------------------
# seed data
# ---------------------------------------------------------------

SEED_FQDN = 1000
SEED_FQDN_IVAL = 500
SEED_GEO = 200
SEED_URL = 50
SEED_IPV4 = 500

async def _seed_if_empty(prox, timeout):
    """Seed pathological test data if the Cortex has fewer than 100 inet:fqdn nodes."""
    existing = await _count_storm(prox, 'inet:fqdn | count', timeout=timeout)
    if existing >= 100:
        print(f'  Cortex has {existing} inet:fqdn nodes — skipping seed')
        return

    print('  Seeding pathological test data ...')

    # inet:fqdn — broad lift target
    batch = []
    for i in range(SEED_FQDN):
        tld = ['com', 'net', 'org', 'io'][i % 4]
        batch.append(f'[ inet:fqdn=host{i}.pathbench.{tld} ]')
    for start in range(0, len(batch), 50):
        chunk = ' '.join(batch[start:start + 50])
        async for _ in prox.storm(chunk):
            pass

    # inet:fqdn with .seen intervals — for interval lift tests
    for i in range(SEED_FQDN_IVAL):
        month = (i % 12) + 1
        q = f'[ inet:fqdn=ival{i}.pathbench.com .seen=("2023-{month:02d}-01","2023-{month:02d}-28") ]'
        async for _ in prox.storm(q):
            pass

    # geo:place with latlong — for geospatial tests
    import random
    rng = random.Random(42)
    for i in range(SEED_GEO):
        lat = rng.uniform(-90, 90)
        lon = rng.uniform(-180, 180)
        q = f'[ geo:place=* :latlong="{lat},{lon}" :name=geotest{i} ]'
        async for _ in prox.storm(q):
            pass

    # inet:url — for URL normalization
    for i in range(SEED_URL):
        q = f'[ inet:url="http://user:pass@host{i}.pathbench.com:{8000+i}/path?q={i}#frag{i}" ]'
        async for _ in prox.storm(q):
            pass

    # inet:ipv4 — for filter regex tests
    batch = []
    for i in range(SEED_IPV4):
        a, rem = divmod(i, 256)
        batch.append(f'[ inet:ipv4=10.99.{a}.{rem} ]')
    for start in range(0, len(batch), 50):
        chunk = ' '.join(batch[start:start + 50])
        async for _ in prox.storm(chunk):
            pass

    total = await _count_storm(prox, 'inet:fqdn | count', timeout=timeout)
    print(f'  Seeded — {total} inet:fqdn nodes now present')


# ---------------------------------------------------------------
# step runner
# ---------------------------------------------------------------

class Step:
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.detail = ''
        self.elapsed = 0.0

    def as_dict(self):
        return {
            'name': self.name,
            'passed': self.passed,
            'detail': self.detail,
            'elapsed_s': round(self.elapsed, 3),
        }


async def run_step(name, func, timeout):
    step = Step(name)
    t0 = time.monotonic()
    try:
        step.detail = await asyncio.wait_for(func(), timeout=timeout)
        step.passed = True
    except asyncio.TimeoutError:
        step.detail = f'Timed out after {timeout}s'
    except Exception as exc:
        step.detail = str(exc)
    step.elapsed = time.monotonic() - t0
    status = 'PASS' if step.passed else 'FAIL'
    print(f'  [{status}] {name} ({step.elapsed:.1f}s) — {step.detail}')
    return step


# ---------------------------------------------------------------
# tests
# ---------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description='Pathological input tests for Synapse Cortex.')
    parser.add_argument('url', help='Telepath URL (e.g. tcp://host:port/cortex)')
    parser.add_argument('--timeout', type=int, default=30,
                        help='Per-test timeout in seconds (default: 30)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    timeout = args.timeout
    steps = []

    async with await s_telepath.openurl(args.url) as prox:
        print(f'Connected to {args.url}')
        print(f'Per-test timeout: {timeout}s\n')

        # Seed data if needed
        await _seed_if_empty(prox, timeout)
        print()

        # -- 1a. Regex backtracking — pathological -----------------
        async def test_regex_backtrack_pathological():
            q = '''
                $text = ""
                $text = $text.ljust(5000, a)
                $text = $lib.str.concat($text, b)
                if ($text ~= "^(a+)+$") { $lib.print(hit) }
            '''
            await _collect_storm(prox, q, timeout=timeout)
            return 'Completed (regex did not hang)'

        steps.append(await run_step(
            '1a. Regex Backtracking — pathological',
            test_regex_backtrack_pathological, timeout))
        if _shutdown.is_set():
            return 1

        # -- 1b. Regex backtracking — benign -----------------------
        async def test_regex_backtrack_benign():
            count = await _count_storm(prox, 'inet:fqdn~="^host[0-9]+"', timeout=timeout)
            return f'{count} nodes matched benign regex'

        steps.append(await run_step(
            '1b. Regex Backtracking — benign',
            test_regex_backtrack_benign, timeout))
        if _shutdown.is_set():
            return 1

        # -- 2a. Exponentiation — 2^100 ---------------------------
        async def test_exp_small():
            q = '$x = $(2 ** 100) $lib.print($x)'
            await _collect_storm(prox, q, timeout=timeout)
            return 'Computed 2^100'

        steps.append(await run_step(
            '2a. Exponentiation — 2^100',
            test_exp_small, timeout))
        if _shutdown.is_set():
            return 1

        # -- 2b. Exponentiation — 2^5000 --------------------------
        async def test_exp_large():
            q = '$x = $(2 ** 5000) $lib.print($x)'
            await _collect_storm(prox, q, timeout=timeout)
            return 'Computed 2^5000'

        steps.append(await run_step(
            '2b. Exponentiation — 2^5000',
            test_exp_large, timeout))
        if _shutdown.is_set():
            return 1

        # -- 3. Concurrent blocking — probe during scan ------------
        async def test_concurrent_blocking():
            # Fire a heavy scan, then immediately probe with a fast query
            heavy = 'inet:fqdn | limit 9999 | count'
            probe = 'inet:fqdn | limit 1'

            t0 = time.monotonic()
            # Run probe alone for baseline
            await _count_storm(prox, probe, timeout=timeout)
            baseline = time.monotonic() - t0

            # Run heavy + probe concurrently
            t0 = time.monotonic()
            _heavy_task = asyncio.create_task(
                _count_storm(prox, heavy, timeout=timeout))
            await asyncio.sleep(0.1)  # let heavy start
            await _count_storm(prox, probe, timeout=timeout)
            probe_time = time.monotonic() - t0
            await _heavy_task

            ratio = probe_time / baseline if baseline > 0 else 0
            return f'{ratio:.1f}x baseline — {"responsive" if ratio < 5 else "BLOCKED"}'

        steps.append(await run_step(
            '3. Concurrent Blocking — probe during scan',
            test_concurrent_blocking, timeout))
        if _shutdown.is_set():
            return 1

        # -- 4. Broad lift throughput ------------------------------
        async def test_broad_lift():
            t0 = time.monotonic()
            count = await _count_storm(prox, 'inet:fqdn', timeout=timeout)
            elapsed = time.monotonic() - t0
            rate = count / elapsed if elapsed > 0 else 0
            return f'{count} nodes at {rate:,.0f} n/s'

        steps.append(await run_step(
            '4. Broad Lift Throughput',
            test_broad_lift, timeout))
        if _shutdown.is_set():
            return 1

        # -- 5. Compression ----------------------------------------
        async def test_compression():
            q = '''
                $data = ""
                $data = $data.ljust(10000, A)
                $lib.print($lib.len($data))
            '''
            msgs = await _collect_storm(prox, q, timeout=timeout)
            prints = [m[1]['mesg'] for m in msgs if m[0] == 'print']
            return f'Compressed string of len {prints[0] if prints else "?"}'

        steps.append(await run_step(
            '5. Compression',
            test_compression, timeout))
        if _shutdown.is_set():
            return 1

        # -- 6. Yield budget — interval lift -----------------------
        async def test_interval_lift():
            q = 'inet:fqdn:seen@=("2023-06-01","2023-06-02")'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'Interval scan returned {count} nodes'

        steps.append(await run_step(
            '6. Yield Budget — Interval Lift',
            test_interval_lift, timeout))
        if _shutdown.is_set():
            return 1

        # -- 7. Yield budget — geospatial lift ---------------------
        async def test_geospatial_lift():
            q = 'geo:place:latlong*near=((34.1,-118.3),500km)'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'Geospatial scan returned {count} nodes'

        steps.append(await run_step(
            '7. Yield Budget — Geospatial Lift',
            test_geospatial_lift, timeout))
        if _shutdown.is_set():
            return 1

        # -- 8a. Type normalization — URL --------------------------
        async def test_norm_url():
            q = '[ inet:url="HTTP://USER:PASS@EXAMPLE.COM:80/PATH?B=2&A=1#FRAG" ]'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'URL normalized ({count} node)'

        steps.append(await run_step(
            '8a. Type Normalization — URL',
            test_norm_url, timeout))
        if _shutdown.is_set():
            return 1

        # -- 8b. Type normalization — FQDN IDN ---------------------
        async def test_norm_fqdn_idn():
            q = '[ inet:fqdn=münchen.example.com inet:fqdn=café.example.com ]'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'IDN FQDN normalized ({count} nodes)'

        steps.append(await run_step(
            '8b. Type Normalization — FQDN IDN',
            test_norm_fqdn_idn, timeout))
        if _shutdown.is_set():
            return 1

        # -- 8c. Type normalization — CPE --------------------------
        async def test_norm_cpe():
            q = '[ it:sec:cpe="cpe:2.3:a:vendor:product:1.0:update:*:*:*:*:*:*" ]'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'CPE normalized ({count} node)'

        steps.append(await run_step(
            '8c. Type Normalization — CPE',
            test_norm_cpe, timeout))
        if _shutdown.is_set():
            return 1

        # -- 9. Filter regex (_ctorCmprRe) -------------------------
        async def test_filter_regex():
            q = 'inet:fqdn=host0.pathbench.com | +inet:fqdn~="^host[0-9]+\\.pathbench"'
            count = await _count_storm(prox, q, timeout=timeout)
            return f'{count} node matched filter regex'

        steps.append(await run_step(
            '9. Filter Regex',
            test_filter_regex, timeout))
        if _shutdown.is_set():
            return 1

        # -- 10a. List operations — unique -------------------------
        async def test_list_unique():
            q = '''
                $list = $lib.list()
                for $i in $lib.range(10000) {
                    $list.append($($i % 500))
                }
                $uniq = $list.unique()
                return($lib.len($uniq))
            '''
            result = await _call_storm(prox, q, timeout=timeout)
            return f'unique() on 10K items → {result} unique'

        steps.append(await run_step(
            '10a. List Operations — unique',
            test_list_unique, timeout))
        if _shutdown.is_set():
            return 1

        # -- 10b. List operations — sort ---------------------------
        async def test_list_sort():
            q = '''
                $list = $lib.list()
                for $i in $lib.range(10000) {
                    $list.append($(10000 - $i))
                }
                $list.sort()
                return($lib.len($list))
            '''
            result = await _call_storm(prox, q, timeout=timeout)
            return f'sort() on 10K items → {result} items'

        steps.append(await run_step(
            '10b. List Operations — sort',
            test_list_sort, timeout))
        if _shutdown.is_set():
            return 1

        # -- 11. Hashing -------------------------------------------
        async def test_hashing():
            q = '''
                $data = ""
                $data = $data.ljust(100000, X)
                $hash = $lib.crypto.hashes.sha256($data.encode())
                return($hash)
            '''
            result = await _call_storm(prox, q, timeout=timeout)
            return f'SHA256 of 100KB → {str(result)[:16]}...'

        steps.append(await run_step(
            '11. Hashing',
            test_hashing, timeout))

    # -- Summary ---------------------------------------------------
    passed = sum(1 for s in steps if s.passed)
    total = len(steps)
    total_time = sum(s.elapsed for s in steps)

    print(f'\n{"Step":<48} {"Result":>6} {"Time":>8}')
    print('-' * 64)
    for s in steps:
        status = 'PASS' if s.passed else 'FAIL'
        print(f'{s.name:<48} {status:>6} {s.elapsed:>7.1f}s')
    print('-' * 64)
    print(f'{"Total":<48} {passed}/{total:>4} {total_time:>7.1f}s')

    if args.output:
        report = {
            'url': args.url,
            'timeout': timeout,
            'steps': [s.as_dict() for s in steps],
            'passed': passed,
            'total': total,
            'total_time_s': round(total_time, 3),
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nResults written to {args.output}')

    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
