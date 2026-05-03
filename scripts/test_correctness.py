#!/usr/bin/env python3.11
"""
Correctness test for multi-process Cortex read/write split.

Seeds test data through the writer, then runs 26 read queries against both
writer and reader to verify result equivalence. Also verifies that 4 write
operations are rejected by the reader with IsReadOnly.
"""
import argparse
import asyncio
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.exc as s_exc
import synapse.telepath as s_telepath

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


# -----------------------------------------------------------------------
# Seed data
# -----------------------------------------------------------------------

def _build_seed_queries():
    """Return storm queries that create the full test dataset."""
    queries = []

    # 50 inet:fqdn
    for i in range(50):
        queries.append(f'[ inet:fqdn=host{i}.test.com ]')

    # 20 inet:ipv4
    for i in range(20):
        queries.append(f'[ inet:ipv4=10.0.0.{i} ]')

    # 10 inet:url
    for i in range(10):
        queries.append(f'[ inet:url="http://host{i}.test.com/page{i}" ]')

    # Tags on some fqdns
    for i in range(10):
        queries.append(f'inet:fqdn=host{i}.test.com [ +#test.tag{i} ]')

    # A dns:a record for pivot tests
    queries.append('[ inet:dns:a=(host1.test.com, 10.0.0.1) ]')

    return queries


async def _seed(prox):
    """Seed all test data through the writer."""
    queries = _build_seed_queries()
    for start in range(0, len(queries), 20):
        chunk = ' '.join(queries[start:start + 20])
        async for _ in prox.storm(chunk):
            pass


# -----------------------------------------------------------------------
# Query helpers
# -----------------------------------------------------------------------

async def _collect_nodes(prox, query):
    """Run a storm query and return sorted list of node primary values."""
    nodes = []
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            nodes.append(mesg[1][0])
    nodes.sort(key=str)
    return nodes


async def _collect_print(prox, query):
    """Run a storm query and return print messages."""
    prints = []
    async for mesg in prox.storm(query):
        if mesg[0] == 'print':
            prints.append(mesg[1].get('mesg', ''))
    return prints


# -----------------------------------------------------------------------
# Read queries (26 total)
# -----------------------------------------------------------------------

READ_QUERIES = [
    # Exact lifts (3)
    ('exact_fqdn',          'node', 'inet:fqdn=host1.test.com'),
    ('exact_ipv4',          'node', 'inet:ipv4=10.0.0.1'),
    ('exact_url',           'node', 'inet:url="http://host0.test.com/page0"'),

    # Broad lifts with limits (3)
    ('broad_fqdn',          'node', 'inet:fqdn | limit 50'),
    ('broad_ipv4',          'node', 'inet:ipv4 | limit 20'),
    ('broad_url',           'node', 'inet:url | limit 10'),

    # Zone filter (2)
    ('zone_com',            'node', 'inet:fqdn:zone=com'),
    ('zone_test_com',       'node', 'inet:fqdn:zone=test.com'),

    # Tag lifts (3)
    ('tag_test',            'node', '#test'),
    ('tag_test_tag0',       'node', '#test.tag0'),
    ('fqdn_with_tag',       'node', 'inet:fqdn#test'),

    # Prefix (2)
    ('prefix_host',         'node', 'inet:fqdn:fqdn~=host'),
    ('prefix_host1',        'node', 'inet:fqdn:fqdn~="host1"'),

    # Count (3)
    ('count_fqdn',          'print', 'inet:fqdn | count'),
    ('count_ipv4',          'print', 'inet:ipv4 | count'),
    ('count_url',           'print', 'inet:url | count'),

    # Pivot (2)
    ('pivot_dns_a',         'node', 'inet:fqdn=host1.test.com -> inet:dns:a'),
    ('pivot_ipv4',          'node', 'inet:dns:a:fqdn=host1.test.com -> inet:ipv4'),

    # Filter (2)
    ('filter_zone_com',     'node', 'inet:fqdn +inet:fqdn:zone=com'),
    ('filter_zone_test',    'node', 'inet:fqdn +inet:fqdn:zone=test.com'),

    # Subquery filter (2)
    ('subq_has_dns',        'node', 'inet:fqdn +{ -> inet:dns:a }'),
    ('subq_no_dns',         'node', 'inet:fqdn -{ -> inet:dns:a } | limit 10'),

    # Expressions and variables (4)
    ('var_assign',          'print', '$x = 42 $lib.print($x)'),
    ('var_len',             'print', '$vals = (1, 2, 3) $lib.print($lib.len($vals))'),
    ('lib_guid',            'print', '$lib.print($lib.guid())'),
    ('expr_math',           'print', '$x = $( 6 * 7 ) $lib.print($x)'),
]

assert len(READ_QUERIES) == 26, f'Expected 26 read queries, got {len(READ_QUERIES)}'


# -----------------------------------------------------------------------
# Write rejection tests (4)
# -----------------------------------------------------------------------

WRITE_REJECTION_QUERIES = [
    ('add_node',        '[ inet:fqdn=reject.test.com ]'),
    ('set_node_data',   'inet:fqdn=host0.test.com $node.data.set(x, 1)'),
    ('add_tag',         'inet:fqdn=host0.test.com [ +#reject.tag ]'),
    ('del_node',        'inet:fqdn=host0.test.com | delnode'),
]


async def _test_write_rejection(prox, name, query):
    """Verify that a write query raises IsReadOnly on the reader."""
    try:
        async for mesg in prox.storm(query):
            if mesg[0] == 'err':
                errname = mesg[1][0]
                if 'IsReadOnly' in errname:
                    return {'name': name, 'pass': True, 'error': errname}
                return {'name': name, 'pass': False, 'error': f'wrong error: {errname}'}
        return {'name': name, 'pass': False, 'error': 'no error raised'}
    except s_exc.IsReadOnly:
        return {'name': name, 'pass': True, 'error': 'IsReadOnly'}
    except Exception as exc:
        errname = type(exc).__name__
        if 'ReadOnly' in errname:
            return {'name': name, 'pass': True, 'error': errname}
        return {'name': name, 'pass': False, 'error': f'{errname}: {exc}'}


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

async def _run_read_query(prox, mode, query):
    if mode == 'node':
        return await _collect_nodes(prox, query)
    return await _collect_print(prox, query)


def _compare(writer_result, reader_result, mode, name):
    """Compare results. For guid/nondeterministic outputs, check shape only."""
    if name == 'lib_guid':
        # GUIDs differ per call — just verify both returned one
        return len(writer_result) == 1 and len(reader_result) == 1

    return writer_result == reader_result


async def main():
    parser = argparse.ArgumentParser(
        description='Correctness test for multi-process Cortex read/write split.')
    parser.add_argument('--writer', required=True,
                        help='Telepath URL for the writer Cortex')
    parser.add_argument('--reader', default=None,
                        help='Telepath URL for the reader Cortex (omit for single-process mode)')
    parser.add_argument('--output', type=str, default=None,
                        help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    single_mode = args.reader is None

    if single_mode:
        print('Single-process mode: recording baseline query results (no reader comparison)')

    async with await s_telepath.openurl(args.writer) as writer:

        reader_ctx = None
        reader = None
        if not single_mode:
            reader_ctx = await s_telepath.openurl(args.reader)
            reader = await reader_ctx.__aenter__()

        try:
            # --- Seed ---
            print('Seeding test data through writer...', flush=True)
            t0 = time.monotonic()
            await _seed(writer)
            seed_time = time.monotonic() - t0
            print(f'  Seeded in {seed_time:.2f}s')

            if not single_mode:
                await asyncio.sleep(6)

            # --- Read queries ---
            if single_mode:
                print(f'\nRunning {len(READ_QUERIES)} read queries against writer...')
            else:
                print(f'\nRunning {len(READ_QUERIES)} read queries against both endpoints...')
            read_results = []

            for name, mode, query in READ_QUERIES:
                if _shutdown.is_set():
                    break

                w_result = await _run_read_query(writer, mode, query)

                if single_mode:
                    print(f'  [RECORDED] {name}')
                    read_results.append({
                        'name': name,
                        'query': query,
                        'pass': True,
                        'writer_count': len(w_result),
                        'reader_count': None,
                    })
                else:
                    r_result = await _run_read_query(reader, mode, query)
                    match = _compare(w_result, r_result, mode, name)
                    status = 'PASS' if match else 'FAIL'
                    print(f'  [{status}] {name}')
                    result = {
                        'name': name,
                        'query': query,
                        'pass': match,
                        'writer_count': len(w_result),
                        'reader_count': len(r_result),
                    }
                    if not match:
                        result['writer_sample'] = w_result[:5]
                        result['reader_sample'] = r_result[:5]
                    read_results.append(result)

            # --- Write rejection ---
            rejection_results = []
            if single_mode:
                print(f'\nSkipping {len(WRITE_REJECTION_QUERIES)} write rejection tests (no reader)')
            else:
                print(f'\nRunning {len(WRITE_REJECTION_QUERIES)} write rejection tests on reader...')
                for name, query in WRITE_REJECTION_QUERIES:
                    if _shutdown.is_set():
                        break
                    result = await _test_write_rejection(reader, name, query)
                    status = 'PASS' if result['pass'] else 'FAIL'
                    print(f'  [{status}] {name}')
                    rejection_results.append(result)

            # --- Summary ---
            read_passed = sum(1 for r in read_results if r['pass'])
            read_total = len(read_results)
            reject_passed = sum(1 for r in rejection_results if r['pass'])
            reject_total = len(rejection_results)
            all_passed = (read_passed == read_total) and (reject_passed == reject_total)

            print(f'\n{"=" * 50}')
            print(f'Read queries:      {read_passed}/{read_total} passed')
            if single_mode:
                print(f'Write rejections:  skipped (single-process mode)')
            else:
                print(f'Write rejections:  {reject_passed}/{reject_total} passed')
            print(f'Overall:           {"PASS" if all_passed else "FAIL"}')

            # --- JSON output ---
            report = {
                'overall': 'PASS' if all_passed else 'FAIL',
                'seed_time': round(seed_time, 3),
                'single_mode': single_mode,
                'read_queries': {
                    'passed': read_passed,
                    'total': read_total,
                    'results': read_results,
                },
                'write_rejections': {
                    'passed': reject_passed,
                    'total': reject_total,
                    'results': rejection_results,
                },
                'params': {
                    'writer': args.writer,
                    'reader': args.reader,
                },
            }

            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(report, f, indent=2)
                print(f'\nJSON written to {args.output}')

        finally:
            if reader_ctx is not None:
                await reader_ctx.__aexit__(None, None, None)

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
