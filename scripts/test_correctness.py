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
import re
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

SENTINEL_FQDN = 'sentinel.correctness.test.com'


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

    # Sentinel node — written last, used to verify replication completeness
    queries.append(f'[ inet:fqdn={SENTINEL_FQDN} ]')

    return queries


async def _seed(prox):
    """Seed all test data through the writer."""
    queries = _build_seed_queries()
    for start in range(0, len(queries), 20):
        chunk = ' '.join(queries[start:start + 20])
        async for _ in prox.storm(chunk):
            pass


async def _wait_for_replication(reader, timeout=30):
    """Poll reader for sentinel node instead of sleeping a fixed duration."""
    query = f'inet:fqdn={SENTINEL_FQDN}'
    deadline = time.monotonic() + timeout
    interval = 0.5
    while time.monotonic() < deadline:
        nodes = await _collect_nodes(reader, query)
        if len(nodes) == 1:
            return True
        await asyncio.sleep(interval)
        interval = min(interval * 1.5, 3.0)
    return False


# -----------------------------------------------------------------------
# Query helpers
# -----------------------------------------------------------------------

async def _collect_nodes(prox, query):
    """Run a storm query and return sorted list of (ndef, props, tags) tuples."""
    nodes = []
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            ndef = mesg[1][0]
            props = mesg[1][1].get('props', {})
            tags = sorted(mesg[1][1].get('tags', {}).keys())
            nodes.append((ndef, props, tags))
    nodes.sort(key=lambda n: str(n[0]))
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

# (name, mode, query, min_expected) — min_expected prevents vacuous [] == [] passes
READ_QUERIES = [
    # Exact lifts (3)
    ('exact_fqdn',          'node', 'inet:fqdn=host1.test.com', 1),
    ('exact_ipv4',          'node', 'inet:ipv4=10.0.0.1', 1),
    ('exact_url',           'node', 'inet:url="http://host0.test.com/page0"', 1),

    # Broad lifts with limits (3)
    ('broad_fqdn',          'node', 'inet:fqdn | limit 50', 50),
    ('broad_ipv4',          'node', 'inet:ipv4 | limit 20', 20),
    ('broad_url',           'node', 'inet:url | limit 10', 10),

    # Zone filter (2) — host*.test.com has zone=test.com, not zone=com
    ('zone_test_com',       'node', 'inet:fqdn:zone=test.com', 50),
    ('zone_com',            'node', 'inet:fqdn:zone=com', 0),

    # Tag lifts (3)
    ('tag_test',            'node', '#test', 10),
    ('tag_test_tag0',       'node', '#test.tag0', 1),
    ('fqdn_with_tag',       'node', 'inet:fqdn#test', 10),

    # Prefix (2) — fixed: was inet:fqdn:fqdn~= which is not a valid property
    ('prefix_host',         'node', 'inet:fqdn~="host"', 50),
    ('prefix_host1',        'node', 'inet:fqdn~="host1"', 11),

    # Count (3)
    ('count_fqdn',          'print', 'inet:fqdn | count', 1),
    ('count_ipv4',          'print', 'inet:ipv4 | count', 1),
    ('count_url',           'print', 'inet:url | count', 1),

    # Pivot (2)
    ('pivot_dns_a',         'node', 'inet:fqdn=host1.test.com -> inet:dns:a', 1),
    ('pivot_ipv4',          'node', 'inet:dns:a:fqdn=host1.test.com -> inet:ipv4', 1),

    # Filter (2)
    ('filter_zone_com',     'node', 'inet:fqdn +inet:fqdn:zone=com', 0),
    ('filter_zone_test',    'node', 'inet:fqdn +inet:fqdn:zone=test.com', 50),

    # Subquery filter (2)
    ('subq_has_dns',        'node', 'inet:fqdn +{ -> inet:dns:a }', 1),
    ('subq_no_dns',         'node', 'inet:fqdn -{ -> inet:dns:a } | limit 10', 10),

    # Expressions and variables (4)
    ('var_assign',          'print', '$x = 42 $lib.print($x)', 1),
    ('var_len',             'print', '$vals = (1, 2, 3) $lib.print($lib.len($vals))', 1),
    ('lib_guid',            'print', '$lib.print($lib.guid())', 1),
    ('expr_math',           'print', '$x = $( 6 * 7 ) $lib.print($x)', 1),
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
# Comparison helpers
# -----------------------------------------------------------------------

_GUID_RE = re.compile(r'^[0-9a-f]{32}$')
_COUNT_RE = re.compile(r'\d+')


async def _run_read_query(prox, mode, query):
    if mode == 'node':
        return await _collect_nodes(prox, query)
    return await _collect_print(prox, query)


def _compare(writer_result, reader_result, mode, name):
    """Compare results with validation for nondeterministic and print outputs."""
    if name == 'lib_guid':
        # GUIDs differ per call — verify both returned a valid GUID
        return (len(writer_result) == 1 and _GUID_RE.match(writer_result[0])
                and len(reader_result) == 1 and _GUID_RE.match(reader_result[0]))

    # For count queries, validate the print message actually contains a number
    if name.startswith('count_'):
        for result in (writer_result, reader_result):
            if not result or not _COUNT_RE.search(result[0]):
                return False

    if mode == 'node':
        # Compare ndefs only for equality (props/tags checked separately)
        w_ndefs = [n[0] for n in writer_result]
        r_ndefs = [n[0] for n in reader_result]
        return w_ndefs == r_ndefs

    return writer_result == reader_result


def _compare_props_tags(writer_result, reader_result):
    """Return list of mismatches in properties or tags between writer and reader nodes."""
    mismatches = []
    for w_node, r_node in zip(writer_result, reader_result):
        ndef = w_node[0]
        if w_node[1] != r_node[1]:
            mismatches.append(f'{ndef}: props differ')
        if w_node[2] != r_node[2]:
            mismatches.append(f'{ndef}: tags differ')
    return mismatches


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

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
    all_passed = False
    error_count = 0
    suspicious = []
    seed_time = 0.0
    read_results = []
    rejection_results = []

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
                print('Waiting for replication (sentinel node)...', flush=True)
                synced = await _wait_for_replication(reader)
                if not synced:
                    print('  WARNING: sentinel node not found on reader after timeout')
                    error_count += 1
                else:
                    print('  Replication confirmed')

            # --- Read queries ---
            if single_mode:
                print(f'\nRunning {len(READ_QUERIES)} read queries against writer...')
            else:
                print(f'\nRunning {len(READ_QUERIES)} read queries against both endpoints...')

            for name, mode, query, min_expected in READ_QUERIES:
                if _shutdown.is_set():
                    break

                try:
                    w_result = await _run_read_query(writer, mode, query)
                except Exception as exc:
                    print(f'  [FAIL] {name} — writer exception: {exc}')
                    error_count += 1
                    read_results.append({
                        'name': name, 'query': query, 'pass': False,
                        'error': f'writer exception: {exc}',
                    })
                    continue

                # Vacuous pass detection
                w_count = len(w_result)
                is_suspicious = (w_count == 0 and min_expected > 0)
                if is_suspicious:
                    suspicious.append(name)

                if single_mode:
                    status = 'SUSPICIOUS' if is_suspicious else 'RECORDED'
                    print(f'  [{status}] {name} (count={w_count}, min_expected={min_expected})')
                    read_results.append({
                        'name': name, 'query': query,
                        'pass': not is_suspicious,
                        'suspicious': is_suspicious,
                        'writer_count': w_count,
                        'reader_count': None,
                        'min_expected': min_expected,
                    })
                    continue

                try:
                    r_result = await _run_read_query(reader, mode, query)
                except Exception as exc:
                    print(f'  [FAIL] {name} — reader exception: {exc}')
                    error_count += 1
                    read_results.append({
                        'name': name, 'query': query, 'pass': False,
                        'error': f'reader exception: {exc}',
                    })
                    continue

                r_count = len(r_result)
                match = _compare(w_result, r_result, mode, name)

                # Check writer meets minimum expected count
                if w_count < min_expected:
                    match = False

                # Check property/tag equivalence for node queries
                prop_tag_mismatches = []
                if mode == 'node' and match:
                    prop_tag_mismatches = _compare_props_tags(w_result, r_result)
                    if prop_tag_mismatches:
                        match = False

                is_suspicious = (w_count == 0 and r_count == 0 and min_expected > 0)
                if is_suspicious:
                    suspicious.append(name)

                status = 'SUSPICIOUS' if is_suspicious else ('PASS' if match else 'FAIL')
                print(f'  [{status}] {name} (w={w_count}, r={r_count}, min={min_expected})')

                result = {
                    'name': name, 'query': query,
                    'pass': match and not is_suspicious,
                    'suspicious': is_suspicious,
                    'writer_count': w_count,
                    'reader_count': r_count,
                    'min_expected': min_expected,
                }
                if not match:
                    result['writer_sample'] = [n[0] for n in w_result[:5]] if mode == 'node' else w_result[:5]
                    result['reader_sample'] = [n[0] for n in r_result[:5]] if mode == 'node' else r_result[:5]
                if prop_tag_mismatches:
                    result['prop_tag_mismatches'] = prop_tag_mismatches[:10]
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
            all_passed = (
                read_passed == read_total
                and reject_passed == reject_total
                and error_count == 0
                and len(suspicious) == 0
            )

            print(f'\n{"=" * 50}')
            print(f'Read queries:      {read_passed}/{read_total} passed')
            if single_mode:
                print(f'Write rejections:  skipped (single-process mode)')
            else:
                print(f'Write rejections:  {reject_passed}/{reject_total} passed')
            if error_count:
                print(f'Errors:            {error_count}')
            if suspicious:
                print(f'Suspicious (0 results, expected >0): {suspicious}')
            print(f'Overall:           {"PASS" if all_passed else "FAIL"}')

            # --- JSON output ---
            report = {
                'overall': 'PASS' if all_passed else 'FAIL',
                'seed_time': round(seed_time, 3),
                'single_mode': single_mode,
                'error_count': error_count,
                'suspicious': suspicious,
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

    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    asyncio.run(main())
