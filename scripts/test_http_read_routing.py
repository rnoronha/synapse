#!/usr/bin/env python3
'''
Local test: verify HTTP API Storm queries are routed to fork workers.

Starts a Cortex with fork mode + HTTPS, sends Storm queries via the HTTP API,
and checks that worker processes gain CPU ticks (proving reads are distributed).

Usage:
    python3 scripts/test_http_read_routing.py [--datadir /tmp/test-cortex] [--workers 2]
'''
import asyncio
import argparse
import json
import os
import ssl
import sys
import tempfile
import time
import signal
import subprocess

import aiohttp


async def get_process_ticks():
    '''Get CPU ticks for all python processes.'''
    ticks = {}
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            with open(f'/proc/{pid}/stat') as f:
                parts = f.read().split()
                comm = parts[1]
                utime = int(parts[13])
                stime = int(parts[14])
                if 'python' in comm.lower():
                    ticks[int(pid)] = utime + stime
        except (FileNotFoundError, PermissionError, IndexError):
            pass
    return ticks


async def send_storm_queries(port, count=100, query='inet:fqdn | limit 10'):
    '''Send Storm queries via HTTPS API.'''
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    url = f'https://localhost:{port}/api/v1/storm/call'
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({'query': query, 'opts': {}})

    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for _ in range(count):
            tasks.append(session.post(url, data=payload, headers=headers))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, Exception) and r.status == 200)
        errs = sum(1 for r in results if isinstance(r, Exception) or (hasattr(r, 'status') and r.status != 200))

        # Close responses
        for r in results:
            if hasattr(r, 'close'):
                r.close()

    return ok, errs


async def main():
    parser = argparse.ArgumentParser(description='Test HTTP API read routing to workers')
    parser.add_argument('--datadir', default=None, help='Cortex data directory')
    parser.add_argument('--workers', type=int, default=2, help='Number of fork workers')
    parser.add_argument('--queries', type=int, default=200, help='Number of queries to send')
    parser.add_argument('--https-port', type=int, default=0, help='HTTPS port (0=random)')
    args = parser.parse_args()

    datadir = args.datadir or tempfile.mkdtemp(prefix='test-cortex-')
    print(f'Data dir: {datadir}')

    # Write cell.yaml with fork mode enabled
    os.makedirs(datadir, exist_ok=True)
    with open(os.path.join(datadir, 'cell.yaml'), 'w') as f:
        f.write(f'auth:anon: root\nmulti:process:core_pct: {args.workers * 100 // os.cpu_count()}\n')

    # Start cortex as subprocess
    cmd = [
        sys.executable, '-m', 'synapse.servers.cortex', datadir,
        '--telepath', 'tcp://0.0.0.0:0/',
        '--https', str(args.https_port),
    ]
    print(f'Starting cortex: {" ".join(cmd)}')
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Wait for HTTPS port to be available
    https_port = None
    start = time.time()
    while time.time() - start < 120:
        line = proc.stdout.readline().decode('utf-8', errors='replace')
        if not line:
            await asyncio.sleep(0.1)
            continue
        sys.stdout.write(f'  [cortex] {line}')
        if 'https' in line.lower() and 'listening' in line.lower():
            # Extract port from log
            import re
            m = re.search(r'https.*?(\d+)', line)
            if m:
                https_port = int(m.group(1))
                break
        if 'Fork mode:' in line:
            print(f'  → Fork mode active')

    if https_port is None or https_port == 0:
        # Try to find it from /proc
        await asyncio.sleep(2)
        print('WARNING: Could not determine HTTPS port from logs, trying 4443')
        https_port = 4443

    print(f'\nHTTPS port: {https_port}')
    print(f'Waiting 5s for workers to initialize...')
    await asyncio.sleep(5)

    # Snapshot ticks BEFORE
    ticks_before = await get_process_ticks()
    main_pid = proc.pid
    print(f'\nMain PID: {main_pid}')
    print(f'Process ticks before: {len(ticks_before)} python processes')

    # Send queries
    print(f'\nSending {args.queries} Storm queries via HTTPS API...')
    t0 = time.time()
    ok, errs = await send_storm_queries(https_port, count=args.queries)
    elapsed = time.time() - t0
    print(f'Results: {ok} OK, {errs} errors, {elapsed:.1f}s ({ok/elapsed:.0f} QPS)')

    # Snapshot ticks AFTER
    await asyncio.sleep(0.5)
    ticks_after = await get_process_ticks()

    # Compute deltas
    print(f'\nCPU tick deltas:')
    worker_ticks = 0
    main_ticks = 0
    for pid in sorted(ticks_after.keys()):
        before = ticks_before.get(pid, 0)
        after = ticks_after[pid]
        delta = after - before
        if delta > 0:
            label = 'MAIN' if pid == main_pid else 'worker/other'
            print(f'  PID {pid} ({label}): +{delta} ticks')
            if pid == main_pid:
                main_ticks = delta
            else:
                worker_ticks += delta

    # Verdict
    print(f'\nSummary:')
    print(f'  Main process ticks: {main_ticks}')
    print(f'  Worker total ticks: {worker_ticks}')

    if worker_ticks > main_ticks * 0.5:
        print(f'  PASS — workers handled significant load ({worker_ticks}/{main_ticks+worker_ticks} = {worker_ticks*100//(main_ticks+worker_ticks+1)}%)')
        result = 0
    elif ok == 0:
        print(f'  FAIL — no successful queries (HTTPS may not be working)')
        result = 1
    else:
        print(f'  FAIL — workers did not pick up load (main={main_ticks}, workers={worker_ticks})')
        result = 1

    # Cleanup
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()

    return result


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
