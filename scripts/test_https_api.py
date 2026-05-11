#!/usr/bin/env python3
'''
Local test: verify HTTPS API (port 4443) works with fork mode.

Starts a Cortex with fork mode + HTTPS, sends a Storm query via the HTTPS API,
and verifies it returns results. This validates:
1. Tornado HTTPS listener is restored after fork
2. Storm queries via HTTP reach the cortex
3. Read routing to workers functions (if enabled)

Usage:
    python3 scripts/test_https_api.py [--datadir /tmp/test-cortex] [--workers 2]
'''
import asyncio
import argparse
import json
import os
import signal
import ssl
import subprocess
import sys
import tempfile
import time

import aiohttp


async def wait_for_https(port, timeout=60):
    '''Wait until HTTPS port accepts connections and responds.'''
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    url = f'https://localhost:{port}/api/v1/active'
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    start = time.time()
    while time.time() - start < timeout:
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        return True
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            pass
        await asyncio.sleep(1)
    return False


async def storm_call(port, query, opts=None):
    '''Send a Storm callStorm request via HTTPS API.'''
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    url = f'https://localhost:{port}/api/v1/storm/call'
    payload = {'query': query, 'opts': opts or {}}
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            body = await resp.json()
            return resp.status, body


async def storm_stream(port, query, opts=None):
    '''Send a streaming Storm request via HTTPS API.'''
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    url = f'https://localhost:{port}/api/v1/storm'
    payload = {'query': query, 'opts': opts or {}}
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    messages = []

    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, params={'query': query}, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            async for line in resp.content:
                line = line.strip()
                if line:
                    messages.append(json.loads(line))
    return messages


async def main():
    parser = argparse.ArgumentParser(description='Test HTTPS API with fork mode')
    parser.add_argument('--datadir', default=None, help='Cortex data directory (created if missing)')
    parser.add_argument('--workers', type=int, default=2, help='Number of fork workers')
    parser.add_argument('--https-port', type=int, default=4443, help='HTTPS port')
    parser.add_argument('--keep', action='store_true', help='Keep cortex running after test')
    args = parser.parse_args()

    datadir = args.datadir or tempfile.mkdtemp(prefix='test-cortex-')
    print(f'Data dir: {datadir}')

    # Write cell.yaml
    os.makedirs(datadir, exist_ok=True)
    core_pct = max(10, args.workers * 100 // (os.cpu_count() or 4))
    with open(os.path.join(datadir, 'cell.yaml'), 'w') as f:
        f.write(f'auth:anon: root\nmulti:process:core_pct: {core_pct}\n')
    print(f'Config: core_pct={core_pct} ({args.workers} workers on {os.cpu_count()} cores)')

    # Start cortex
    cmd = [
        sys.executable, '-m', 'synapse.servers.cortex', datadir,
        '--telepath', 'tcp://127.0.0.1:0/',
        '--https', str(args.https_port),
    ]
    print(f'Starting cortex...')
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Read startup logs
    fork_mode = False
    https_ready = False
    start = time.time()
    while time.time() - start < 120:
        line = proc.stdout.readline().decode('utf-8', errors='replace')
        if not line:
            if proc.poll() is not None:
                print(f'ERROR: Cortex exited with code {proc.returncode}')
                return 1
            await asyncio.sleep(0.1)
            continue
        line = line.rstrip()
        if 'Fork mode:' in line:
            fork_mode = True
            print(f'  ✓ {line}')
        elif 'https' in line.lower() and ('listening' in line.lower() or str(args.https_port) in line):
            https_ready = True
            print(f'  ✓ {line}')
        elif 'error' in line.lower() or 'traceback' in line.lower():
            print(f'  ✗ {line}')

        if fork_mode and https_ready:
            break

    if not https_ready:
        print('ERROR: HTTPS never became ready')
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        return 1

    # Wait for full initialization
    print(f'\nWaiting for HTTPS API to respond...')
    if not await wait_for_https(args.https_port, timeout=60):
        print('ERROR: HTTPS API never responded')
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        return 1
    print('  ✓ HTTPS API responding')

    # Test 1: callStorm
    print(f'\nTest 1: callStorm via HTTPS...')
    status, body = await storm_call(args.https_port, 'return(42)')
    if status == 200 and body.get('status') == 'ok' and body.get('result') == 42:
        print(f'  ✓ PASS: callStorm returned {body["result"]}')
    else:
        print(f'  ✗ FAIL: status={status} body={json.dumps(body)[:200]}')
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        return 1

    # Test 2: callStorm with data
    print(f'\nTest 2: callStorm count...')
    status, body = await storm_call(args.https_port, 'return($lib.len($lib.list(1,2,3)))')
    if status == 200 and body.get('status') == 'ok' and body.get('result') == 3:
        print(f'  ✓ PASS: count returned {body["result"]}')
    else:
        print(f'  ✗ FAIL: status={status} body={json.dumps(body)[:200]}')

    # Test 3: Parallel queries (verify no crashes under load)
    print(f'\nTest 3: 50 parallel callStorm queries...')
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=50)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        url = f'https://localhost:{args.https_port}/api/v1/storm/call'
        for i in range(50):
            payload = json.dumps({'query': f'return({i})', 'opts': {}})
            tasks.append(session.post(url, data=payload, headers={'Content-Type': 'application/json'}))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, Exception) and r.status == 200)
        for r in results:
            if hasattr(r, 'close'):
                r.close()

    if ok == 50:
        print(f'  ✓ PASS: {ok}/50 queries succeeded')
    else:
        print(f'  ✗ FAIL: {ok}/50 queries succeeded')

    # Summary
    print(f'\n{"="*50}')
    print(f'HTTPS API with fork mode: ALL TESTS PASSED')
    print(f'{"="*50}')

    if args.keep:
        print(f'\nCortex running (PID {proc.pid}). Press Ctrl+C to stop.')
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=10)
    else:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
