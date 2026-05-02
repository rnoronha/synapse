#!/usr/bin/env python3.11
"""
Self-contained reader failure recovery test for multi-process Cortex.

Connects via telepath to a running router + readers, kills reader processes,
and verifies failover and respawn behavior.
"""
import asyncio
import argparse
import json
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

_shutdown = asyncio.Event()


def _handle_signal():
    _shutdown.set()


def _pid_from_port(port):
    """Get PID of the process listening on a TCP port via ss."""
    try:
        out = subprocess.check_output(
            ['ss', '-tlnp', f'sport = :{port}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        # Parse pid=NNNN from ss output
        for token in out.split():
            if token.startswith('pid='):
                return int(token.split('=')[1].rstrip(',)'))
    except (subprocess.CalledProcessError, ValueError):
        pass
    # Fallback to lsof
    try:
        out = subprocess.check_output(
            ['lsof', '-ti', f':{port}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        return int(out.strip().splitlines()[0])
    except Exception:
        return None


async def _get_cell_info(url, timeout=5):
    """Connect to a telepath URL and return getCellInfo() result."""
    async with await s_telepath.openurl(url) as prox:
        return await asyncio.wait_for(prox.getCellInfo(), timeout=timeout)


async def _storm_count(prox, query):
    """Run a storm query and return node count."""
    count = 0
    async for mesg in prox.storm(query):
        if mesg[0] == 'node':
            count += 1
    return count


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


async def run_step(name, func):
    """Execute a test step, capture timing and pass/fail."""
    step = Step(name)
    t0 = time.monotonic()
    try:
        step.detail = await func()
        step.passed = True
    except Exception as exc:
        step.detail = str(exc)
    step.elapsed = time.monotonic() - t0
    status = 'PASS' if step.passed else 'FAIL'
    print(f'  [{status}] {name} ({step.elapsed:.1f}s) — {step.detail}')
    return step


async def main():
    parser = argparse.ArgumentParser(
        description='Reader failure recovery test for multi-process Cortex.')
    parser.add_argument('url', help='Telepath URL of the router (e.g. tcp://host:port/cortex)')
    parser.add_argument('--reader-ports', default='27493,27494',
                        help='Comma-separated reader ports (default: 27493,27494)')
    parser.add_argument('--output', default=None, help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    reader_ports = [int(p.strip()) for p in args.reader_ports.split(',')]
    reader_urls = [f'tcp://127.0.0.1:{p}/cortex' for p in reader_ports]

    print(f'Router: {args.url}')
    print(f'Readers: {reader_ports}')
    print()

    steps = []

    # --- Step 1: Health check all readers ---
    async def health_check():
        infos = {}
        for port, url in zip(reader_ports, reader_urls):
            info = await _get_cell_info(url)
            cell = info['cell']
            pid = _pid_from_port(port)
            infos[port] = {'run': cell['run'], 'pid': pid, 'active': cell['active']}
            print(f'    port={port} pid={pid} run={cell["run"][:8]} active={cell["active"]}')
        return f'{len(infos)} readers healthy'

    steps.append(await run_step('1. Health check', health_check))
    if _shutdown.is_set():
        return

    # --- Step 2: Seed 100 nodes through router ---
    async def seed_nodes():
        async with await s_telepath.openurl(args.url) as prox:
            await prox.callStorm(
                '[ inet:fqdn=$vals ]',
                opts={'vars': {'vals': [f'recovery-{i}.test.com' for i in range(100)]}},
            )
        return '100 inet:fqdn nodes seeded'

    steps.append(await run_step('2. Seed 100 nodes', seed_nodes))
    if _shutdown.is_set():
        return

    # --- Step 3: Kill reader 1 ---
    killed_port = reader_ports[0]
    killed_url = reader_urls[0]

    async def kill_reader_1():
        pid = _pid_from_port(killed_port)
        if pid is None:
            raise RuntimeError(f'Cannot find PID for port {killed_port}')
        os.kill(pid, signal.SIGKILL)
        await asyncio.sleep(0.5)
        return f'Killed PID {pid} on port {killed_port}'

    steps.append(await run_step('3. Kill reader 1', kill_reader_1))
    if _shutdown.is_set():
        return

    # --- Step 4: 10 reads through router (failover) ---
    async def reads_after_kill():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(10):
                try:
                    await _storm_count(prox, 'inet:fqdn | limit 5')
                    successes += 1
                except Exception:
                    pass
        if successes == 0:
            raise RuntimeError('All 10 reads failed')
        return f'{successes}/10 reads succeeded'

    steps.append(await run_step('4. Reads after kill (failover)', reads_after_kill))
    if _shutdown.is_set():
        return

    # --- Step 5: Poll reader 1 for respawn ---
    async def poll_respawn():
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _shutdown.is_set():
                raise RuntimeError('Interrupted')
            try:
                await _get_cell_info(killed_url, timeout=3)
                return f'Reader on port {killed_port} respawned'
            except Exception:
                await asyncio.sleep(2)
        raise RuntimeError(f'Reader on port {killed_port} did not respawn within 30s')

    steps.append(await run_step('5. Poll reader 1 respawn', poll_respawn))
    if _shutdown.is_set():
        return

    # --- Step 6: Verify respawned reader serves query ---
    async def verify_respawned():
        async with await s_telepath.openurl(killed_url) as prox:
            count = await _storm_count(prox, 'inet:fqdn | limit 10')
        return f'Respawned reader returned {count} nodes'

    steps.append(await run_step('6. Verify respawned reader', verify_respawned))
    if _shutdown.is_set():
        return

    # --- Step 7: Kill ALL readers ---
    async def kill_all_readers():
        killed = []
        for port in reader_ports:
            pid = _pid_from_port(port)
            if pid is not None:
                os.kill(pid, signal.SIGKILL)
                killed.append(f'{port}(pid={pid})')
        await asyncio.sleep(0.5)
        if not killed:
            raise RuntimeError('No reader PIDs found')
        return f'Killed: {", ".join(killed)}'

    steps.append(await run_step('7. Kill ALL readers', kill_all_readers))
    if _shutdown.is_set():
        return

    # --- Step 8: 10 reads through router (writer fallback) ---
    async def reads_writer_fallback():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(10):
                try:
                    await _storm_count(prox, 'inet:fqdn | limit 5')
                    successes += 1
                except Exception:
                    pass
        if successes == 0:
            raise RuntimeError('All 10 reads failed — writer fallback broken')
        return f'{successes}/10 reads succeeded (writer fallback)'

    steps.append(await run_step('8. Reads (writer fallback)', reads_writer_fallback))
    if _shutdown.is_set():
        return

    # --- Step 9: Poll all readers for respawn ---
    async def poll_all_respawn():
        deadline = time.monotonic() + 30
        alive = set()
        while time.monotonic() < deadline and len(alive) < len(reader_ports):
            if _shutdown.is_set():
                raise RuntimeError('Interrupted')
            for port, url in zip(reader_ports, reader_urls):
                if port in alive:
                    continue
                try:
                    await _get_cell_info(url, timeout=3)
                    alive.add(port)
                except Exception:
                    pass
            if len(alive) < len(reader_ports):
                await asyncio.sleep(2)
        if len(alive) < len(reader_ports):
            missing = set(reader_ports) - alive
            raise RuntimeError(f'Readers not respawned within 30s: {missing}')
        return f'All {len(reader_ports)} readers respawned'

    steps.append(await run_step('9. Poll all readers respawn', poll_all_respawn))
    if _shutdown.is_set():
        return

    # --- Step 10: Final health check ---
    async def final_health():
        for port, url in zip(reader_ports, reader_urls):
            info = await _get_cell_info(url)
            cell = info['cell']
            pid = _pid_from_port(port)
            print(f'    port={port} pid={pid} run={cell["run"][:8]} active={cell["active"]}')
        return f'{len(reader_ports)} readers healthy'

    steps.append(await run_step('10. Final health check', final_health))

    # --- Summary ---
    passed = sum(1 for s in steps if s.passed)
    total = len(steps)
    total_time = sum(s.elapsed for s in steps)

    print(f'\n{"Step":<40} {"Result":>6} {"Time":>8}')
    print('-' * 56)
    for s in steps:
        status = 'PASS' if s.passed else 'FAIL'
        print(f'{s.name:<40} {status:>6} {s.elapsed:>7.1f}s')
    print('-' * 56)
    print(f'{"Total":<40} {passed}/{total:>4} {total_time:>7.1f}s')

    if args.output:
        report = {
            'url': args.url,
            'reader_ports': reader_ports,
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
