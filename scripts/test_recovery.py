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
import re
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.telepath as s_telepath

_shutdown = asyncio.Event()

# Minimum success ratio for failover read tests (steps 4 & 8)
_FAILOVER_MIN_SUCCESSES = 8


def _handle_signal():
    _shutdown.set()


def _pid_from_port(port):
    """Get PID of the process listening on a TCP port via ss."""
    try:
        out = subprocess.check_output(
            ['ss', '-tlnp', f'sport = :{port}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            if not line.startswith('LISTEN'):
                continue
            match = re.search(r'pid=(\d+)', line)
            if match:
                return int(match.group(1))
    except (subprocess.CalledProcessError, ValueError):
        pass
    return None


def _wait_pid_gone(pid, timeout=5):
    """Poll until a PID no longer exists. Raises if still alive after timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)  # signal 0 = existence check
        except ProcessLookupError:
            return
        time.sleep(0.2)
    raise RuntimeError(f'PID {pid} still alive after {timeout}s')


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
        self.suspicious = False
        self.detail = ''
        self.elapsed = 0.0

    def as_dict(self):
        return {
            'name': self.name,
            'passed': self.passed,
            'suspicious': self.suspicious,
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
        step.detail = f'EXCEPTION: {type(exc).__name__}: {exc}'
    step.elapsed = time.monotonic() - t0
    status = 'PASS' if step.passed else 'FAIL'
    suffix = ' [SUSPICIOUS: vacuous result]' if step.suspicious else ''
    print(f'  [{status}] {name} ({step.elapsed:.1f}s) — {step.detail}{suffix}')
    return step


async def main():
    parser = argparse.ArgumentParser(
        description='Reader failure recovery test for multi-process Cortex.')
    parser.add_argument('url', help='Telepath URL of the router (e.g. tcp://host:port/cortex)')
    parser.add_argument('--reader-ports', default='',
                        help='Comma-separated reader ports (omit for single-process mode)')
    parser.add_argument('--output', default=None, help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    reader_ports = [int(p.strip()) for p in args.reader_ports.split(',') if p.strip()]

    if not reader_ports:
        print('WARNING: Single-process mode — recovery test SKIPPED (no readers configured)')
        step_names = [
            '1. Health check', '2. Seed 100 nodes', '3. Kill reader 1',
            '4. Reads after kill (failover)', '5. Poll reader 1 respawn',
            '6. Verify respawned reader', '7. Kill ALL readers',
            '8. Reads (writer fallback)', '9. Poll all readers respawn',
            '10. Final health check',
        ]
        report = {
            'url': args.url,
            'reader_ports': [],
            'skipped': True,
            'steps': [{'name': n, 'passed': False, 'suspicious': False,
                        'detail': 'SKIPPED — no readers configured', 'elapsed_s': 0.0}
                       for n in step_names],
            'passed': 0,
            'failed': 0,
            'skipped_count': len(step_names),
            'total': len(step_names),
            'total_time_s': 0.0,
        }
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f'Results written to {args.output}')
        print('Result: SKIP (no assertions executed)')
        return 2  # distinct from pass(0) and fail(1)

    reader_urls = [f'tcp://127.0.0.1:{p}/cortex' for p in reader_ports]

    print(f'Router: {args.url}')
    print(f'Readers: {reader_ports}')
    print()

    steps = []
    errors = 0

    # --- Step 1: Health check all readers ---
    async def health_check():
        infos = {}
        for port, url in zip(reader_ports, reader_urls):
            info = await _get_cell_info(url)
            cell = info['cell']
            if not cell.get('active'):
                raise RuntimeError(f'Reader on port {port} is not active')
            pid = _pid_from_port(port)
            infos[port] = {'run': cell['run'], 'pid': pid, 'active': cell['active']}
            print(f'    port={port} pid={pid} run={cell["run"][:8]} active={cell["active"]}')
        return f'{len(infos)} readers healthy (all active)'

    steps.append(await run_step('1. Health check', health_check))
    if _shutdown.is_set():
        return 1

    # --- Step 2: Seed 100 nodes through router ---
    async def seed_nodes():
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(100):
                async for _ in prox.storm(f'[inet:fqdn=recovery-{i}.test.com]'):
                    pass
            # Verify all 100 nodes exist
            count = await _storm_count(prox, 'inet:fqdn=recovery-*.test.com')
        if count < 100:
            raise RuntimeError(f'Expected 100 nodes, got {count}')
        return f'{count} inet:fqdn nodes seeded and verified'

    steps.append(await run_step('2. Seed 100 nodes', seed_nodes))
    if _shutdown.is_set():
        return 1

    # --- Step 3: Kill reader 1 ---
    killed_port = reader_ports[0]
    killed_url = reader_urls[0]

    async def kill_reader_1():
        pid = _pid_from_port(killed_port)
        if pid is None:
            raise RuntimeError(f'Cannot find PID for port {killed_port}')
        os.kill(pid, signal.SIGKILL)
        _wait_pid_gone(pid)
        return f'Killed PID {pid} on port {killed_port} (confirmed dead)'

    steps.append(await run_step('3. Kill reader 1', kill_reader_1))
    if _shutdown.is_set():
        return 1

    # --- Step 4: 10 reads through router (failover) ---
    async def reads_after_kill():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(10):
                try:
                    cnt = await _storm_count(prox, 'inet:fqdn | limit 5')
                    if cnt > 0:
                        successes += 1
                    # cnt == 0 doesn't count as success
                except Exception:
                    pass
        if successes < _FAILOVER_MIN_SUCCESSES:
            raise RuntimeError(
                f'Only {successes}/10 reads succeeded (minimum: {_FAILOVER_MIN_SUCCESSES})')
        return f'{successes}/10 reads succeeded'

    steps.append(await run_step('4. Reads after kill (failover)', reads_after_kill))
    if _shutdown.is_set():
        return 1

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
        return 1

    # --- Step 6: Verify respawned reader serves correct data ---
    async def verify_respawned():
        async with await s_telepath.openurl(killed_url) as prox:
            count = await _storm_count(prox, 'inet:fqdn | limit 10')
            if count == 0:
                raise RuntimeError('Respawned reader returned 0 nodes — possible data loss')
            # Verify a specific seeded node exists (data correctness)
            specific = await _storm_count(prox, 'inet:fqdn=recovery-0.test.com')
            if specific == 0:
                raise RuntimeError(
                    'Respawned reader missing seeded node recovery-0.test.com — data corruption')
        return f'Respawned reader returned {count} nodes, specific node verified'

    steps.append(await run_step('6. Verify respawned reader', verify_respawned))
    if _shutdown.is_set():
        return 1

    # --- Step 7: Kill ALL readers ---
    async def kill_all_readers():
        killed = []
        for port in reader_ports:
            pid = _pid_from_port(port)
            if pid is not None:
                os.kill(pid, signal.SIGKILL)
                _wait_pid_gone(pid)
                killed.append(f'{port}(pid={pid})')
        if not killed:
            raise RuntimeError('No reader PIDs found')
        return f'Killed (confirmed dead): {", ".join(killed)}'

    steps.append(await run_step('7. Kill ALL readers', kill_all_readers))
    if _shutdown.is_set():
        return 1

    # --- Step 8: 10 reads through router (writer fallback) ---
    async def reads_writer_fallback():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(10):
                try:
                    cnt = await _storm_count(prox, 'inet:fqdn | limit 5')
                    if cnt > 0:
                        successes += 1
                except Exception:
                    pass
        if successes < _FAILOVER_MIN_SUCCESSES:
            raise RuntimeError(
                f'Only {successes}/10 reads succeeded (minimum: {_FAILOVER_MIN_SUCCESSES})')
        return f'{successes}/10 reads succeeded (writer fallback)'

    steps.append(await run_step('8. Reads (writer fallback)', reads_writer_fallback))
    if _shutdown.is_set():
        return 1

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
        return 1

    # --- Step 10: Final health check ---
    async def final_health():
        for port, url in zip(reader_ports, reader_urls):
            info = await _get_cell_info(url)
            cell = info['cell']
            if not cell.get('active'):
                raise RuntimeError(f'Reader on port {port} is not active after recovery')
            pid = _pid_from_port(port)
            print(f'    port={port} pid={pid} run={cell["run"][:8]} active={cell["active"]}')
        return f'{len(reader_ports)} readers healthy (all active)'

    steps.append(await run_step('10. Final health check', final_health))

    # --- Summary ---
    passed = sum(1 for s in steps if s.passed)
    failed = sum(1 for s in steps if not s.passed)
    suspicious = sum(1 for s in steps if s.suspicious)
    total = len(steps)
    total_time = sum(s.elapsed for s in steps)

    print(f'\n{"Step":<40} {"Result":>6} {"Time":>8}')
    print('-' * 56)
    for s in steps:
        status = 'PASS' if s.passed else 'FAIL'
        flag = ' ⚠' if s.suspicious else ''
        print(f'{s.name:<40} {status:>6} {s.elapsed:>7.1f}s{flag}')
    print('-' * 56)
    print(f'{"Total":<40} {passed}/{total:>4} {total_time:>7.1f}s')
    if suspicious:
        print(f'  ⚠ {suspicious} step(s) flagged SUSPICIOUS (vacuous results)')

    all_pass = passed == total

    if args.output:
        report = {
            'url': args.url,
            'reader_ports': reader_ports,
            'skipped': False,
            'steps': [s.as_dict() for s in steps],
            'passed': passed,
            'failed': failed,
            'suspicious': suspicious,
            'total': total,
            'total_time_s': round(total_time, 3),
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nResults written to {args.output}')

    return 0 if all_pass else 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
