#!/usr/bin/env python3.11
"""
Worker failure recovery test for multi-process Cortex (v3 shared-port).

In v3, all workers share a single port (27492) via EPOLLEXCLUSIVE. Workers
are child processes of the cortex. This test kills workers by PID, verifies
reads still succeed through remaining workers, and confirms respawn.
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
_FAILOVER_MIN_SUCCESSES = 8


def _handle_signal():
    _shutdown.set()


def _get_cortex_pid(url):
    """Resolve the parent cortex PID from a telepath URL's port via ss.

    In fork mode, multiple processes share the port via EPOLLEXCLUSIVE.
    We find all PIDs on the port, then return the one whose PPID is NOT
    another cortex process (i.e. the parent/writer).
    """
    match = re.search(r':(\d+)/', url)
    if not match:
        return None
    port = match.group(1)
    try:
        out = subprocess.check_output(
            ['ss', '-tlnp', f'sport = :{port}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        pids = set()
        for line in out.splitlines():
            if 'LISTEN' not in line:
                continue
            for m in re.finditer(r'pid=(\d+)', line):
                pids.add(int(m.group(1)))
        if not pids:
            return None
        if len(pids) == 1:
            return pids.pop()
        # Multiple PIDs: the parent is the one whose PPID is not in the set
        for pid in sorted(pids):
            try:
                stat = subprocess.check_output(
                    ['ps', '-o', 'ppid=', '-p', str(pid)],
                    text=True, stderr=subprocess.DEVNULL,
                ).strip()
                ppid = int(stat)
                if ppid not in pids:
                    return pid
            except (subprocess.CalledProcessError, ValueError):
                continue
        # Fallback: return the lowest PID (likely the parent)
        return min(pids)
    except (subprocess.CalledProcessError, ValueError):
        pass
    return None


def _get_worker_pids(cortex_pid):
    """Return list of fork-mode worker child PIDs of the cortex process.

    Only returns children that share the listening port (via ss), filtering
    out non-worker children like axon/jsonstor subprocesses.
    """
    try:
        out = subprocess.check_output(
            ['pgrep', '-P', str(cortex_pid)],
            text=True, stderr=subprocess.DEVNULL,
        )
        all_children = [int(p) for p in out.strip().split('\n') if p.strip()]
    except subprocess.CalledProcessError:
        return []

    if not all_children:
        return []

    # Find PIDs that share the cortex listening port
    try:
        ss_out = subprocess.check_output(
            ['ss', '-tlnp'], text=True, stderr=subprocess.DEVNULL,
        )
        port_pids = set()
        for line in ss_out.splitlines():
            if ':27492' not in line or 'LISTEN' not in line:
                continue
            for m in re.finditer(r'pid=(\d+)', line):
                port_pids.add(int(m.group(1)))
    except subprocess.CalledProcessError:
        port_pids = set()

    # Workers are children that share the listening port
    workers = [p for p in all_children if p in port_pids]
    # Fallback: if ss filtering found nothing, return all children
    return workers if workers else all_children


def _wait_pid_gone(pid, timeout=15):
    """Poll until a PID no longer exists."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.2)
    raise RuntimeError(f'PID {pid} still alive after {timeout}s')


async def _storm_count(prox, query):
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
    step = Step(name)
    t0 = time.monotonic()
    try:
        step.detail = await func()
        step.passed = True
    except Exception as exc:
        step.detail = f'EXCEPTION: {type(exc).__name__}: {exc}'
    step.elapsed = time.monotonic() - t0
    status = 'PASS' if step.passed else 'FAIL'
    print(f'  [{status}] {name} ({step.elapsed:.1f}s) — {step.detail}')
    return step


async def main():
    parser = argparse.ArgumentParser(
        description='Worker failure recovery test for v3 shared-port Cortex.')
    parser.add_argument('url', help='Telepath URL of the cortex (e.g. tcp://host:27492/cortex)')
    parser.add_argument('--cortex-pid', type=int, default=None,
                        help='PID of the cortex process (auto-detected from port if omitted)')
    parser.add_argument('--output', default=None, help='Path to write JSON results')
    args = parser.parse_args()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    cortex_pid = args.cortex_pid or _get_cortex_pid(args.url)
    if cortex_pid is None:
        print('ERROR: Cannot determine cortex PID. Provide --cortex-pid or ensure cortex is listening.')
        return 1

    worker_pids = _get_worker_pids(cortex_pid)
    if not worker_pids:
        print(f'WARNING: No worker children of cortex PID {cortex_pid} — recovery test SKIPPED')
        step_names = [
            '1. Health check', '2. Seed 100 nodes', '3. Kill worker 1',
            '4. Reads after kill (failover)', '5. Poll worker 1 respawn',
            '6. Verify respawned worker', '7. Kill ALL workers',
            '8. Reads (writer fallback)', '9. Poll all workers respawn',
            '10. Final health check',
        ]
        report = {
            'url': args.url, 'cortex_pid': cortex_pid,
            'skipped': True,
            'steps': [{'name': n, 'passed': False, 'suspicious': False,
                        'detail': 'SKIPPED — no workers found', 'elapsed_s': 0.0}
                       for n in step_names],
            'passed': 0, 'failed': 0, 'skipped_count': len(step_names),
            'total': len(step_names), 'total_time_s': 0.0,
        }
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
        print('Result: SKIP (no assertions executed)')
        return 2

    print(f'Cortex PID: {cortex_pid}')
    print(f'Workers: {worker_pids}')
    print()

    steps = []

    # --- Step 1: Health check ---
    async def health_check():
        async with await s_telepath.openurl(args.url) as prox:
            info = await asyncio.wait_for(prox.getCellInfo(), timeout=5)
            if not info['cell'].get('active'):
                raise RuntimeError('Cortex is not active')
        pids = _get_worker_pids(cortex_pid)
        for pid in pids:
            print(f'    worker pid={pid}')
        return f'Cortex active, {len(pids)} workers running'

    steps.append(await run_step('1. Health check', health_check))
    if _shutdown.is_set():
        return 1

    # --- Step 2: Seed 100 nodes ---
    async def seed_nodes():
        async with await s_telepath.openurl(args.url) as prox:
            for i in range(100):
                async for _ in prox.storm(f'[inet:fqdn=recovery-{i}.test.com]'):
                    pass
            count = 0
            async for mesg in prox.storm('inet:fqdn | count'):
                if mesg[0] == 'print':
                    m = re.search(r'(\d+)', mesg[1]['mesg'])
                    if m:
                        count = int(m.group(1))
        if count < 100:
            raise RuntimeError(f'Expected >=100 nodes, got {count}')
        return f'{count} inet:fqdn nodes seeded and verified'

    steps.append(await run_step('2. Seed 100 nodes', seed_nodes))
    if _shutdown.is_set():
        return 1

    # --- Step 3: Kill worker 1 ---
    killed_pid = worker_pids[0]

    async def kill_worker_1():
        os.kill(killed_pid, signal.SIGKILL)
        _wait_pid_gone(killed_pid)
        return f'Killed worker PID {killed_pid} (confirmed dead)'

    steps.append(await run_step('3. Kill worker 1', kill_worker_1))
    if _shutdown.is_set():
        return 1

    # --- Step 4: 10 reads through shared port (failover) ---
    async def reads_after_kill():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for _ in range(10):
                try:
                    cnt = await _storm_count(prox, 'inet:fqdn | limit 5')
                    if cnt > 0:
                        successes += 1
                except Exception:
                    pass
        if successes < _FAILOVER_MIN_SUCCESSES:
            raise RuntimeError(f'Only {successes}/10 reads succeeded (minimum: {_FAILOVER_MIN_SUCCESSES})')
        return f'{successes}/10 reads succeeded'

    steps.append(await run_step('4. Reads after kill (failover)', reads_after_kill))
    if _shutdown.is_set():
        return 1

    # --- Step 5: Poll for worker respawn (new child PID appears) ---
    async def poll_respawn():
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _shutdown.is_set():
                raise RuntimeError('Interrupted')
            pids = _get_worker_pids(cortex_pid)
            if len(pids) >= len(worker_pids):
                new_pids = set(pids) - set(worker_pids)
                return f'Worker respawned: {len(pids)} workers (new PIDs: {new_pids or "recycled"})'
            await asyncio.sleep(2)
        raise RuntimeError(f'Worker count did not recover within 30s (have {len(_get_worker_pids(cortex_pid))}, need {len(worker_pids)})')

    steps.append(await run_step('5. Poll worker 1 respawn', poll_respawn))
    if _shutdown.is_set():
        return 1

    # --- Step 6: Verify reads work after respawn ---
    async def verify_respawned():
        async with await s_telepath.openurl(args.url) as prox:
            count = await _storm_count(prox, 'inet:fqdn | limit 10')
            if count == 0:
                raise RuntimeError('Returned 0 nodes after respawn — possible data loss')
            specific = await _storm_count(prox, 'inet:fqdn=recovery-0.test.com')
            if specific == 0:
                raise RuntimeError('Missing seeded node recovery-0.test.com')
        return f'Reads verified: {count} nodes, specific node confirmed'

    steps.append(await run_step('6. Verify respawned worker', verify_respawned))
    if _shutdown.is_set():
        return 1

    # --- Step 7: Kill ALL workers ---
    async def kill_all_workers():
        current_pids = _get_worker_pids(cortex_pid)
        if not current_pids:
            raise RuntimeError('No worker PIDs found')
        killed = []
        for pid in current_pids:
            os.kill(pid, signal.SIGKILL)
            _wait_pid_gone(pid)
            killed.append(str(pid))
        return f'Killed all workers: PIDs {", ".join(killed)}'

    steps.append(await run_step('7. Kill ALL workers', kill_all_workers))
    if _shutdown.is_set():
        return 1

    # --- Step 8: 10 reads (writer fallback) ---
    async def reads_writer_fallback():
        successes = 0
        async with await s_telepath.openurl(args.url) as prox:
            for _ in range(10):
                try:
                    cnt = await _storm_count(prox, 'inet:fqdn | limit 5')
                    if cnt > 0:
                        successes += 1
                except Exception:
                    pass
        if successes < _FAILOVER_MIN_SUCCESSES:
            raise RuntimeError(f'Only {successes}/10 reads succeeded (minimum: {_FAILOVER_MIN_SUCCESSES})')
        return f'{successes}/10 reads succeeded (writer fallback)'

    steps.append(await run_step('8. Reads (writer fallback)', reads_writer_fallback))
    if _shutdown.is_set():
        return 1

    # --- Step 9: Poll all workers respawn ---
    async def poll_all_respawn():
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _shutdown.is_set():
                raise RuntimeError('Interrupted')
            pids = _get_worker_pids(cortex_pid)
            if len(pids) >= len(worker_pids):
                return f'All workers respawned: {len(pids)} workers'
            await asyncio.sleep(2)
        current = len(_get_worker_pids(cortex_pid))
        raise RuntimeError(f'Workers not fully respawned within 30s ({current}/{len(worker_pids)})')

    steps.append(await run_step('9. Poll all workers respawn', poll_all_respawn))
    if _shutdown.is_set():
        return 1

    # --- Step 10: Final health check ---
    async def final_health():
        async with await s_telepath.openurl(args.url) as prox:
            info = await asyncio.wait_for(prox.getCellInfo(), timeout=5)
            if not info['cell'].get('active'):
                raise RuntimeError('Cortex is not active after recovery')
        pids = _get_worker_pids(cortex_pid)
        for pid in pids:
            print(f'    worker pid={pid}')
        return f'Cortex active, {len(pids)} workers healthy'

    steps.append(await run_step('10. Final health check', final_health))

    # --- Summary ---
    passed = sum(1 for s in steps if s.passed)
    failed = sum(1 for s in steps if not s.passed)
    total = len(steps)
    total_time = sum(s.elapsed for s in steps)

    print(f'\n{"Step":<40} {"Result":>6} {"Time":>8}')
    print('-' * 56)
    for s in steps:
        status = 'PASS' if s.passed else 'FAIL'
        print(f'{s.name:<40} {status:>6} {s.elapsed:>7.1f}s')
    print('-' * 56)
    print(f'{"Total":<40} {passed}/{total:>4} {total_time:>7.1f}s')

    all_pass = passed == total

    if args.output:
        report = {
            'url': args.url, 'cortex_pid': cortex_pid,
            'worker_pids': worker_pids, 'skipped': False,
            'steps': [s.as_dict() for s in steps],
            'passed': passed, 'failed': failed,
            'total': total, 'total_time_s': round(total_time, 3),
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'\nResults written to {args.output}')

    return 0 if all_pass else 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
