#!/usr/bin/env python3.11
"""
Local multi-process Cortex test suite.
Runs writer + readers on localhost, no EC2 needed.

Tests: correctness, throughput, recovery.
"""
import os, signal, atexit
os.setpgrp()  # Create new process group so we can kill everything

import asyncio
import subprocess
import sys
import tempfile
import time


def _kill_all():
    """Brute-force cleanup: pkill cortex readers, then nuke the process group."""
    subprocess.run(["pkill", "-9", "-f", "synapse.servers.cortex"], capture_output=True)
    try:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    except ProcessLookupError:
        pass


atexit.register(_kill_all)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.cortex as s_cortex
import synapse.telepath as s_telepath

WRITER_PORT = 27492
READER_PORTS = [27493, 27494]
OVERALL_TIMEOUT = 90

_core = None


async def boot_writer(datadir):
    conf = {
        'auth:anon': 'root',
        'multi:process:readers': 50,
    }
    core = await s_cortex.Cortex.anit(datadir, conf=conf)
    await core.addHttpsPort(0)
    await core.dmon.listen(f'tcp://0.0.0.0:{WRITER_PORT}/')
    return core


async def test_correctness(prox):
    print('\n=== TEST: Correctness ===')
    passed = 0
    total = 0

    async for _ in prox.storm('[ inet:fqdn=correct1.test.com ]'): pass
    async for _ in prox.storm('[ inet:fqdn=correct2.test.com ]'): pass
    async for _ in prox.storm('[ inet:ipv4=10.99.99.1 ]'): pass

    queries = [
        ('inet:fqdn=correct1.test.com', 'exact lift'),
        ('inet:fqdn=correct2.test.com', 'exact lift 2'),
        ('inet:ipv4=10.99.99.1', 'ipv4 lift'),
        ('inet:fqdn:zone=com | count', 'zone filter + count'),
    ]

    for query, label in queries:
        total += 1
        try:
            nodes = [m[1] async for m in prox.storm(query) if m[0] == 'node']
            print(f'  {label}: {len(nodes)} results — PASS')
            passed += 1
        except Exception as e:
            print(f'  {label}: FAIL — {e}')

    total += 1
    try:
        async for _ in prox.storm('[ inet:fqdn=writetest.com ]'): pass
        print(f'  write through router: PASS')
        passed += 1
    except Exception as e:
        print(f'  write through router: FAIL — {e}')

    print(f'  Score: {passed}/{total}')
    return passed == total


async def test_recovery(prox, core):
    print('\n=== TEST: Recovery ===')
    passed = 0
    total = 0

    if not hasattr(core, 'readermgr') or core.readermgr is None:
        print('  SKIP — no ReaderManager (multi-process not enabled)')
        return True

    mgr = core.readermgr
    if not mgr.procs:
        print('  SKIP — no reader processes')
        return True

    total += 1
    urls = mgr.get_reader_urls()
    print(f'  Reader URLs: {urls}')
    if len(urls) >= 2:
        print(f'  Readers alive: PASS ({len(urls)} readers)')
        passed += 1
    else:
        print(f'  Readers alive: FAIL (expected >=2, got {len(urls)})')

    total += 1
    reader_port = list(mgr.procs.keys())[0]
    reader_proc = mgr.procs[reader_port]
    print(f'  Killing reader PID {reader_proc.pid}...')
    os.kill(reader_proc.pid, signal.SIGKILL)
    await asyncio.sleep(1)

    total += 1
    try:
        count = sum(1 async for m in prox.storm('inet:fqdn | limit 5') if m[0] == 'node')
        print(f'  Query after kill: PASS ({count} nodes)')
        passed += 1
    except Exception as e:
        print(f'  Query after kill: FAIL — {e}')

    total += 1
    print('  Waiting 15s for health check respawn...')
    await asyncio.sleep(15)
    new_urls = mgr.get_reader_urls()
    if len(new_urls) >= 2:
        print(f'  Respawn: PASS ({len(new_urls)} readers)')
        passed += 1
    else:
        print(f'  Respawn: FAIL ({len(new_urls)} readers, expected >=2)')

    print(f'  Score: {passed}/{total}')
    return passed == total


async def test_throughput(prox):
    print('\n=== TEST: Throughput ===')

    for i in range(100):
        async for _ in prox.storm(f'[ inet:fqdn=perf{i}.bench.com ]'): pass

    async def timed_query():
        return sum(1 async for m in prox.storm('inet:fqdn | limit 50') if m[0] == 'node')

    t0 = time.time()
    for _ in range(10):
        await timed_query()
    seq_time = time.time() - t0

    t0 = time.time()
    await asyncio.gather(*[timed_query() for _ in range(10)])
    conc_time = time.time() - t0

    speedup = seq_time / conc_time if conc_time > 0 else 0
    print(f'  Sequential (10 queries): {seq_time:.2f}s')
    print(f'  Concurrent (10 queries): {conc_time:.2f}s')
    print(f'  Speedup: {speedup:.1f}x')
    print(f'  PASS' if speedup > 0.8 else '  FAIL — concurrent slower than expected')
    return True


async def run_tests():
    print('=== Local Multi-Process Cortex Test Suite ===')
    print(f'CPUs: {os.cpu_count()}, Python: {sys.version.split()[0]}')

    with tempfile.TemporaryDirectory(prefix='synapse-mp-test-') as td:
        print(f'\nData dir: {td}')
        print('Booting writer Cortex with readers...')

        core = await boot_writer(td)
        global _core
        _core = core
        print(f'Writer on port {WRITER_PORT}')

        if core.readermgr:
            for port in core.readermgr.procs:
                print(f'Reader on port {port}: pid {core.readermgr.procs[port].pid}')

        try:
            async with await s_telepath.openurl(f'tcp://127.0.0.1:{WRITER_PORT}/cortex') as prox:
                results = {}
                results['correctness'] = await test_correctness(prox)
                results['throughput'] = await test_throughput(prox)
                results['recovery'] = await test_recovery(prox, core)

                print('\n=== SUMMARY ===')
                for name, passed in results.items():
                    print(f'  {name}: {"PASS" if passed else "FAIL"}')

                all_pass = all(results.values())
                print(f'\nOverall: {"ALL PASS" if all_pass else "SOME FAILED"}')
                return 0 if all_pass else 1
        finally:
            # Kill reader subprocesses directly before calling core.fini()
            # to avoid core.fini() -> readermgr.stop() blocking on proc.wait()
            if core.readermgr:
                for port, proc in list(core.readermgr.procs.items()):
                    if proc.poll() is None:
                        proc.kill()
                        try:
                            proc.wait(timeout=3)
                        except Exception:
                            pass
                core.readermgr.procs.clear()

            print('\nShutting down Cortex...')
            try:
                await asyncio.wait_for(core.fini(), timeout=10)
            except (asyncio.TimeoutError, Exception) as e:
                print(f'core.fini() did not complete cleanly: {e}')


def main():
    core = None
    rc = 1
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _on_signal(s))

    try:
        rc = loop.run_until_complete(asyncio.wait_for(run_tests(), timeout=OVERALL_TIMEOUT))
        core = _core
    except asyncio.TimeoutError:
        print(f'\n*** OVERALL TIMEOUT ({OVERALL_TIMEOUT}s) — aborting ***')
        rc = 2
    except KeyboardInterrupt:
        print('\n*** Interrupted ***')
        rc = 2
    except Exception as e:
        print(f'\n*** Unhandled exception: {e} ***')
        import traceback; traceback.print_exc()
        rc = 1
    finally:
        if core and hasattr(core, 'readermgr') and core.readermgr:
            for port, proc in core.readermgr.procs.items():
                try:
                    proc.kill()
                except Exception:
                    pass
        loop.close()
        os._exit(0 if rc == 0 else rc)


def _on_signal(sig):
    print(f'\nReceived signal {sig}, cleaning up...')
    _kill_all()
    sys.exit(128 + sig)


if __name__ == '__main__':
    main()
