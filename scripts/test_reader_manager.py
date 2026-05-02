'''
Local test for ReaderManager — spawn, health-check, and restart readers.

Usage:
    python3 scripts/test_reader_manager.py
'''
import os
import sys
import signal
import socket
import asyncio
import tempfile

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import synapse.cortex as s_cortex
import synapse.telepath as s_telepath
import synapse.lib.readermanager as s_readermanager


def _find_free_base_port(count=2):
    '''Find a base port where count consecutive ports are all free.'''
    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            base = s.getsockname()[1]
        # Verify the next (count-1) ports are also free
        ok = True
        for i in range(1, count):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('127.0.0.1', base + i))
            except OSError:
                ok = False
                break
        if ok:
            return base
    raise RuntimeError(f'Could not find {count} consecutive free ports')


async def test_spawn_and_health():
    '''Test that ReaderManager spawns readers and they respond to getCellInfo.'''
    with tempfile.TemporaryDirectory() as tdir:
        async with await s_cortex.Cortex.anit(tdir, conf={'auth:anon': 'root'}) as core:
            await core.dmon.listen('tcp://127.0.0.1:0/')

            base_port = _find_free_base_port(2)
            mgr = await s_readermanager.ReaderManager.anit(tdir, count=2, base_port=base_port)
            try:
                await mgr.start()

                urls = mgr.get_reader_urls()
                assert len(urls) == 2, f'Expected 2 reader URLs, got {len(urls)}'

                for url in urls:
                    async with await s_telepath.openurl(url) as prox:
                        info = await prox.getCellInfo()
                        assert info is not None
                        print(f'  Reader {url} responded: celltype={info["cell"]["type"]}')

                print('PASS: spawn and health check')
            finally:
                await mgr.fini()


async def test_kill_and_restart():
    '''Test that killing a reader process triggers a respawn.'''
    with tempfile.TemporaryDirectory() as tdir:
        async with await s_cortex.Cortex.anit(tdir, conf={'auth:anon': 'root'}) as core:
            await core.dmon.listen('tcp://127.0.0.1:0/')

            base_port = _find_free_base_port(2)
            mgr = await s_readermanager.ReaderManager.anit(tdir, count=2, base_port=base_port)
            try:
                await mgr.start()

                # Kill the first reader
                port = base_port
                proc = mgr.procs[port]
                old_pid = proc.pid
                print(f'  Killing reader on port {port} (pid {old_pid})')
                os.kill(old_pid, signal.SIGKILL)
                proc.wait()

                # Manually trigger one health cycle
                for p in list(mgr.procs.keys()):
                    if not await mgr._check_reader(p):
                        mgr._kill_proc(p)
                        await mgr._spawn_reader(p)

                new_proc = mgr.procs.get(port)
                assert new_proc is not None, 'Reader was not respawned'
                assert new_proc.pid != old_pid, 'Reader PID did not change'

                url = f'tcp://127.0.0.1:{port}/cortex'
                async with await s_telepath.openurl(url) as prox:
                    info = await prox.getCellInfo()
                    assert info is not None

                print(f'  Reader respawned on port {port} (new pid {new_proc.pid})')
                print('PASS: kill and restart')
            finally:
                await mgr.fini()


async def main():
    print('=== Test: spawn and health ===')
    await test_spawn_and_health()
    print()
    print('=== Test: kill and restart ===')
    await test_kill_and_restart()
    print()
    print('All tests passed.')


if __name__ == '__main__':
    asyncio.run(main())
