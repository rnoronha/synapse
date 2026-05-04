#!/usr/bin/env python3.11
'''
Reproduce and verify the v3 fork worker bug.

Root cause: worker.py used select.EPOLLEXCLUSIVE unconditionally,
which doesn't exist in Python < 3.12. Workers crashed immediately
after fork with AttributeError. The "Caught SIGTERM" messages in
EC2 logs were from forkserver pool processes during Phase 1 teardown,
not from the forked workers — a red herring.

Fix: graceful degradation to plain EPOLLIN when EPOLLEXCLUSIVE is
unavailable.

This test replicates the exact fork sequence from cell.py _runForkMode:
  Phase 1: asyncio.run(init) — init Cortex, loop closes on return
  Phase 2: os.fork() × N — fork read workers
  Phase 3: asyncio.run(serve) — writer event loop
'''
import os
import sys
import time
import signal
import asyncio
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.cortex as s_cortex
import synapse.telepath as s_telepath
import synapse.lib.base as s_base
import synapse.lib.arbiter as s_arbiter
import synapse.lib.worker as s_worker

NUM_WORKERS = 2
WORKER_SETTLE_TIME = 3.0


def main():
    td = tempfile.mkdtemp(prefix='v3fork-')
    print(f'[parent pid={os.getpid()}] tempdir: {td}')

    # Phase 1: Init the Cortex (mirrors _initForFork)
    async def init_cortex():
        core = await s_cortex.Cortex.anit(td, conf={'auth:anon': 'root'})
        await core.dmon.listen('tcp://0.0.0.0:0/')

        listen_fd = port = None
        for server in core.dmon.listenservers:
            for sock in server.sockets:
                if sock.family in (2, 10):
                    listen_fd = sock.fileno()
                    port = sock.getsockname()[1]
                    break
            if listen_fd is not None:
                break
        assert listen_fd is not None

        uds_path = os.path.join(td, 'worker.sock')
        await core.dmon.listen(f'unix://{uds_path}')
        dup_fd = os.dup(listen_fd)
        core._syn_refs += 1  # prevent fini during asyncio.run teardown

        return {'cell': core, 'listen_fd': dup_fd, 'uds_path': uds_path,
                'datadir': td, 'port': port}

    print('[phase1] Initializing Cortex...')
    info = asyncio.run(init_cortex())
    cell, listen_fd, uds_path, datadir, port = (
        info['cell'], info['listen_fd'], info['uds_path'],
        info['datadir'], info['port'])
    cell.loop = None  # E-1 fix: null stale loop ref

    print(f'[phase1] Done. port={port} isfini={cell.isfini} refs={cell._syn_refs}')

    # Phase 2: Fork workers (mirrors _runForkMode)
    def _worker_entry(listen_sock, uds_path_arg, worker_id):
        s_worker.worker_main(listen_sock, uds_path_arg, datadir, cell=cell)

    print(f'[phase2] Forking {NUM_WORKERS} workers...')
    arbiter = s_arbiter.Arbiter()
    pids = arbiter.fork_workers(NUM_WORKERS, listen_fd, uds_path, _worker_entry)
    print(f'[phase2] Worker PIDs: {pids}')

    # Assert workers survive the settle period
    print(f'[check] Waiting {WORKER_SETTLE_TIME}s...')
    time.sleep(WORKER_SETTLE_TIME)

    dead = []
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            dead.append(pid)

    if dead:
        print(f'FAIL: {len(dead)} worker(s) died within {WORKER_SETTLE_TIME}s')
        for pid in dead:
            try:
                rpid, status = os.waitpid(pid, os.WNOHANG)
                if rpid != 0:
                    if os.WIFSIGNALED(status):
                        print(f'  pid {pid}: signal {os.WTERMSIG(status)}')
                    else:
                        print(f'  pid {pid}: exit code {os.WEXITSTATUS(status)}')
            except ChildProcessError:
                print(f'  pid {pid}: already reaped')
        os._exit(1)

    print(f'[check] All {NUM_WORKERS} workers alive')

    # Phase 3: Writer serves, verify telepath (mirrors _writer_serve)
    test_ok = False

    async def writer_serve():
        nonlocal test_ok
        import gc
        new_loop = asyncio.get_running_loop()
        for obj in gc.get_objects():
            if isinstance(obj, s_base.Base) and obj.anitted:
                obj.loop = new_loop
                obj.finievt = asyncio.Event()
        cell.loop = new_loop
        cell._syn_refs -= 1

        s_arbiter._reopen_writer_slabs()
        arbiter.install_loop_signal_handler(cell.loop)

        cell.dmon.listenservers.clear()
        for spath in (os.path.join(cell.dirn, 'sock'), uds_path):
            try:
                os.unlink(spath)
            except FileNotFoundError:
                pass
        try:
            await cell.dmon.listen(f'unix://{os.path.join(cell.dirn, "sock")}')
        except OSError:
            pass
        await cell._restoreDmonListener(listen_fd)
        await cell.dmon.listen(f'unix://{uds_path}')

        # Verify telepath
        async with await s_telepath.openurl(f'tcp://127.0.0.1:{port}/cortex') as prox:
            info = await prox.getCellInfo()
            assert info is not None, 'getCellInfo returned None'
            print(f'[phase3] Telepath OK')

        # Verify workers still alive after serving
        for pid in pids:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                print(f'FAIL: worker {pid} died during phase 3')
                return

        print(f'[phase3] All workers still alive')
        test_ok = True

        arbiter.shutdown()
        await cell.fini()

    try:
        asyncio.run(writer_serve())
    finally:
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    if test_ok:
        print('\nPASS: Workers survived fork, settle, and telepath serving')
        os._exit(0)
    else:
        print('\nFAIL: Workers did not survive')
        os._exit(1)


if __name__ == '__main__':
    main()
