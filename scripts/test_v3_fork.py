#!/usr/bin/env python3.11
'''
Reproduce and verify v3 fork bugs:

BUG 1: Forkserver pool workers (SpawnProcess-1/2/3) get SIGTERM during startup.
  Root cause: forkpool not shut down before os.fork() in arbiter.
  Fix: _shutdown_forkpool() called pre-fork in arbiter.fork_workers().

BUG 2: NoSuchObj: name=None — workers accept connections but cortex share
  isn't registered under the right names.
  Root cause: worker dmon only shared '*', not 'cortex' or other names.
  Fix: worker copies all parent dmon share names to its own dmon.

BUG 3: ~50% error rate under sustained soak load.
  Pool links (t2:init) land on different fork workers than the original
  tele:syn handshake.  The on-the-fly session bypasses getTeleApi() and
  stores the raw Cell instead of a CoreApi, causing auth/method failures.

This test replicates the exact fork sequence from cell.py startmain:
  Phase 1: asyncio.run(init) — init Cortex, loop closes on return
  Phase 2: os.fork() × N — fork read workers
  Phase 3: asyncio.run(serve) — writer event loop + soak test
'''
import io
import os
import sys
import time
import signal
import asyncio
import logging
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import synapse.cortex as s_cortex
import synapse.telepath as s_telepath
import synapse.lib.base as s_base
import synapse.lib.arbiter as s_arbiter
import synapse.lib.worker as s_worker

NUM_WORKERS = 2
WORKER_SETTLE_TIME = 3.0
SOAK_DURATION = 30


def main():
    # Capture stderr to detect SIGTERM messages (BUG 1 evidence)
    stderr_capture = io.StringIO()
    log_handler = logging.StreamHandler(stderr_capture)
    log_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(log_handler)

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

        # Seed test data
        async with await core.snap() as snap:
            for i in range(20):
                await snap.addNode('inet:fqdn', f'test{i}.example.com')

        dup_fd = os.dup(listen_fd)
        core._syn_refs += 1  # prevent fini during asyncio.run teardown
        # Also protect the nexsroot and its children from being fini'd
        # during asyncio.run teardown (they are Base objects with active
        # tasks that get cancelled when the init loop closes).
        # Use +3 to survive multiple fini() calls from different teardown paths.
        core.nexsroot._syn_refs += 3

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
    stderr_capture.truncate(0)
    stderr_capture.seek(0)

    def _worker_entry(listen_sock, uds_path_arg, worker_id):
        s_worker.worker_main(listen_sock, uds_path_arg, datadir, cell=cell)

    print(f'[phase2] Forking {NUM_WORKERS} workers...')
    arbiter = s_arbiter.Arbiter()
    pids = arbiter.fork_workers(NUM_WORKERS, listen_fd, uds_path, _worker_entry)
    print(f'[phase2] Worker PIDs: {pids}')

    # BUG 1 CHECK: Assert workers survive the settle period (no SIGTERM)
    print(f'[check] Waiting {WORKER_SETTLE_TIME}s for workers to settle...')
    time.sleep(WORKER_SETTLE_TIME)

    dead = []
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            dead.append(pid)

    if dead:
        print(f'FAIL BUG 1: {len(dead)} worker(s) died within {WORKER_SETTLE_TIME}s')
        os._exit(1)

    print(f'[check] BUG 1 OK: All {NUM_WORKERS} workers alive')

    captured = stderr_capture.getvalue()
    if 'SIGTERM' in captured or 'SpawnProcess' in captured:
        print(f'FAIL BUG 1: SIGTERM or SpawnProcess found in logs')
        os._exit(1)

    # Phase 3: Writer serves, verify telepath + soak test
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
        cell.nexsroot._syn_refs -= 3

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

        # Match real cell.py: re-fire active coros and start the nexus
        cell._fireActiveCoros()
        if not cell.nexsroot.started:
            await cell.nexsroot.recover()
            await cell.nexsroot.startup()

        # BUG 2 CHECK: Verify telepath query with specific name 'cortex'
        try:
            async with await s_telepath.openurl(f'tcp://127.0.0.1:{port}/cortex') as prox:
                info = await prox.getCellInfo()
                assert info is not None
                ctype = info.get('cell', {}).get('type', '')
                assert ctype == 'cortex', f'Expected cortex, got {ctype}'
                print(f'[phase3] BUG 2 OK: Telepath /cortex query succeeded')
        except Exception as e:
            print(f'FAIL BUG 2: Telepath /cortex query failed: {e}')
            return

        # CRITERION 6 CHECK: Write forwarding via UDS — read-after-write
        # Retry up to 3 times — the writer's nexus may need a moment to
        # re-initialize after the fork (pre-existing race condition).
        for attempt in range(3):
            try:
                async with await s_telepath.openurl(f'tcp://127.0.0.1:{port}/cortex') as prox:
                    # Write a node (should be forwarded to writer via UDS)
                    await prox.callStorm('[ inet:fqdn=write-test.com ]')
                    print(f'[phase3] Write forwarding: callStorm succeeded')

                # Read it back via writer UDS (workers have stale LMDB snapshots)
                async with await s_telepath.openurl(f'unix://{uds_path}') as wprox:
                    found = False
                    async for mesg in wprox.storm('inet:fqdn=write-test.com'):
                        if mesg[0] == 'node':
                            found = True
                    if found:
                        print(f'[phase3] CRITERION 6 OK: read-after-write succeeded')
                    else:
                        print(f'FAIL CRITERION 6: wrote write-test.com but read returned no node')
                        return
                break
            except Exception as e:
                if attempt < 2:
                    print(f'[phase3] CRITERION 6 attempt {attempt+1} failed ({e}), retrying...')
                    await asyncio.sleep(1.0)
                else:
                    print(f'FAIL CRITERION 6: read-after-write failed after 3 attempts: {e}')
                    return

        # READONLY:TRUE CHECKS — verify the try-forward mechanism
        try:
            async with await s_telepath.openurl(f'tcp://127.0.0.1:{port}/cortex') as prox:

                # 1. Read query executes locally (no IsReadOnly error in stream)
                mesgs = []
                async for mesg in prox.storm('$lib.print(hello)'):
                    mesgs.append(mesg)
                errs = [m for m in mesgs if m[0] == 'err']
                prints = [m for m in mesgs if m[0] == 'print']
                assert not errs, f'Read query produced errors: {errs}'
                assert len(prints) > 0, f'Read query returned no print (got {[m[0] for m in mesgs]})'
                print(f'[readonly] CHECK 1 OK: read query local, {len(prints)} prints, no errors')

                # 2. Write via storm (not callStorm) — IsReadOnly caught, forwarded
                mesgs = []
                async for mesg in prox.storm('[ inet:fqdn=readonly-storm-test.com ]'):
                    mesgs.append(mesg)
                errs = [m for m in mesgs if m[0] == 'err' and m[1][0] == 'IsReadOnly']
                nodes = [m for m in mesgs if m[0] == 'node']
                assert not errs, f'IsReadOnly leaked to client: {errs}'
                assert len(nodes) > 0, 'Write-via-storm returned no nodes after forward'
                print(f'[readonly] CHECK 2 OK: write via storm forwarded, {len(nodes)} nodes')

                # 3. Mixed query — read with write side-effect (lib function)
                #    $lib.print() is readonly-safe, but node creation is not
                mesgs = []
                async for mesg in prox.storm('[ inet:fqdn=readonly-mixed-test.com ] | limit 1'):
                    mesgs.append(mesg)
                errs = [m for m in mesgs if m[0] == 'err' and m[1][0] == 'IsReadOnly']
                nodes = [m for m in mesgs if m[0] == 'node']
                assert not errs, f'IsReadOnly leaked to client in mixed query: {errs}'
                assert len(nodes) > 0, 'Mixed query returned no nodes after forward'
                print(f'[readonly] CHECK 3 OK: mixed query forwarded, {len(nodes)} nodes')

                # 4. callStorm write — IsReadOnly exception caught, forwarded
                result = await prox.callStorm('[ inet:fqdn=readonly-callstorm-test.com ] return($node.repr())')
                assert result is not None, 'callStorm write returned None'
                print(f'[readonly] CHECK 4 OK: callStorm write forwarded, result={result}')

        except Exception as e:
            print(f'FAIL READONLY: {e}')
            import traceback; traceback.print_exc()
            return

        # BUG 3 CHECK: Soak test — sustained load with multiple concurrent proxies
        # to stress pool link distribution across fork workers
        CONCURRENCY = 4
        print(f'[soak] Running {SOAK_DURATION}s sustained load test ({CONCURRENCY} concurrent proxies)...')
        errors = 0
        total = 0
        error_types = {}
        lock = asyncio.Lock()

        async def soak_worker(worker_id):
            nonlocal errors, total
            deadline = time.monotonic() + SOAK_DURATION
            async with await s_telepath.openurl(f'tcp://127.0.0.1:{port}/cortex') as prox:
                while time.monotonic() < deadline:
                    async with lock:
                        total += 1
                        my_total = total
                    try:
                        count = 0
                        async for mesg in prox.storm('inet:fqdn | limit 5'):
                            if mesg[0] == 'node':
                                count += 1
                    except Exception as e:
                        async with lock:
                            errors += 1
                            my_errors = errors
                            etype = f'{type(e).__name__}: {e}'
                            error_types[etype] = error_types.get(etype, 0) + 1
                            if my_errors <= 5:
                                print(f'  Error {my_errors} (w{worker_id}): {etype}')
                    await asyncio.sleep(0.01)

        await asyncio.gather(*[soak_worker(i) for i in range(CONCURRENCY)])

        rate = errors / total * 100 if total else 0
        print(f'[soak] Total: {total}, Errors: {errors}, Rate: {rate:.1f}%')
        if error_types:
            print(f'[soak] Error breakdown:')
            for etype, cnt in sorted(error_types.items(), key=lambda x: -x[1]):
                print(f'  {cnt:4d}x {etype}')

        if rate > 1.0:
            print(f'FAIL BUG 3: Error rate {rate:.1f}% exceeds 1% threshold')
        else:
            print(f'[soak] BUG 3 OK: Error rate {rate:.1f}% < 1%')
            test_ok = True

        # Verify workers still alive
        for pid in pids:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                print(f'FAIL: worker {pid} died during soak')
                test_ok = False

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
        print('\nPASS: All bugs fixed')
        os._exit(0)
    else:
        print('\nFAIL: Test did not complete successfully')
        os._exit(1)


if __name__ == '__main__':
    main()
