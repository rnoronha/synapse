#!/usr/bin/env python3.11
'''
Integration test for the thick router (cell:router:mode='thick').

The thick router owns all client connections on a separate port,
classifies queries, and dispatches reads to workers / writes to the writer
via socketpair RPC.

This test:
1. Starts a Cortex with cell:router:mode='thick' on a temp directory
2. Connects via telepath to the thick router port
3. Verifies: read queries, write queries, IsReadOnly re-dispatch,
   streaming results, mixed read/write workload
4. Runs a 30s soak test
5. Reports PASS/FAIL

NOT pytest — standalone script. Run with: python3.11 scripts/test_thick_router.py
'''
import gc
import os
import sys
import time
import signal
import socket
import asyncio
import logging
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import msgpack
import synapse.cortex as s_cortex
import synapse.daemon as s_daemon
import synapse.telepath as s_telepath
import synapse.lib.base as s_base
import synapse.lib.arbiter as s_arbiter
import synapse.lib.worker as s_worker
import synapse.lib.lmdbslab as s_lmdbslab
import synapse.lib.msgpack as s_msgpack

NUM_WORKERS = 2
WORKER_SETTLE_TIME = 4.0
SOAK_DURATION = 30

logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(name)s: %(message)s')


def _thick_router_main_with_cell(listen_fd, worker_dispatch_fds, writer_dispatch_fd, cell):
    '''
    Thick router entry point that uses the inherited cell for Daemon sharing.
    This fixes the cell=None issue in the stock _thick_router_main.
    '''
    from synapse.lib.thickrouter import ThickRouter

    async def _run():
        router = ThickRouter(
            cell=cell,
            worker_fds=worker_dispatch_fds,
            writer_fd=writer_dispatch_fd,
        )
        await router.start()

        # Listen on the inherited socket fd
        sock = socket.socket(fileno=listen_fd)
        sock.setblocking(False)
        await router.listen(f'tcp://0.0.0.0', ssl=None, sock=sock)

        # Block until shutdown signal
        stop_event = asyncio.Event()

        def _on_term():
            stop_event.set()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, _on_term)
        loop.add_signal_handler(signal.SIGINT, _on_term)

        await stop_event.wait()
        await router.stop()

    asyncio.run(_run())


def main():
    td = tempfile.mkdtemp(prefix='thickrouter-')
    print(f'[parent pid={os.getpid()}] tempdir: {td}')

    # Phase 1: Init the Cortex
    async def init_cortex():
        core = await s_cortex.Cortex.anit(td, conf={
            'auth:anon': 'root',
            'cell:router:mode': 'thick',
        })
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

        # Create thick router listen socket on dynamic port
        thick_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        thick_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        thick_sock.bind(('0.0.0.0', 0))
        thick_sock.listen(128)
        thick_sock.setblocking(False)
        thick_port = thick_sock.getsockname()[1]

        dup_fd = os.dup(listen_fd)
        core._syn_refs += 1
        core.nexsroot._syn_refs += 3

        return {
            'cell': core, 'listen_fd': dup_fd, 'uds_path': uds_path,
            'datadir': td, 'port': port,
            'thick_listen_fd': thick_sock.fileno(),
            'thick_sock': thick_sock, 'thick_port': thick_port,
        }

    print('[phase1] Initializing Cortex (thick router mode)...')
    info = asyncio.run(init_cortex())
    cell = info['cell']
    listen_fd = info['listen_fd']
    uds_path = info['uds_path']
    port = info['port']
    thick_listen_fd = info['thick_listen_fd']
    thick_sock = info['thick_sock']
    thick_port = info['thick_port']
    cell.loop = None

    print(f'[phase1] Done. main_port={port} thick_port={thick_port}')

    # Phase 2: Fork workers and thick router
    def _worker_entry(control_fd, uds_path_arg, worker_id, write_fd=None, dispatch_fd=None):
        s_worker.worker_main(control_fd, uds_path_arg, info['datadir'], cell=cell,
                             write_fd=write_fd, dispatch_fd=dispatch_fd)

    print(f'[phase2] Forking {NUM_WORKERS} workers...')
    arbiter = s_arbiter.Arbiter(router_mode='thick')
    pids = arbiter.fork_workers(NUM_WORKERS, listen_fd, uds_path, _worker_entry)
    print(f'[phase2] Worker PIDs: {pids}')

    # Fork thin router
    router_pid = arbiter.fork_router(listen_fd)
    print(f'[phase2] Thin router PID: {router_pid}')

    # Fork thick router manually (with cell available)
    # Build dispatch fds like arbiter.fork_thick_router does
    dispatch_fds = {}
    for wid, (router_fd, _) in arbiter._dispatch_channels.items():
        dispatch_fds[wid] = router_fd
    writer_dispatch_fd = arbiter._thick_writer_channel[0]

    thick_router_pid = os.fork()
    if thick_router_pid == 0:
        # child — thick router
        try:
            # Close worker-end dispatch fds
            for wid, (_, worker_fd) in arbiter._dispatch_channels.items():
                if worker_fd >= 0:
                    try:
                        os.close(worker_fd)
                    except OSError:
                        pass
            # Close writer-end of thick writer channel
            if arbiter._thick_writer_channel[1] >= 0:
                try:
                    os.close(arbiter._thick_writer_channel[1])
                except OSError:
                    pass
            # Close control channels
            for wid, (r_fd, w_fd) in arbiter._control_channels.items():
                for fd in (r_fd, w_fd):
                    if fd >= 0:
                        try:
                            os.close(fd)
                        except OSError:
                            pass
            # Close write channels
            for wid, (w_worker_fd, w_writer_fd) in arbiter._write_channels.items():
                for fd in (w_worker_fd, w_writer_fd):
                    if fd >= 0:
                        try:
                            os.close(fd)
                        except OSError:
                            pass

            # Rebind Base objects to new loop in thick router process
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            for obj in gc.get_objects():
                if isinstance(obj, s_base.Base) and obj.anitted:
                    obj.loop = new_loop
                    obj.finievt = asyncio.Event()
            cell.loop = new_loop

            # Re-open LMDB slabs readonly for the thick router
            import lmdb as _lmdb
            for slab in list(s_lmdbslab.Slab.allslabs.values()):
                path = slab.path
                try:
                    slab.lenv = _lmdb.open(str(path), map_size=slab.mapsize,
                        max_dbs=128, max_readers=256, writemap=False,
                        readonly=True, readahead=slab.readahead)
                    slab.readonly = True
                    slab.isfini = False
                except Exception:
                    pass

            _thick_router_main_with_cell(thick_listen_fd, dispatch_fds,
                                         writer_dispatch_fd, cell)
        except Exception as e:
            import traceback; traceback.print_exc()
        finally:
            os._exit(0)

    # parent — close router-end dispatch fds
    for wid, (router_fd, worker_fd) in list(arbiter._dispatch_channels.items()):
        if router_fd >= 0:
            try:
                os.close(router_fd)
            except OSError:
                pass
            arbiter._dispatch_channels[wid] = (-1, worker_fd)
    if arbiter._thick_writer_channel[0] >= 0:
        try:
            os.close(arbiter._thick_writer_channel[0])
        except OSError:
            pass
        arbiter._thick_writer_channel = (-1, arbiter._thick_writer_channel[1])

    print(f'[phase2] Thick router PID: {thick_router_pid}')

    # Wait for processes to settle
    print(f'[check] Waiting {WORKER_SETTLE_TIME}s for processes to settle...')
    time.sleep(WORKER_SETTLE_TIME)

    dead = []
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            dead.append(pid)
    if dead:
        print(f'FAIL: {len(dead)} worker(s) died during settle')
        os._exit(1)
    try:
        os.kill(thick_router_pid, 0)
    except ProcessLookupError:
        print('FAIL: Thick router died during settle')
        os._exit(1)
    print(f'[check] All processes alive')

    # Phase 3: Writer serves + tests
    test_ok = False

    async def writer_serve():
        nonlocal test_ok
        import synapse.lib.coro as s_coro
        new_loop = asyncio.get_running_loop()
        for obj in gc.get_objects():
            if isinstance(obj, s_base.Base) and obj.anitted:
                obj.loop = new_loop
                obj.isfini = False
                obj.finievt = asyncio.Event()
                for attr in list(vars(obj)):
                    if attr == 'finievt':
                        continue
                    val = getattr(obj, attr, None)
                    if isinstance(val, asyncio.Event):
                        if isinstance(val, s_coro.Event):
                            setattr(obj, attr, s_coro.Event())
                        else:
                            setattr(obj, attr, asyncio.Event())
        cell.loop = new_loop
        cell._syn_refs -= 1
        cell.nexsroot._syn_refs -= 3

        s_arbiter._reopen_writer_slabs()
        print('[phase3] Writer slabs reopened')
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
        os.close(listen_fd)
        await cell.dmon.listen(f'unix://{uds_path}')
        print('[phase3] UDS listener ready')

        # Start write channel listener (worker → writer)
        import synapse.lib.writechannel as s_writechannel
        writer_fds = arbiter.get_writer_fds()
        wc_listener = None
        if writer_fds:
            wc_listener = s_writechannel.WriteChannelListener(cell, writer_fds)
            await wc_listener.start()

        # Start thick writer dispatch listener (thick router → writer)
        thick_writer_fd = arbiter.get_thick_writer_fd()
        thick_wc_listener = None
        if thick_writer_fd is not None:
            thick_wc_listener = s_writechannel.WriteChannelListener(
                cell, [thick_writer_fd], send_ready=True)
            await thick_wc_listener.start()

        cell._fireActiveCoros()
        if not cell.nexsroot.started:
            await cell.nexsroot.recover()
            await cell.nexsroot.startup()

        # Give thick router time to start its event loop and listen
        await asyncio.sleep(2.0)
        print('[phase3] Writer ready, starting tests...')

        url = f'tcp://127.0.0.1:{thick_port}/cortex'

        # --- TEST 1: Read queries return results ---
        try:
            async with await s_telepath.openurl(url) as prox:
                mesgs = []
                async for mesg in prox.storm('inet:fqdn | limit 5'):
                    mesgs.append(mesg)
                nodes = [m for m in mesgs if m[0] == 'node']
                errs = [m for m in mesgs if m[0] == 'err']
                assert not errs, f'Read query errors: {errs}'
                assert len(nodes) == 5, f'Expected 5 nodes, got {len(nodes)}'
                print(f'[test1] PASS: Read queries return results ({len(nodes)} nodes)')
        except Exception as e:
            print(f'FAIL TEST 1 (read queries): {e}')
            import traceback; traceback.print_exc()
            return

        # --- TEST 2: Write queries succeed and data persists ---
        try:
            async with await s_telepath.openurl(url) as prox:
                mesgs = []
                async for mesg in prox.storm('[ inet:fqdn=thick-write-test.com ]'):
                    mesgs.append(mesg)
                nodes = [m for m in mesgs if m[0] == 'node']
                errs = [m for m in mesgs if m[0] == 'err']
                assert not errs, f'Write query errors: {errs}'
                assert len(nodes) == 1, f'Expected 1 node, got {len(nodes)}'

            # Verify persistence via writer UDS
            async with await s_telepath.openurl(f'unix://{uds_path}') as wprox:
                found = False
                async for mesg in wprox.storm('inet:fqdn=thick-write-test.com'):
                    if mesg[0] == 'node':
                        found = True
                assert found, 'Write did not persist'
                print(f'[test2] PASS: Write queries succeed and data persists')
        except Exception as e:
            print(f'FAIL TEST 2 (write queries): {e}')
            import traceback; traceback.print_exc()
            return

        # --- TEST 3: IsReadOnly re-dispatch ---
        try:
            async with await s_telepath.openurl(url) as prox:
                result = await prox.callStorm(
                    '[ inet:fqdn=readonly-redispatch.com ] return($node.repr())')
                assert result is not None, 'callStorm returned None'
                print(f'[test3] PASS: IsReadOnly re-dispatch works (result={result})')
        except Exception as e:
            print(f'FAIL TEST 3 (IsReadOnly re-dispatch): {e}')
            import traceback; traceback.print_exc()
            return

        # --- TEST 4: Streaming results arrive incrementally ---
        try:
            async with await s_telepath.openurl(url) as prox:
                mesgs = []
                async for mesg in prox.storm(
                        '[ inet:fqdn=stream1.thick.com inet:fqdn=stream2.thick.com inet:fqdn=stream3.thick.com ]'):
                    mesgs.append(mesg)
                nodes = [m for m in mesgs if m[0] == 'node']
                errs = [m for m in mesgs if m[0] == 'err']
                finis = [m for m in mesgs if m[0] == 'fini']
                assert not errs, f'Streaming errors: {errs}'
                assert len(nodes) == 3, f'Expected 3 nodes, got {len(nodes)}'
                assert len(finis) == 1, f'Expected 1 fini, got {len(finis)}'
                print(f'[test4] PASS: Streaming results ({len(nodes)} nodes, fini received)')
        except Exception as e:
            print(f'FAIL TEST 4 (streaming): {e}')
            import traceback; traceback.print_exc()
            return

        # --- TEST 5: Mixed read/write workload on same connection ---
        try:
            async with await s_telepath.openurl(url) as prox:
                # Write
                mesgs = []
                async for mesg in prox.storm('[ inet:fqdn=mixed-thick-w1.com ]'):
                    mesgs.append(mesg)
                assert len([m for m in mesgs if m[0] == 'node']) == 1

                # Read
                mesgs = []
                async for mesg in prox.storm('$lib.print(between)'):
                    mesgs.append(mesg)
                assert not [m for m in mesgs if m[0] == 'err']

                # Write
                mesgs = []
                async for mesg in prox.storm('[ inet:fqdn=mixed-thick-w2.com ]'):
                    mesgs.append(mesg)
                assert len([m for m in mesgs if m[0] == 'node']) == 1

                # Read
                mesgs = []
                async for mesg in prox.storm('$lib.print(alive)'):
                    mesgs.append(mesg)
                assert len([m for m in mesgs if m[0] == 'print']) == 1

                # callStorm write
                result = await prox.callStorm(
                    '[ inet:fqdn=mixed-thick-w3.com ] return($node.repr())')
                assert result is not None

                print(f'[test5] PASS: Mixed read/write workload on same connection')
        except Exception as e:
            print(f'FAIL TEST 5 (mixed workload): {e}')
            import traceback; traceback.print_exc()
            return

        # --- TEST 6: Soak test ---
        CONCURRENCY = 4
        print(f'[soak] Running {SOAK_DURATION}s sustained load ({CONCURRENCY} proxies)...')
        errors = 0
        total = 0
        error_types = {}
        lock = asyncio.Lock()

        async def soak_worker(worker_id):
            nonlocal errors, total
            deadline = time.monotonic() + SOAK_DURATION
            try:
                async with await s_telepath.openurl(url) as prox:
                    while time.monotonic() < deadline:
                        async with lock:
                            total += 1
                        try:
                            count = 0
                            async for mesg in prox.storm('inet:fqdn | limit 5'):
                                if mesg[0] == 'node':
                                    count += 1
                        except Exception as e:
                            async with lock:
                                errors += 1
                                etype = f'{type(e).__name__}: {e}'
                                error_types[etype] = error_types.get(etype, 0) + 1
                                if errors <= 5:
                                    print(f'  Error {errors} (w{worker_id}): {etype}')
                        await asyncio.sleep(0.01)
            except Exception as e:
                async with lock:
                    errors += 1
                    etype = f'{type(e).__name__}: {e}'
                    error_types[etype] = error_types.get(etype, 0) + 1

        await asyncio.gather(*[soak_worker(i) for i in range(CONCURRENCY)])

        rate = errors / total * 100 if total else 0
        print(f'[soak] Total: {total}, Errors: {errors}, Rate: {rate:.1f}%')
        if error_types:
            print(f'[soak] Error breakdown:')
            for etype, cnt in sorted(error_types.items(), key=lambda x: -x[1]):
                print(f'  {cnt:4d}x {etype}')

        if rate > 1.0:
            print(f'FAIL SOAK: Error rate {rate:.1f}% exceeds 1%')
            return

        print(f'[soak] PASS: Error rate {rate:.1f}% < 1%')

        # Verify processes still alive
        for pid in pids:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                print(f'FAIL: worker {pid} died during soak')
                return
        try:
            os.kill(thick_router_pid, 0)
        except ProcessLookupError:
            print(f'FAIL: thick router died during soak')
            return

        test_ok = True

        if thick_wc_listener is not None:
            await thick_wc_listener.stop()
        if wc_listener is not None:
            await wc_listener.stop()
        arbiter.shutdown()
        await cell.fini()

    try:
        asyncio.run(writer_serve())
    finally:
        for pid in pids + [thick_router_pid, router_pid]:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    if test_ok:
        print('\nPASS: All thick router tests passed')
        os._exit(0)
    else:
        print('\nFAIL: Test did not complete successfully')
        os._exit(1)


if __name__ == '__main__':
    main()
