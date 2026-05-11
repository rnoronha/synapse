'''
Fork-mode lifecycle logic extracted from synapse.lib.cell.

This module implements the two-phase init → fork → serve lifecycle used
when a Cell (e.g. Cortex) is configured for multi-process operation.
'''
# Process-global flag: set to True in forked read-only worker processes.
# Checked by lmdbslab to suppress writes and force readonly slab opens.
is_readonly_worker = False
import os
import gc
import asyncio
import logging
import socket
import threading

import synapse.common as s_common
import synapse.telepath as s_telepath

import synapse.lib.base as s_base
import synapse.lib.link as s_link
import synapse.lib.output as s_output
import synapse.lib.lmdbslab as s_lmdbslab

logger = logging.getLogger(__name__)


def run(cls, argv, outp=None):
    '''Sync entry point that supports the init → fork → serve lifecycle.

    If the cell has fork mode enabled (getForkInfo() returns non-None),
    the lifecycle is:

    1. asyncio.run(init) — initialize the cell, event loop closes on return
    2. os.fork() × N — fork read workers (no event loop running, safe to fork)
    3. Writer: asyncio.run(serve) — new event loop for the writer process
    4. Workers: each creates its own event loop via worker_main()

    If fork mode is not enabled, falls back to the normal asyncio.run(execmain) path.
    '''
    if not may_fork(cls, argv):
        asyncio.run(cls.execmain(argv, outp=outp))
        return

    import synapse.lib.arbiter as s_arbiter
    import synapse.lib.worker as s_worker

    if outp is None:
        outp = s_output.stdout

    # Phase 1: Initialize the cell (event loop created and destroyed by asyncio.run)
    cell_info = asyncio.run(init_for_fork(cls, argv, outp=outp))

    cell = cell_info['cell']
    fork_info = cell_info.get('fork_info')

    if fork_info is None:
        # prepareFork() returned None — fall back to normal serve.
        async def _fallback_serve():
            cell.loop = asyncio.get_running_loop()
            cell.isfini = False
            cell.finievt = asyncio.Event()
            cell.dmon.isfini = False
            cell.dmon.finievt = asyncio.Event()
            await restore_after_init_loop(cell)
            await cell.main()
        asyncio.run(_fallback_serve())
        return

    # Phase 2: Fork workers (no event loop running — safe to fork)
    listen_fd = fork_info['listen_fd']
    uds_path = fork_info['uds_path']
    datadir = fork_info['datadir']
    count = fork_info['count']

    cell.loop = None

    def _worker_entry(control_fd, uds_path_arg, worker_id, write_fd=None, read_fd=None):
        s_worker.worker_main(control_fd, uds_path_arg, datadir, cell=cell, write_fd=write_fd, read_fd=read_fd)

    arbiter = s_arbiter.Arbiter()
    arbiter.fork_workers(count, listen_fd, uds_path, _worker_entry)

    # Router not needed — writer keeps dmon listener and forwards reads
    # to workers via ReadForwarder. This allows mirror connections to
    # reach the writer's dmon (which creates proper CellApi objects).

    # Phase 3: Writer process creates a new event loop and serves
    async def _writer_serve():

        import signal
        import faulthandler
        faulthandler.register(signal.SIGUSR1, all_threads=True)

        import synapse.glob as s_glob
        new_loop = asyncio.get_running_loop()

        s_glob._glob_loop = new_loop
        s_glob._glob_thrd = threading.current_thread()

        fini_count = 0
        new_tid = threading.current_thread().ident
        for obj in gc.get_objects():
            if isinstance(obj, s_base.Base) and obj.anitted:
                if obj.isfini:
                    fini_count += 1
                obj.loop = new_loop
                obj.tid = new_tid
                obj.isfini = False
                obj.finievt = asyncio.Event()
        cell.loop = new_loop
        if fini_count:
            logger.warning('Reset isfini on %d Base objects', fini_count)

        cell._syn_refs -= 1

        if hasattr(cell, 'nexsroot') and cell.nexsroot is not None:
            logger.info('Writer: nexsroot.isfini=%s nexsroot.readonly=%s',
                       cell.nexsroot.isfini, cell.nexsroot.readonly)
            # Clear stale mirror windows from init phase — mirrors will
            # reconnect and get fresh windows after the writer is serving.
            cell.nexsroot._linkmirrors.clear()
            cell.nexsroot._mirrors.clear()
            cell.nexsroot.donexslog = True

        s_arbiter._reopen_writer_slabs()

        # Re-open nexus slabs if fini'd during init loop teardown.
        if hasattr(cell, 'nexsroot') and cell.nexsroot is not None:
            import lmdb as _lmdb
            nexs_slabs = [cell.nexsroot.nexsslab]
            nexslog = cell.nexsroot.nexslog
            if nexslog is not None and getattr(nexslog, 'tailslab', None) is not None:
                nexs_slabs.append(nexslog.tailslab)
            for slab in nexs_slabs:
                if slab is None:
                    continue
                path = slab.path
                slab.isfini = False
                slab._syn_refs = 1
                try:
                    slab.lenv.close()
                except Exception:
                    pass
                slab.lenv = _lmdb.open(str(path),
                    map_size=slab.mapsize, max_dbs=128, max_readers=256,
                    writemap=True, readonly=False, readahead=slab.readahead,
                    map_async=True)
                slab._initCoXact()
                old_dbnames = dict(slab.dbnames)
                slab.dbnames = {None: (None, False)}
                for name, (db, dupsort) in old_dbnames.items():
                    if name is None:
                        continue
                    try:
                        newdb = slab.lenv.open_db(name.encode('utf8'), txn=slab.xact, dupsort=dupsort)
                        slab.dbnames[name] = (newdb, dupsort)
                    except Exception:
                        pass
                s_lmdbslab.Slab.allslabs[str(path)] = slab
                print(f'[writer] Re-opened nexus slab: {path}')

            # Refresh nexslog seqn db handles (they reference old LMDB handles)
            nexslog = cell.nexsroot.nexslog
            if nexslog is not None and hasattr(nexslog, 'tailseqn') and nexslog.tailseqn is not None:
                seqn = nexslog.tailseqn
                if seqn.slab and not seqn.slab.isfini:
                    seqn.db = seqn.slab.initdb('nexuslog')

        for slab in list(s_lmdbslab.Slab.allslabs.values()):
            if not slab.readonly and slab.xact is None:
                logger.warning('Slab %s had xact=None after reopen, re-initializing', slab.path)
                slab._initCoXact()

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
            logger.warning('Failed to re-create local unix socket')

        # Restore the dmon SSL listener (for mirrors and telepath clients)
        await restore_dmon_listener(cell, listen_fd)

        await cell.dmon.listen(f'unix://{uds_path}')

        writer_fds = arbiter.get_writer_fds()
        if writer_fds:
            import synapse.lib.writechannel as s_writechannel
            wc_listener = s_writechannel.WriteChannelListener(cell, writer_fds)
            await wc_listener.start()
            arbiter._wc_listener = wc_listener

        reader_fds = arbiter.get_reader_fds()
        if reader_fds:
            import synapse.lib.readchannel as s_readchannel
            rc_forwarder = s_readchannel.ReadForwarder(cell, reader_fds)
            await rc_forwarder.start()
            arbiter._rc_forwarder = rc_forwarder

        # Re-add cell cert path (removed by onfini during init loop teardown)
        if hasattr(cell, 'certpath'):
            import synapse.lib.certdir as s_certdir
            s_certdir.addCertPath(cell.certpath)

        # Restart outbound telepath clients (AHA) BEFORE cell activation.
        # setCellActive calls _setAhaActive which needs a working ahaclient.
        if cell.ahaclient is not None:
            old_client = cell.ahaclient
            old_client._fini_atexit = False
            old_client.anitted = False
            cell.ahaclient = None
            cell._fini_funcs = [f for f in cell._fini_funcs if f is not old_client.fini]
            import synapse.telepath as s_telepath
            s_telepath.aha_clients.clear()

        # Re-activate the cell based on its state at fork time.
        # Leaders (isactive=True at fork) get re-activated.
        # Followers (isactive=False, demoted by AHA) stay inactive.
        was_active = cell.isactive
        cell.isactive = False
        if hasattr(cell, 'activebase') and cell.activebase is not None:
            cell.activebase.isfini = True
            cell.activebase = None
        cell.nexslock = asyncio.Lock()
        if was_active:
            await cell.setCellActive(True)
            print(f'[writer] leader active={cell.isactive}')

        # Restore HTTPS/Tornado listeners (died with the init event loop).
        if hasattr(cell, 'httpds') and hasattr(cell, 'https_listeners'):
            old_listeners = list(cell.https_listeners)
            cell.httpds.clear()
            cell.https_listeners.clear()
            for info in old_listeners:
                try:
                    await cell.addHttpsPort(info['port'], host=info['host'])
                except Exception as e:
                    logger.warning('Writer: failed to restore HTTPS on %s:%s: %s', info['host'], info['port'], e)

        s_lmdbslab.Slab.synctask = None
        for slab in s_lmdbslab.Slab.allslabs.values():
            if not slab.readonly:
                await s_lmdbslab.Slab.initSyncLoop(slab)
                break

        # Restart the drive subprocess (died during fork)
        if hasattr(cell, 'drive') and cell.drive is not None:
            import synapse.lib.drive as s_drive
            path = os.path.join(cell.dirn, 'slabs', 'drive.lmdb')
            sockpath = os.path.join(cell.sockdirn, 'drive')
            try:
                os.unlink(sockpath)
            except FileNotFoundError:
                pass
            try:
                spawner = s_drive.FileDrive.spawner(base=cell, sockpath=sockpath)
                cell.drive = await spawner(path)
                cell.onfini(cell.drive.fini)
            except Exception as e:
                print(f'[writer] Drive restart failed: {e}')

        # Re-init AHA in background (don't block startup)
        async def _reinit_aha():
            try:
                await cell._initAhaRegistry()
                if cell.ahaclient is None:
                    return

                ahaname = cell.conf.get('aha:name')
                ahanetw = cell.conf.get('aha:network')
                ahalead = cell.conf.get('aha:leader')

                # Initial registration
                proxy = await cell.ahaclient.proxy(timeout=60)
                info = await cell.getAhaInfo()
                if ahaname:
                    await proxy.addAhaSvc(ahaname, info, network=ahanetw)
                if ahalead and cell.isactive:
                    await proxy.addAhaSvc(ahalead, info, network=ahanetw)
                if cell.ahasvcname:
                    await proxy.modAhaSvcInfo(cell.ahasvcname, {'ready': True})
                if ahalead and cell.isactive:
                    await proxy.modAhaSvcInfo(f'{ahalead}.{ahanetw}', {'ready': True})
                print(f'[writer] AHA registered and ready=True (svcname={cell.ahasvcname})')

                # Persistent registration loop (matches stock _runAhaRegLoop)
                async def _aha_reg_loop():
                    while not cell.isfini:
                        try:
                            p = await cell.ahaclient.proxy()
                            info = await cell.getAhaInfo()
                            await p.addAhaSvc(ahaname, info, network=ahanetw)
                            if cell.isactive and ahalead is not None:
                                await p.addAhaSvc(ahalead, info, network=ahanetw)
                            await p.waitfini()
                        except Exception:
                            await asyncio.sleep(1)

                asyncio.create_task(_aha_reg_loop())
            except Exception as e:
                print(f'[writer] AHA re-init failed: {e}')

        asyncio.create_task(_reinit_aha())

        async def _reinit_nexs_follower():
            '''Restart the nexsroot follower client (for mirrors pulling from leader).'''
            mirurl = cell.conf.get('mirror')
            if mirurl is None:
                return
            try:
                # Wait for AHA to be fully ready
                for _ in range(120):
                    if cell.ahaclient is not None:
                        try:
                            await cell.ahaclient.proxy(timeout=5)
                            break
                        except Exception:
                            pass
                    await asyncio.sleep(1)
                # Restart the nexsroot follower
                await cell.nexsroot.startup()
                print(f'[writer] Nexus follower restarted (mirror={mirurl})')
                # Check if client connected
                if cell.nexsroot.client is not None:
                    try:
                        await cell.nexsroot.client.proxy(timeout=10)
                        print('[writer] Nexus follower client connected!')
                    except Exception as e:
                        print(f'[writer] Nexus follower client NOT connected: {e}')
            except Exception as e:
                import traceback
                print(f'[writer] Nexus follower restart failed: {e}')
                traceback.print_exc()

        asyncio.create_task(_reinit_nexs_follower())

        # Watchdog: print heartbeat every 60s to detect event loop stalls
        async def _watchdog():
            import time
            prev_tasks = 0
            while not cell.isfini:
                await asyncio.sleep(60)
                ntasks = len(asyncio.all_tasks())
                nlinks = len(cell.dmon.links) if hasattr(cell.dmon, 'links') else -1
                print(f'[writer] heartbeat t={int(time.time())} tasks={ntasks} delta={ntasks - prev_tasks} links={nlinks}', flush=True)
                prev_tasks = ntasks

        asyncio.create_task(_watchdog())

        await cell.main()

    try:
        asyncio.run(_writer_serve())
    finally:
        arbiter.shutdown()


def may_fork(cls, argv):
    '''Quick check whether this cell class might use fork mode.'''
    if not hasattr(cls, 'prepareFork'):
        return False

    # Check environment variable first
    env_val = os.environ.get('SYN_CORTEX_MULTI_PROCESS_CORE_PCT', '')
    if env_val.isdigit() and int(env_val) > 0:
        return True

    import yaml
    dirn = None
    for arg in argv:
        if not arg.startswith('-'):
            dirn = arg
            break
    if dirn is None:
        return False

    cellpath = os.path.join(dirn, 'cell.yaml')
    try:
        with open(cellpath) as f:
            conf = yaml.safe_load(f) or {}
        return conf.get('multi:process:core_pct', 0) > 0
    except (OSError, yaml.YAMLError):
        return False


async def init_for_fork(cls, argv, outp=None):
    '''Initialize the cell and return it with fork info.'''
    if outp is None:
        outp = s_output.stdout

    cell = await cls.initFromArgv(argv, outp=outp)

    fork_info = None
    if hasattr(cell, 'prepareFork'):
        fork_info = await cell.prepareFork()

    if fork_info is not None:
        fork_info['listen_fd'] = os.dup(fork_info['listen_fd'])

    cell._syn_refs += 1

    # Detach the forkserver pool so asyncio.run() exit doesn't kill
    # SpawnProcess workers. We clear the pool reference and unregister
    # the atexit handler. The writer will re-create the pool after fork.
    import synapse.lib.processpool as s_processpool
    if s_processpool.forkpool is not None:
        # Prevent shutdown() from killing workers
        s_processpool.forkpool._processes = {}
        s_processpool.forkpool._broken = True
        # Unregister the atexit handler
        import atexit
        atexit.unregister(s_processpool.forkpool.shutdown)
        s_processpool.forkpool = None

    # Close HTTPS server sockets before fork so the writer can rebind after
    if hasattr(cell, 'httpds'):
        for httpd in cell.httpds:
            for sock in getattr(httpd, '_sockets', {}).values():
                sock.close()
            if hasattr(httpd, '_sockets'):
                httpd._sockets.clear()

    return {'cell': cell, 'fork_info': fork_info}


async def restore_dmon_listener(cell, listen_fd):
    '''Re-create the dmon TCP listener using an inherited socket fd.'''
    sock = socket.socket(fileno=listen_fd)
    sock.setblocking(False)

    turl = cell._getDmonListen()
    sslctx = None
    if turl is not None:
        info = s_telepath.chopurl(turl)
        if info.get('scheme') == 'ssl':
            caname = info.get('ca')
            hostname = info.get('hostname', info.get('host'))
            sslctx = cell.dmon.certdir.getServerSSLContext(hostname=hostname, caname=caname)

    is_tls = sslctx is not None

    async def onconn(reader, writer):
        info = {'tls': is_tls}
        link = await s_link.Link.anit(reader, writer, info=info)
        link.schedCoro(cell.dmon._onLinkInit(link))

    server = await asyncio.start_server(onconn, sock=sock, ssl=sslctx)
    cell.dmon.listenservers.append(server)


async def restore_after_init_loop(cell):
    '''Restore cell state after the init event loop has been closed.'''
    cell.dmon.listenservers.clear()

    sockpath = os.path.join(cell.dirn, 'sock')
    try:
        os.unlink(sockpath)
    except FileNotFoundError:
        pass

    try:
        await cell.dmon.listen(f'unix://{sockpath}')
    except OSError:
        logger.warning('Failed to re-create local unix socket at %s', sockpath)

    turl = cell._getDmonListen()
    if turl is not None:
        cell.sockaddr = await cell.dmon.listen(turl)

    # Re-create HTTPS listeners (Tornado servers die with the old event loop)
    if hasattr(cell, 'httpds') and hasattr(cell, 'https_listeners'):
        old_listeners = list(cell.https_listeners)
        cell.httpds.clear()
        cell.https_listeners.clear()
        for info in old_listeners:
            try:
                await cell.addHttpsPort(info['port'], host=info['host'])
            except Exception:
                logger.warning('Failed to restore HTTPS listener on %s:%s', info['host'], info['port'])

    cell._fireActiveCoros()


# --- Cortex fork-mode helpers extracted from synapse/cortex.py ---

import synapse.lib.coro as s_coro


def lmdb_reader_check(cortex):
    '''Clear stale LMDB reader slots from crashed reader processes.'''
    stale = 0
    stale += cortex.slab.lenv.reader_check()
    for layr in cortex.layers.values():
        stale += layr.layrslab.lenv.reader_check()
    if stale:
        logger.info('Cleared %d stale LMDB reader slot(s)', stale)
    return stale


async def lmdb_reader_check_loop(cortex):
    while not cortex.isfini:
        await cortex.waitfini(timeout=60)
        if not cortex.isfini:
            await s_coro.executor(lambda: lmdb_reader_check(cortex))


async def init_cortex_fork_mode(cortex, pct):
    '''Set up fork-mode config. Socket lookup and UDS listener happen later
    in prepareFork(), after initServiceNetwork has created the TCP listener.
    '''
    cores = os.cpu_count() or 1
    count = max(1, int(cores * pct / 100))

    cortex._forkinfo = {
        'count': count,
        'uds_path': os.path.join(cortex.dirn, 'worker.sock'),
        'datadir': cortex.dirn,
    }
    logger.info('Fork mode: configured for %d worker(s)', count)


async def prepare_cortex_fork(cortex):
    '''Finalize fork setup after all init phases complete.

    Starts the UDS listener and resolves the TCP listening socket fd.
    Returns the fork info dict, or None if fork mode is not active.
    '''
    if cortex._forkinfo is None:
        return None

    # Start UDS endpoint for workers to forward writes
    uds_path = cortex._forkinfo['uds_path']
    await cortex.dmon.listen(f'unix://{uds_path}')
    logger.info('Fork mode: UDS listener at %s', uds_path)

    # Find the main TCP/SSL listening socket from the dmon
    listen_sock = None
    for server in cortex.dmon.listenservers:
        for sock in server.sockets:
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                listen_sock = sock
                break
        if listen_sock is not None:
            break

    if listen_sock is None:
        logger.error('Fork mode: no TCP listening socket found, cannot fork')
        cortex._forkinfo = None
        return None

    cortex._forkinfo['listen_fd'] = listen_sock.fileno()
    cortex._forkinfo['listen_url'] = getattr(cortex, '_listen_url', None) or cortex._getDmonListen()
    return cortex._forkinfo
