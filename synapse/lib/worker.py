'''
ReadOnlyWorker — forked read worker for the prefork Cortex architecture.

After the parent Cortex initializes fully, it forks N workers that inherit
the listening socket. Each worker re-opens LMDB in readonly mode, accepts
client connections with EPOLLEXCLUSIVE, serves reads locally, and forwards
writes to the writer process via a Telepath UDS connection.
'''
import os
import re
import time
import select
import socket
import asyncio
import logging

import lmdb

import synapse.exc as s_exc
import synapse.daemon as s_daemon
import synapse.telepath as s_telepath
import synapse.lib.link as s_link
import synapse.lib.lmdbslab as s_lmdbslab

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query classification (copied from queryrouter.py — will be sole copy after
# Phase 2 removes queryrouter.py)
# ---------------------------------------------------------------------------

_write_commands = frozenset((
    'auth.user.add', 'auth.user.del', 'auth.role.add', 'auth.role.del',
    'auth.user.grant', 'auth.user.revoke', 'auth.user.addrule', 'auth.user.delrule',
    'auth.role.addrule', 'auth.role.delrule',
    'cron.add', 'cron.del', 'cron.mod', 'cron.move', 'cron.enable', 'cron.disable',
    'cron.cleanup',
    'delnode',
    'dmon.add', 'dmon.del',
    'feed.ingest',
    'graph.add', 'graph.del',
    'layer.add', 'layer.del', 'layer.set', 'layer.pull.add', 'layer.push.add',
    'macro.set', 'macro.del',
    'merge',
    'model.edge.set', 'model.edge.del',
    'model.depr.lock', 'model.depr.unlock',
    'movetag',
    'pkg.load', 'pkg.del',
    'queue.add', 'queue.del',
    'service.add', 'service.del',
    'trigger.add', 'trigger.del', 'trigger.mod', 'trigger.enable', 'trigger.disable',
    'view.add', 'view.del', 'view.set', 'view.merge',
))

_re_edit_bracket = re.compile(r'(?<![a-zA-Z0-9_\)\]])\[')
_re_write_cmd = re.compile(
    r'\|\s*(' + '|'.join(re.escape(c) for c in sorted(_write_commands, key=len, reverse=True)) + r')\b'
)
_re_write_patterns = re.compile(
    r'\$node\.data\.set\b'
    r'|\$node\.data\.pop\b'
    r'|\$lib\.queue\.\w+\.put\b'
    r'|->>\s*\w+'
)

_re_comment = re.compile(r'//[^\n]*')


def classify(text):
    '''Classify a Storm query as 'read' or 'write'.'''
    stripped = _re_comment.sub('', text)
    if _re_edit_bracket.search(stripped):
        return 'write'
    if _re_write_cmd.search(stripped):
        return 'write'
    if _re_write_patterns.search(stripped):
        return 'write'
    return 'read'


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    '''Three-state circuit breaker: closed → open → half-open → closed.'''

    def __init__(self, threshold=3, recovery_timeout=1.0):
        self._failures = 0
        self._threshold = threshold
        self._recovery_timeout = recovery_timeout
        self._last_failure = 0.0
        self._state = 'closed'

    @property
    def is_open(self):
        if self._state == 'open':
            if time.monotonic() - self._last_failure > self._recovery_timeout:
                self._state = 'half-open'
                return False
            return True
        return False

    def record_failure(self):
        self._failures += 1
        self._last_failure = time.monotonic()
        if self._failures >= self._threshold:
            self._state = 'open'
            logger.error('Circuit breaker OPEN after %d failures', self._failures)

    def record_success(self):
        if self._state != 'closed':
            logger.info('Circuit breaker CLOSED')
        self._failures = 0
        self._state = 'closed'


# ---------------------------------------------------------------------------
# Worker entry point (called after fork)
# ---------------------------------------------------------------------------

def worker_main(control_fd, uds_path, datadir, cell=None, write_fd=None):
    '''
    Entry point for a forked read worker process.

    Args:
        control_fd: File descriptor of the UDS control channel from the router.
        uds_path: Path to the writer's UDS endpoint for write forwarding.
        datadir: Cortex data directory (for LMDB slab re-open).
        cell: The inherited Cortex cell object (shared via dmon for telepath).
        write_fd: File descriptor of the write channel socketpair to the writer.
    '''
    # Neutralize the inherited forkpool (stale threads/pipes after fork)
    import synapse.lib.processpool as s_processpool
    if getattr(s_processpool, 'forkpool', None) is not None:
        s_processpool.forkpool.shutdown(wait=False)
        s_processpool.forkpool = None

    _reopen_lmdb_readonly(datadir)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Rebind all inherited Base objects to the worker's new event loop.
    # Without this, the Cell and its children reference the dead init loop.
    if cell is not None:
        import gc
        import synapse.lib.base as s_base
        for obj in gc.get_objects():
            if isinstance(obj, s_base.Base) and obj.anitted:
                obj.loop = loop
                obj.finievt = asyncio.Event()
        cell.loop = loop

    worker = ReadOnlyWorker(control_fd, uds_path, cell=cell, write_fd=write_fd)
    try:
        loop.run_until_complete(worker.serve())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def _reopen_lmdb_readonly(datadir):
    '''Re-open inherited LMDB slabs in readonly mode.

    The parent arbiter already closed the lenv handles before fork, so we
    must NOT close them again (double-close).  We reconnect each Slab
    object to a fresh readonly LMDB environment so the inherited Cell
    can serve reads through its normal data access layer.
    '''
    for slab in list(s_lmdbslab.Slab.allslabs.values()):
        path = slab.path
        try:
            slab.lenv = lmdb.open(
                str(path),
                map_size=0,
                max_dbs=128,
                max_readers=256,
                readonly=True,
                create=False,
                readahead=False,
            )
            slab.isfini = False
            slab.readonly = True
            slab.xact = None
            slab.txnrefcount = 0

            # Re-open all cached database handles against the new env
            old_dbnames = dict(slab.dbnames)
            slab.dbnames = {None: (None, False)}
            for name, (db, dupsort) in old_dbnames.items():
                if name is None:
                    continue
                try:
                    newdb = slab.lenv.open_db(name.encode('utf8'), create=False, dupsort=dupsort)
                    slab.dbnames[name] = (newdb, dupsort)
                except Exception:
                    logger.warning('Worker: failed to re-open db %s in slab %s', name, path)

            logger.debug('Worker: re-opened slab %s readonly (%d dbs)', path, len(slab.dbnames) - 1)
        except Exception:
            logger.exception('Worker: failed to re-open slab %s', path)


# ---------------------------------------------------------------------------
# ReadOnlyWorker
# ---------------------------------------------------------------------------

class ReadOnlyWorker:
    '''
    Forked read worker. Receives connection fds from the router via a UDS
    control channel, serves Storm reads locally, and forwards writes to
    the writer via Telepath-over-UDS.
    '''

    def __init__(self, control_fd, uds_path, cell=None, write_fd=None):
        self._control_fd = control_fd
        self._uds_path = uds_path
        self._cell = cell
        self._write_fd = write_fd
        self._dmon = None
        self._writer_proxy = None
        self._circuit = CircuitBreaker(threshold=3, recovery_timeout=1.0)
        self._write_sem = None  # created in serve() inside the event loop
        self._stopping = False
        self._pid = os.getpid()

    async def serve(self):
        '''Main worker loop. Receives fds from router via recvmsg on control channel.'''
        self._write_sem = asyncio.Semaphore(5)
        loop = asyncio.get_running_loop()

        if self._cell is not None:
            self._install_write_forwarding()

        if self._cell is not None:
            self._dmon = await s_daemon.Daemon.anit()
            parent_dmon = getattr(self._cell, 'dmon', None)
            if parent_dmon is not None:
                for name, item in parent_dmon.shared.items():
                    self._dmon.share(name, self._cell)
            else:
                self._dmon.share('*', self._cell)

        control_sock = socket.socket(fileno=self._control_fd)
        control_sock.setblocking(False)

        # Send READY to the router
        try:
            control_sock.send(b'\x52')
        except OSError:
            logger.error('Worker %d: failed to send READY', self._pid)

        logger.info('Worker %d: receiving connections via control channel', self._pid)

        try:
            while not self._stopping:
                if self._circuit.is_open:
                    await asyncio.sleep(self._circuit._recovery_timeout)
                    continue

                # Wait for the control socket to be readable
                readable = await loop.run_in_executor(None, self._poll_control, control_sock)
                if not readable:
                    continue

                fd = await loop.run_in_executor(None, self._recv_fd, control_sock)
                if fd is None:
                    if self._stopping:
                        break
                    # Control channel closed — router died
                    logger.warning('Worker %d: control channel closed', self._pid)
                    break

                conn = socket.socket(fileno=fd)
                conn.setblocking(False)
                loop.create_task(self._handle_connection(conn, conn.getpeername()))
        finally:
            control_sock.detach()
            if self._dmon is not None:
                await self._dmon.fini()
            await self._close_writer_proxy()
            logger.info('Worker %d: stopped', self._pid)

    def _poll_control(self, control_sock):
        '''Poll the control socket for readability (blocking, run in executor).'''
        try:
            r, _, _ = select.select([control_sock], [], [], 1.0)
            return bool(r)
        except (OSError, ValueError):
            return False

    def _recv_fd(self, control_sock):
        '''Receive a file descriptor from the control channel.'''
        from synapse.lib.router import recv_fd
        return recv_fd(control_sock)

    async def _handle_connection(self, conn, addr):
        '''Handle a single client connection via the telepath dmon protocol.'''
        if self._dmon is None:
            conn.close()
            return

        reader, writer = await asyncio.open_connection(sock=conn)
        link = await s_link.Link.anit(reader, writer)
        link.schedCoro(self._dmon._onLinkInit(link))

    def _install_write_forwarding(self):
        '''Monkey-patch cell.storm() and cell.callStorm() to forward writes.

        CoreApi.storm() calls self.cell.storm(), so patching the cell methods
        is the minimal interception point that works with the existing dmon
        pipeline.

        Strategy: execute all queries with readonly=True in Storm opts.
        Synapse's own runtime enforces readonly — if a write is attempted,
        IsReadOnly surfaces as an ('err', ('IsReadOnly', ...)) message in
        the storm stream (view.storm catches exceptions and converts them)
        or as a Python exception in callStorm.  On detection, we forward
        the query to the writer process via UDS.

        The regex classify() is retained as an optional fast-path: queries
        that are obviously writes skip the local readonly attempt entirely.
        '''
        cell = self._cell
        _orig_storm = cell.storm
        _orig_callStorm = cell.callStorm
        worker = self

        async def _patched_storm(text, opts=None):
            # Fast-path: skip readonly attempt for obvious writes
            if classify(text) == 'write':
                async for mesg in worker._forward_storm(text, opts):
                    yield mesg
                return

            # Try locally with readonly enforcement
            ropts = dict(opts) if opts else {}
            ropts['readonly'] = True

            async for mesg in _orig_storm(text, opts=ropts):
                if mesg[0] == 'err' and mesg[1][0] == 'IsReadOnly':
                    # Write detected by Synapse runtime — forward to writer
                    async for fmesg in worker._forward_storm(text, opts):
                        yield fmesg
                    return
                yield mesg

        async def _patched_callStorm(text, opts=None):
            # Fast-path: skip readonly attempt for obvious writes
            if classify(text) == 'write':
                return await worker._forward_callStorm(text, opts)

            # Try locally with readonly enforcement
            ropts = dict(opts) if opts else {}
            ropts['readonly'] = True

            try:
                return await _orig_callStorm(text, opts=ropts)
            except s_exc.IsReadOnly:
                return await worker._forward_callStorm(text, opts)

        cell.storm = _patched_storm
        cell.callStorm = _patched_callStorm

    async def _forward_storm(self, text, opts):
        '''Forward a storm query to the writer, yielding messages.'''
        if self._circuit.is_open:
            raise s_exc.SynErr(mesg='Writer unavailable (circuit breaker open)')
        try:
            await asyncio.wait_for(self._write_sem.acquire(), timeout=30.0)
        except asyncio.TimeoutError:
            raise s_exc.SynErr(mesg='Write forwarding backlogged')
        try:
            proxy = await self._get_writer_proxy()
            async for mesg in proxy.storm(text, opts=opts):
                yield mesg
            self._circuit.record_success()
        except (OSError, ConnectionError, asyncio.TimeoutError) as e:
            self._circuit.record_failure()
            raise s_exc.SynErr(mesg=f'Write forwarding failed: {e}')
        finally:
            self._write_sem.release()

    async def _forward_callStorm(self, text, opts):
        '''Forward a callStorm query to the writer, returning the result.'''
        if self._circuit.is_open:
            raise s_exc.SynErr(mesg='Writer unavailable (circuit breaker open)')
        try:
            await asyncio.wait_for(self._write_sem.acquire(), timeout=30.0)
        except asyncio.TimeoutError:
            raise s_exc.SynErr(mesg='Write forwarding backlogged')
        try:
            proxy = await self._get_writer_proxy()
            result = await proxy.callStorm(text, opts=opts)
            self._circuit.record_success()
            return result
        except (OSError, ConnectionError, asyncio.TimeoutError) as e:
            self._circuit.record_failure()
            raise s_exc.SynErr(mesg=f'Write forwarding failed: {e}')
        finally:
            self._write_sem.release()

    async def _get_writer_proxy(self):
        '''Lazy-reconnecting Telepath proxy to the writer's UDS endpoint.'''
        if self._writer_proxy is not None and not self._writer_proxy.isfini:
            return self._writer_proxy
        self._writer_proxy = await s_telepath.openurl(f'unix://{self._uds_path}')
        return self._writer_proxy

    async def _close_writer_proxy(self):
        if self._writer_proxy is not None:
            await self._writer_proxy.fini()
            self._writer_proxy = None

    def shutdown(self):
        '''Signal the worker to stop accepting new connections.'''
        self._stopping = True
        logger.info('Worker %d: shutdown requested', self._pid)
