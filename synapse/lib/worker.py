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

def worker_main(listening_sock_fd, uds_path, datadir, cell=None):
    '''
    Entry point for a forked read worker process.

    Args:
        listening_sock_fd: File descriptor of the inherited listening socket.
        uds_path: Path to the writer's UDS endpoint for write forwarding.
        datadir: Cortex data directory (for LMDB slab re-open).
        cell: The inherited Cortex cell object (shared via dmon for telepath).
    '''
    # Neutralize the inherited forkpool (stale threads/pipes after fork)
    import synapse.lib.processpool as s_processpool
    if getattr(s_processpool, 'forkpool', None) is not None:
        s_processpool.forkpool.shutdown(wait=False)
        s_processpool.forkpool = None

    _reopen_lmdb_readonly(datadir)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    worker = ReadOnlyWorker(listening_sock_fd, uds_path, cell=cell)
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
    must NOT close them again (double-close).  We just collect the paths
    from the inherited allslabs metadata, clear the stale entries, and
    re-open fresh readonly environments.
    '''
    paths = [slab.path for slab in s_lmdbslab.Slab.allslabs.values()]
    s_lmdbslab.Slab.allslabs.clear()

    for path in paths:
        env = lmdb.open(
            str(path),
            map_size=0,  # use current file size; avoids MDB_MAP_RESIZED
            max_dbs=128,
            max_readers=256,
            readonly=True,
            create=False,
            readahead=False,
        )
        _readonly_envs[path] = env


# Module-level registry of re-opened readonly LMDB environments
_readonly_envs: dict[str, lmdb.Environment] = {}


# ---------------------------------------------------------------------------
# ReadOnlyWorker
# ---------------------------------------------------------------------------

class ReadOnlyWorker:
    '''
    Forked read worker. Accepts connections on the inherited listening socket
    with EPOLLEXCLUSIVE, serves Storm reads locally, and forwards writes to
    the writer via Telepath-over-UDS.
    '''

    def __init__(self, listen_fd, uds_path, cell=None):
        self._listen_fd = listen_fd
        self._uds_path = uds_path
        self._cell = cell
        self._dmon = None
        self._writer_proxy = None
        self._circuit = CircuitBreaker(threshold=3, recovery_timeout=1.0)
        self._write_sem = None  # created in serve() inside the event loop
        self._stopping = False
        self._pid = os.getpid()

    async def serve(self):
        '''Main worker loop. Registers listening socket with EPOLLEXCLUSIVE.'''
        self._write_sem = asyncio.Semaphore(5)
        loop = asyncio.get_running_loop()

        # Create a Dmon to serve the telepath protocol on accepted connections
        if self._cell is not None:
            self._dmon = await s_daemon.Daemon.anit()
            self._dmon.share('*', self._cell)

        listen_sock = socket.socket(fileno=self._listen_fd)
        listen_sock.setblocking(False)

        # Register with EPOLLEXCLUSIVE to avoid thundering herd
        epoll = select.epoll()
        epoll.register(self._listen_fd, select.EPOLLIN | select.EPOLLEXCLUSIVE)

        logger.info('Worker %d: accepting connections', self._pid)

        try:
            while not self._stopping:
                if self._circuit.is_open:
                    # Back off when writer is unreachable
                    await asyncio.sleep(self._circuit._recovery_timeout)
                    continue

                try:
                    events = await loop.run_in_executor(None, epoll.poll, 1.0)
                except OSError:
                    break

                for fd, event in events:
                    if fd == self._listen_fd and (event & select.EPOLLIN):
                        await self._do_accept(listen_sock, loop)
        finally:
            epoll.unregister(self._listen_fd)
            epoll.close()
            # Don't close the socket — fd is shared with parent
            listen_sock.detach()
            if self._dmon is not None:
                await self._dmon.fini()
            await self._close_writer_proxy()
            logger.info('Worker %d: stopped', self._pid)

    async def _do_accept(self, listen_sock, loop):
        '''Accept one connection and spawn a handler task.'''
        try:
            conn, addr = listen_sock.accept()
            conn.setblocking(False)
        except BlockingIOError:
            return
        except OSError:
            return

        loop.create_task(self._handle_connection(conn, addr))

    async def _handle_connection(self, conn, addr):
        '''Handle a single client connection via the telepath dmon protocol.'''
        if self._dmon is None:
            conn.close()
            return

        reader, writer = await asyncio.open_connection(sock=conn)
        link = await s_link.Link.anit(reader, writer)
        link.schedCoro(self._dmon._onLinkInit(link))

    async def storm(self, text, opts=None):
        '''
        Classify and execute a Storm query.

        Reads execute locally against readonly LMDB.
        Writes forward to the writer via UDS.
        '''
        if classify(text) == 'write':
            async for mesg in self._forward_write(text, opts):
                yield mesg
            return

        try:
            async for mesg in self._execute_read(text, opts):
                yield mesg
        except s_exc.IsReadOnly:
            # classify() false negative — transparently retry via writer
            async for mesg in self._forward_write(text, opts):
                yield mesg

    async def _execute_read(self, text, opts):
        '''Execute a read query locally against readonly LMDB snapshots.'''
        # This will be wired to the Cortex's view.storm() in Phase 2
        # when the worker inherits the full Cortex object graph.
        # For Phase 1b, this is the integration point.
        raise NotImplementedError('_execute_read requires Cortex view wiring (Phase 2)')

    async def _forward_write(self, text, opts):
        '''Forward a write query to the writer process via UDS.'''
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
