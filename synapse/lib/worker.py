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
import synapse.telepath as s_telepath
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

def worker_main(listening_sock_fd, uds_path, datadir):
    '''
    Entry point for a forked read worker process.

    Args:
        listening_sock_fd: File descriptor of the inherited listening socket.
        uds_path: Path to the writer's UDS endpoint for write forwarding.
        datadir: Cortex data directory (for LMDB slab re-open).
    '''
    _reopen_lmdb_readonly(datadir)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    worker = ReadOnlyWorker(listening_sock_fd, uds_path)
    try:
        loop.run_until_complete(worker.serve())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def _reopen_lmdb_readonly(datadir):
    '''Close inherited LMDB envs and re-open them in readonly mode.'''
    slabs = list(s_lmdbslab.Slab.allslabs.values())
    for slab in slabs:
        path = slab.path
        slab.lenv.close()
        s_lmdbslab.Slab.allslabs.pop(path, None)

        # Re-open fresh in readonly mode against the same files
        mapsize = os.path.getsize(os.path.join(path, 'data.mdb'))
        env = lmdb.open(
            str(path),
            map_size=mapsize,
            max_dbs=128,
            max_readers=256,
            readonly=True,
            create=False,
            readahead=True,
        )
        # Stash the env so the worker can use it; keyed by path for lookup
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

    def __init__(self, listen_fd, uds_path):
        self._listen_fd = listen_fd
        self._uds_path = uds_path
        self._writer_proxy = None
        self._circuit = CircuitBreaker(threshold=3, recovery_timeout=1.0)
        self._write_sem = None  # created in serve() inside the event loop
        self._stopping = False
        self._pid = os.getpid()

    async def serve(self):
        '''Main worker loop. Registers listening socket with EPOLLEXCLUSIVE.'''
        self._write_sem = asyncio.Semaphore(5)
        loop = asyncio.get_running_loop()

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
        '''Handle a single client connection: classify and route queries.'''
        loop = asyncio.get_running_loop()
        reader, writer = await asyncio.open_connection(sock=conn)
        try:
            # The connection is a Telepath link. We read Storm requests
            # from the Telepath protocol and route them.
            # For Phase 1b, we use a simplified protocol: the worker acts
            # as a Telepath-aware proxy. Full dmon integration comes in Phase 2.
            #
            # For now, each accepted connection is handed to the Daemon
            # infrastructure that the Cortex already uses. The worker's
            # storm() method overrides the Cortex's to classify and route.
            pass
        except Exception:
            logger.exception('Worker %d: connection handler error', self._pid)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

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
