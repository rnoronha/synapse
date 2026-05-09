'''
ReadOnlyWorker — forked read worker for the prefork Cortex architecture.

After the parent Cortex initializes fully, it forks N workers that inherit
the listening socket. Each worker re-opens LMDB in readonly mode, accepts
client connections with EPOLLEXCLUSIVE, serves reads locally, and forwards
writes to the writer process via msgpack RPC over a pre-connected socketpair.
'''
import os
import re
import time
import select
import socket
import asyncio
import logging

import lmdb
import msgpack

import synapse.exc as s_exc
import synapse.common as s_common
import synapse.daemon as s_daemon
import synapse.lib.link as s_link
import synapse.lib.scope as s_scope
import synapse.lib.lmdbslab as s_lmdbslab
import synapse.lib.msgpack as s_msgpack

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query classification
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
# Worker entry point (called after fork)
# ---------------------------------------------------------------------------

def worker_main(control_fd, uds_path, datadir, cell=None, write_fd=None, read_fd=None):
    '''
    Entry point for a forked read worker process.

    Args:
        control_fd: File descriptor of the UDS control channel from the router.
        uds_path: Path to the writer's UDS endpoint (legacy, unused with write_fd).
        datadir: Cortex data directory (for LMDB slab re-open).
        cell: The inherited Cortex cell object (shared via dmon for telepath).
        write_fd: File descriptor of the write channel socketpair to the writer.
        read_fd: File descriptor of the read channel socketpair from the writer.
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
    if cell is not None:
        import gc
        import synapse.lib.base as s_base
        for obj in gc.get_objects():
            if isinstance(obj, s_base.Base) and obj.anitted:
                obj.loop = loop
                obj.finievt = asyncio.Event()
        cell.loop = loop

    worker = ReadOnlyWorker(control_fd, uds_path, cell=cell, write_fd=write_fd, read_fd=read_fd)
    try:
        loop.run_until_complete(worker.serve())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def _reopen_lmdb_readonly(datadir):
    '''Re-open inherited LMDB slabs in readonly mode.'''
    import synapse.lib.forkmode as s_forkmode
    s_forkmode.is_readonly_worker = True

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
# WorkerDaemon — Daemon subclass for fork workers
# ---------------------------------------------------------------------------

class WorkerDaemon(s_daemon.Daemon):
    '''Daemon subclass that creates sessions on-the-fly for pool connections.

    Fork workers receive pooled connections that bypass the normal tele:syn
    handshake, so the session referenced in t2:init may not exist locally.
    This subclass overrides _onTaskV2Init to create a session when needed.
    '''

    async def _ensureLinkSess(self, link, name):
        '''Create or re-use a session for a pool connection.'''
        sess = link.get('sess')
        if sess is not None:
            item = sess.getSessItem(name)
            if item is None:
                raise s_exc.NoSuchObj(name=name)
            return (sess, item)

        item = await self._getSharedItem(name or '*')
        if item is None:
            raise s_exc.NoSuchObj(name=name)

        sess = await s_daemon.Sess.anit()

        async def sessfini():
            self.sessions.pop(sess.iden, None)

        sess.onfini(sessfini)
        link.onfini(sess.fini)
        self.sessions[sess.iden] = sess
        link.set('sess', sess)

        sess.setSessItem(name, item)
        return (sess, item)

    async def _onTaskV2Init(self, link: s_link.Link, mesg):

        name = mesg[1].get('name')
        sidn = mesg[1].get('sess')
        todo = mesg[1].get('todo')

        try:

            if sidn is None or todo is None:
                raise s_exc.NoSuchObj(name=name)

            sess = self.sessions.get(sidn)

            # If the session doesn't exist locally (e.g. pool connection
            # landed on a different fork worker), create one on-the-fly.
            if sess is None:
                sess, item = await self._ensureLinkSess(link, name)
            else:
                item = sess.getSessItem(name)
                if item is None:
                    raise s_exc.NoSuchObj(name=name)

            s_scope.set('sess', sess)
            s_scope.set('link', link)

            methname, args, kwargs = todo

            if methname[0] == '_':
                raise s_exc.NoSuchMeth.init(methname, item)

            meth = getattr(item, methname, None)
            if meth is None:
                raise s_exc.NoSuchMeth.init(methname, item)

            sessitem = await s_daemon.t2call(link, meth, args, kwargs)
            if sessitem is not None:
                sess.onfini(sessitem)

        except (asyncio.CancelledError, Exception) as e:
            logger.exception(f'Error on t2:init: {s_common.trimText(repr(mesg), n=80)} link={link.getAddrInfo()}')
            if not link.isfini:
                retn = s_common.retnexc(e)
                await link.tx(('t2:fini', {'retn': retn}))


# ---------------------------------------------------------------------------
# ReadOnlyWorker
# ---------------------------------------------------------------------------

_RECV_BUF = 65536
_REQ_COUNTER = 0


class ReadOnlyWorker:
    '''
    Forked read worker. Receives connection fds from the router via a UDS
    control channel, serves Storm reads locally, and forwards writes to
    the writer via msgpack RPC over a pre-connected socketpair.
    '''

    def __init__(self, control_fd, uds_path, cell=None, write_fd=None, read_fd=None):
        self._control_fd = control_fd
        self._uds_path = uds_path
        self._cell = cell
        self._write_fd = write_fd
        self._read_fd = read_fd
        self._write_alive = write_fd is not None
        self._dmon = None
        self._sslctx = None
        self._stopping = False
        self._pid = os.getpid()
        # Demux state: single reader task dispatches to per-request queues
        self._write_lock = None       # asyncio.Lock for serializing sends
        self._write_ready = None      # asyncio.Event set when writer is ready
        self._pending_reqs = {}       # {req_id: asyncio.Queue}
        self._reader_task = None

    async def serve(self):
        '''Main worker loop. Receives fds from router via recvmsg on control channel.'''
        loop = asyncio.get_running_loop()

        if self._cell is not None:
            self._cell.isactive = True
            # Workers don't need the drive subprocess (it's for writes/backups)
            self._cell.drive = None
            self._install_write_forwarding()

        if self._cell is not None and self._read_fd is not None:
            import synapse.lib.readchannel as s_readchannel
            self._read_listener = s_readchannel.ReadChannelListener(self._cell, self._read_fd)
            await self._read_listener.start()

        if self._cell is not None:
            self._dmon = await WorkerDaemon.anit()
            parent_dmon = getattr(self._cell, 'dmon', None)
            if parent_dmon is not None:
                for name, item in parent_dmon.shared.items():
                    self._dmon.share(name, self._cell)
            else:
                self._dmon.share('*', self._cell)

            # Resolve SSL context for incoming connections
            turl = (self._cell._forkinfo or {}).get('listen_url') \
                or getattr(self._cell, '_listen_url', None) \
                or self._cell._getDmonListen()
            logger.info('Worker %d: listen_url=%s', self._pid, turl)
            if turl is not None and 'ssl://' in turl:
                import synapse.telepath as s_telepath
                import synapse.lib.certdir as s_certdir
                info = s_telepath.chopurl(turl)
                hostname = info.get('hostname', info.get('host'))
                caname = info.get('ca')
                certpath = os.path.join(self._cell.dirn, 'certs')
                certdir = s_certdir.CertDir(path=(certpath,))
                logger.info('Worker %d: resolving SSL ctx hostname=%s ca=%s', self._pid, hostname, caname)
                self._sslctx = certdir.getServerSSLContext(hostname=hostname, caname=caname)
                # Allow clients without certs (CERT_OPTIONAL instead of CERT_REQUIRED)
                import ssl as _ssl
                self._sslctx.verify_mode = _ssl.CERT_OPTIONAL
                logger.info('Worker %d: SSL context resolved: %s', self._pid, self._sslctx)

        control_sock = socket.socket(fileno=self._control_fd)
        control_sock.setblocking(False)

        try:
            control_sock.send(b'\x52')
        except OSError:
            logger.error('Worker %d: failed to send READY', self._pid)

        logger.info('Worker %d: receiving connections via control channel', self._pid)

        try:
            while not self._stopping:
                readable = await loop.run_in_executor(None, self._poll_control, control_sock)
                if not readable:
                    continue

                fd = await loop.run_in_executor(None, self._recv_fd, control_sock)
                if fd is None:
                    if self._stopping:
                        break
                    logger.warning('Worker %d: control channel closed', self._pid)
                    break

                conn = socket.socket(fileno=fd)
                conn.setblocking(False)
                loop.create_task(self._handle_connection(conn, conn.getpeername()))
        finally:
            control_sock.detach()
            if self._dmon is not None:
                await self._dmon.fini()
            if self._write_fd is not None:
                try:
                    os.close(self._write_fd)
                except OSError:
                    pass
            if self._read_fd is not None:
                try:
                    os.close(self._read_fd)
                except OSError:
                    pass
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

        try:
            if self._sslctx is not None:
                # Server-side TLS: use start_tls on the raw connection
                reader, writer = await asyncio.open_connection(sock=conn)
                transport = writer.transport
                loop = asyncio.get_running_loop()
                new_transport = await loop.start_tls(transport, transport.get_protocol(), self._sslctx, server_side=True)
                # Rebind the reader/writer to the new TLS transport
                reader._transport = new_transport
                writer._transport = new_transport
            else:
                reader, writer = await asyncio.open_connection(sock=conn)
            link = await s_link.Link.anit(reader, writer)
            link.schedCoro(self._dmon._onLinkInit(link))
        except Exception:
            logger.exception('Worker %d: _handle_connection failed', self._pid)
            conn.close()

    # ------------------------------------------------------------------
    # Write forwarding via socketpair RPC
    # ------------------------------------------------------------------

    def _install_write_forwarding(self):
        '''Monkey-patch cell.storm() and cell.callStorm() to forward writes.

        Strategy: execute all queries with readonly=True. If IsReadOnly
        surfaces, forward to the writer via the write channel socketpair.
        Regex classify() provides a fast-path for obvious writes.
        '''
        cell = self._cell
        _orig_storm = cell.storm
        _orig_callStorm = cell.callStorm
        worker = self

        # Start the demux reader and init the send lock
        self._write_lock = asyncio.Lock()
        self._write_ready = asyncio.Event()
        if self._write_fd is not None:
            self._reader_task = asyncio.get_running_loop().create_task(self._demux_reader())

        def _refresh_all_ro_slabs():
            for slab in s_lmdbslab.Slab.allslabs.values():
                if slab.readonly:
                    slab._refresh_ro_xact()

        async def _patched_storm(text, opts=None):
            _refresh_all_ro_slabs()
            if classify(text) == 'write':
                async for mesg in worker._forward_write_stream(text, opts):
                    yield mesg
                return

            ropts = dict(opts) if opts else {}
            ropts['readonly'] = True

            async for mesg in _orig_storm(text, opts=ropts):
                if mesg[0] == 'err' and mesg[1][0] == 'IsReadOnly':
                    async for fmesg in worker._forward_write_stream(text, opts):
                        yield fmesg
                    return
                yield mesg

        async def _patched_callStorm(text, opts=None):
            _refresh_all_ro_slabs()
            if classify(text) == 'write':
                return await worker._forward_callStorm(text, opts)

            ropts = dict(opts) if opts else {}
            ropts['readonly'] = True

            try:
                return await _orig_callStorm(text, opts=ropts)
            except s_exc.IsReadOnly:
                return await worker._forward_callStorm(text, opts)

        cell.storm = _patched_storm
        cell.callStorm = _patched_callStorm

    async def _demux_reader(self):
        '''Background task: read from write_fd and dispatch to per-request queues.'''
        loop = asyncio.get_running_loop()

        # Wait for the writer's ready byte before processing responses.
        # The WriteChannelListener sends 0x01 on each writer-end fd once
        # its epoll loop is registered and ready to receive requests.
        try:
            ready_byte = await asyncio.wait_for(
                loop.run_in_executor(None, os.read, self._write_fd, 1),
                timeout=60.0)
            if not ready_byte:
                logger.error('Worker %d: write channel closed before ready', self._pid)
                self._write_alive = False
                return
            logger.info('Worker %d: write channel ready', self._pid)
        except (OSError, asyncio.TimeoutError) as e:
            logger.error('Worker %d: write channel ready wait failed: %s', self._pid, e)
            self._write_alive = False
            return

        # Signal that writes can now be forwarded
        self._write_ready.set()

        unpacker = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)
        while self._write_alive:
            try:
                data = await loop.run_in_executor(None, os.read, self._write_fd, _RECV_BUF)
            except OSError:
                break
            if not data:
                break
            unpacker.feed(data)
            for resp in unpacker:
                req_id = resp[1]
                q = self._pending_reqs.get(req_id)
                if q is not None:
                    q.put_nowait(resp)
        # Channel dead — signal all waiters
        self._write_alive = False
        for q in self._pending_reqs.values():
            q.put_nowait(None)

    async def _send_write_rpc(self, req):
        '''Send a write RPC request, serialized via lock.'''
        # Wait for the writer to signal readiness (ready byte received)
        if self._write_ready is not None and not self._write_ready.is_set():
            try:
                await asyncio.wait_for(self._write_ready.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                raise s_exc.SynErr(mesg='Writer unavailable (write channel not ready)')

        if not self._write_alive:
            raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')

        data = s_msgpack.en(req)
        loop = asyncio.get_running_loop()
        async with self._write_lock:
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._sendall, data),
                    timeout=30.0)
            except (OSError, BrokenPipeError, asyncio.TimeoutError):
                self._write_alive = False
                raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')

    def _sendall(self, data):
        '''Write all bytes to the write channel fd, handling partial writes.'''
        mv = memoryview(data)
        while mv:
            sent = os.write(self._write_fd, mv)
            mv = mv[sent:]

    async def _forward_write_stream(self, text, opts):
        '''Forward a storm query to the writer via socketpair, yielding messages.'''
        global _REQ_COUNTER
        _REQ_COUNTER += 1
        req_id = _REQ_COUNTER

        q = asyncio.Queue()
        self._pending_reqs[req_id] = q
        try:
            await self._send_write_rpc(('storm', req_id, text, opts))
            while True:
                resp = await asyncio.wait_for(q.get(), timeout=30.0)
                if resp is None:
                    raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')
                if resp[0] == 'msg':
                    yield resp[2]
                elif resp[0] == 'done':
                    return
                elif resp[0] == 'err':
                    raise s_exc.SynErr(mesg=resp[2].get('mesg', 'Write forwarding error'))
        except asyncio.TimeoutError:
            raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')
        finally:
            self._pending_reqs.pop(req_id, None)

    async def _forward_callStorm(self, text, opts):
        '''Forward a callStorm to the writer, returning the result value.'''
        global _REQ_COUNTER
        _REQ_COUNTER += 1
        req_id = _REQ_COUNTER

        q = asyncio.Queue()
        self._pending_reqs[req_id] = q
        try:
            await self._send_write_rpc(('callStorm', req_id, text, opts))
            while True:
                resp = await asyncio.wait_for(q.get(), timeout=30.0)
                if resp is None:
                    raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')
                if resp[0] == 'result':
                    return resp[2]
                elif resp[0] == 'err':
                    raise s_exc.SynErr(mesg=resp[2].get('mesg', 'Write forwarding error'))
        except asyncio.TimeoutError:
            raise s_exc.SynErr(mesg='Writer unavailable (write channel dead)')
        finally:
            self._pending_reqs.pop(req_id, None)

    def shutdown(self):
        '''Signal the worker to stop accepting new connections.'''
        self._stopping = True
        logger.info('Worker %d: shutdown requested', self._pid)
