'''
Read channel: forward HTTP API read queries from writer to workers.

Symmetric with writechannel.py — the writer monkey-patches cell.storm()
and cell.callStorm() to forward read-classified queries to workers via
per-worker socketpairs.  Workers execute reads locally (readonly LMDB)
and stream results back.

Protocol (msgpack-encoded, same framing as write channel):

  Request:  ('storm', req_id, text, opts)
            ('callStorm', req_id, text, opts)
  Response: ('msg', req_id, mesg)
            ('done', req_id)
            ('result', req_id, val)
            ('err', req_id, excinfo)
'''
import os
import select
import asyncio
import logging

import msgpack

import synapse.lib.msgpack as s_msgpack

logger = logging.getLogger(__name__)

_RECV_BUF = 65536


class ReadChannelListener:
    '''Runs on each WORKER: listens for read requests from the writer,
    executes them locally, and streams results back.'''

    def __init__(self, cell, read_fd):
        self._cell = cell
        self._fd = read_fd
        self._running = False
        self._task = None
        self._send_lock = asyncio.Lock()

    async def start(self):
        self._running = True
        # Send ready byte immediately (don't wait for task to be scheduled)
        try:
            os.write(self._fd, b'\x01')
        except OSError as e:
            logger.error('Read channel: failed to send ready byte: %s', e)
            return
        self._task = asyncio.get_running_loop().create_task(self._listen())
        logger.info('Read channel listener started on fd %d (pid %d)', self._fd, os.getpid())

    async def stop(self):
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        try:
            os.close(self._fd)
        except OSError:
            pass

    async def _listen(self):
        loop = asyncio.get_running_loop()
        unpacker = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)

        try:
            while self._running:
                try:
                    data = await loop.run_in_executor(None, self._blocking_read)
                except OSError:
                    break
                if data is None:
                    continue  # select timeout, keep waiting
                if not data:
                    break  # EOF — peer closed
                unpacker.feed(data)
                for msg in unpacker:
                    loop.create_task(self._handle(msg))
        except asyncio.CancelledError:
            pass

    def _blocking_read(self):
        r, _, _ = select.select([self._fd], [], [], 1.0)
        if r:
            return os.read(self._fd, _RECV_BUF)
        return None  # timeout, not EOF

    async def _handle(self, msg):
        try:
            kind = msg[0]
            req_id = msg[1]
            text = msg[2]
            opts = msg[3] if len(msg) > 3 else None
        except (IndexError, TypeError):
            logger.error('Malformed read request: %r', msg)
            return

        if kind == 'storm':
            try:
                async for mesg in self._cell.storm(text, opts=opts):
                    await self._send(('msg', req_id, mesg))
                await self._send(('done', req_id))
            except Exception as e:
                await self._send(('err', req_id, {'mesg': str(e), 'err': e.__class__.__name__}))
        elif kind == 'callStorm':
            try:
                result = await self._cell.callStorm(text, opts=opts)
                await self._send(('result', req_id, result))
            except Exception as e:
                await self._send(('err', req_id, {'mesg': str(e), 'err': e.__class__.__name__}))
        else:
            await self._send(('err', req_id, {'mesg': f'Unknown kind: {kind}'}))

    async def _send(self, msg):
        data = s_msgpack.en(msg)
        loop = asyncio.get_running_loop()
        async with self._send_lock:
            await loop.run_in_executor(None, self._sendall, data)

    def _sendall(self, data):
        mv = memoryview(data)
        while mv:
            try:
                sent = os.write(self._fd, mv)
            except OSError:
                return
            mv = mv[sent:]


class ReadForwarder:
    '''Runs on the WRITER: monkey-patches cell.storm()/callStorm() to
    forward read-classified queries to workers via round-robin.'''

    def __init__(self, cell, worker_fds):
        '''
        Args:
            cell: The Cortex cell.
            worker_fds: List of writer-end fds for read channel socketpairs.
        '''
        self._cell = cell
        self._fds = list(worker_fds)
        self._alive = [True] * len(worker_fds)
        self._rr_index = 0
        self._req_counter = 0
        self._pending = {}  # {req_id: asyncio.Queue}
        self._send_locks = {}
        self._reader_tasks = []
        self._ready_events = [asyncio.Event() for _ in worker_fds]

    async def start(self):
        loop = asyncio.get_running_loop()
        for i, fd in enumerate(self._fds):
            task = loop.create_task(self._demux_reader(i, fd))
            self._reader_tasks.append(task)

        # Give workers a moment to send ready bytes, but don't block
        print('[writer] ReadForwarder: sleeping 2s...', flush=True)
        await asyncio.sleep(2)
        print('[writer] ReadForwarder: sleep done', flush=True)
        ready_count = sum(1 for evt in self._ready_events if evt.is_set())
        if ready_count == 0:
            # No workers ready — mark all alive and hope they respond
            # (the 30s timeout in _forward_* will catch dead workers)
            for i in range(len(self._fds)):
                self._alive[i] = True

        self._install_patch()
        print(f'[writer] ReadForwarder: patch installed, {ready_count} ready', flush=True)
        logger.info('Read forwarder started with %d/%d workers ready', ready_count, len(self._fds))

    async def stop(self):
        for task in self._reader_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        for fd in self._fds:
            try:
                os.close(fd)
            except OSError:
                pass

    async def _demux_reader(self, idx, fd):
        '''Read responses from one worker and dispatch to pending queues.'''
        loop = asyncio.get_running_loop()
        unpacker = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)

        # Wait for ready byte
        try:
            ready = await asyncio.wait_for(
                loop.run_in_executor(None, os.read, fd, 1), timeout=60.0)
            if not ready:
                self._alive[idx] = False
                return
        except (OSError, asyncio.TimeoutError):
            self._alive[idx] = False
            return

        self._ready_events[idx].set()

        try:
            while True:
                try:
                    data = await loop.run_in_executor(None, os.read, fd, _RECV_BUF)
                except OSError:
                    break
                if not data:
                    break
                unpacker.feed(data)
                for resp in unpacker:
                    req_id = resp[1]
                    q = self._pending.get(req_id)
                    if q is not None:
                        q.put_nowait(resp)
        except asyncio.CancelledError:
            pass
        finally:
            self._alive[idx] = False
            for q in self._pending.values():
                q.put_nowait(None)

    def _pick_worker(self):
        '''Round-robin pick a live worker fd. Returns (fd, idx) or None.'''
        n = len(self._fds)
        for _ in range(n):
            idx = self._rr_index % n
            self._rr_index += 1
            if self._alive[idx]:
                return self._fds[idx], idx
        return None

    def _install_patch(self):
        from synapse.lib.worker import classify

        cell = self._cell
        _orig_storm = cell.storm
        _orig_callStorm = cell.callStorm
        forwarder = self

        async def _patched_storm(text, opts=None):
            if classify(text) == 'read':
                target = forwarder._pick_worker()
                if target is not None:
                    async for mesg in forwarder._forward_storm(target[0], text, opts):
                        yield mesg
                    return
            async for mesg in _orig_storm(text, opts=opts):
                yield mesg

        async def _patched_callStorm(text, opts=None):
            if classify(text) == 'read':
                target = forwarder._pick_worker()
                if target is not None:
                    return await forwarder._forward_callStorm(target[0], text, opts)
            return await _orig_callStorm(text, opts=opts)

        cell.storm = _patched_storm
        cell.callStorm = _patched_callStorm
        # Store originals for fallback
        self._orig_storm = _orig_storm
        self._orig_callStorm = _orig_callStorm

    async def _forward_storm(self, fd, text, opts):
        self._req_counter += 1
        req_id = self._req_counter

        q = asyncio.Queue()
        self._pending[req_id] = q
        try:
            await self._send(fd, ('storm', req_id, text, opts))
            deadline = asyncio.get_event_loop().time() + 300  # 5min max per query
            while True:
                try:
                    resp = await asyncio.wait_for(q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    # Check if we've exceeded the overall deadline
                    if asyncio.get_event_loop().time() > deadline:
                        break
                    continue
                if resp is None:
                    async for mesg in self._orig_storm(text, opts=opts):
                        yield mesg
                    return
                if resp[0] == 'msg':
                    yield resp[2]
                elif resp[0] == 'done':
                    return
                elif resp[0] == 'err':
                    async for mesg in self._orig_storm(text, opts=opts):
                        yield mesg
                    return
        except asyncio.CancelledError:
            pass
        finally:
            self._pending.pop(req_id, None)

    async def _forward_callStorm(self, fd, text, opts):
        self._req_counter += 1
        req_id = self._req_counter

        q = asyncio.Queue()
        self._pending[req_id] = q
        try:
            await self._send(fd, ('callStorm', req_id, text, opts))
            deadline = asyncio.get_event_loop().time() + 300
            while True:
                try:
                    resp = await asyncio.wait_for(q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if asyncio.get_event_loop().time() > deadline:
                        return await self._orig_callStorm(text, opts=opts)
                    continue
                if resp is None:
                    return await self._orig_callStorm(text, opts=opts)
                if resp[0] == 'result':
                    return resp[2]
                elif resp[0] == 'err':
                    return await self._orig_callStorm(text, opts=opts)
        except asyncio.CancelledError:
            return await self._orig_callStorm(text, opts=opts)
        finally:
            self._pending.pop(req_id, None)

    async def _send(self, fd, msg):
        data = s_msgpack.en(msg)
        lock = self._send_locks.setdefault(fd, asyncio.Lock())
        loop = asyncio.get_running_loop()
        async with lock:
            await loop.run_in_executor(None, self._sendall, fd, data)

    def _sendall(self, fd, data):
        mv = memoryview(data)
        while mv:
            try:
                sent = os.write(fd, mv)
            except OSError:
                return
            mv = mv[sent:]
