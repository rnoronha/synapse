'''
Write channel listener for the writer process.

The writer process listens on per-worker write channel socketpairs via epoll.
Workers send write requests (storm/callStorm) as msgpack-encoded tuples;
the writer executes them against the cell and streams results back.

Protocol (all messages are msgpack-encoded, self-delimiting on SOCK_STREAM):

  Request:  ('storm', req_id, text, opts)
  Response: ('msg', req_id, mesg) ...  (one per storm yield)
            ('done', req_id)           (stream complete)
            ('err', req_id, excinfo)   (on error)
'''
import os
import select
import asyncio
import logging

import msgpack

import synapse.lib.msgpack as s_msgpack

logger = logging.getLogger(__name__)

# Read buffer size for recv() calls on the write channel socketpairs.
_RECV_BUF = 65536


class WriteChannelListener:
    '''Epoll-based listener that receives write RPCs from worker socketpairs.

    Args:
        cell: The Cortex cell object (provides storm/callStorm methods).
        writer_fds: List of writer-end file descriptors from the arbiter.
    '''

    def __init__(self, cell, writer_fds):
        self._cell = cell
        self._writer_fds = list(writer_fds)
        self._running = False
        self._task = None
        self._ready = asyncio.Event()
        self._send_locks = {}
        self._new_fds = []

    async def add_fd(self, fd):
        '''Register a new worker write channel fd after worker restart.'''
        self._writer_fds.append(fd)
        self._new_fds.append(fd)

    async def start(self):
        '''Start the write channel listener and wait until it is polling.

        Returns only after the epoll loop has entered its first poll(),
        ensuring that any writes already buffered in the socketpairs will
        be picked up.
        '''
        self._running = True
        self._task = asyncio.get_running_loop().create_task(self._listen())
        await self._ready.wait()
        logger.info('Write channel listener ready with %d worker fds', len(self._writer_fds))

    async def stop(self):
        '''Stop the listener and close all writer-end fds.'''
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for fd in self._writer_fds:
            try:
                os.close(fd)
            except OSError:
                pass
        logger.info('Write channel listener stopped')

    async def _listen(self):
        '''Main listener loop: epoll on all writer fds, dispatch requests.'''
        loop = asyncio.get_running_loop()
        ep = select.epoll()

        # Per-fd msgpack unpacker for SOCK_STREAM framing
        unpackers = {}
        for fd in self._writer_fds:
            ep.register(fd, select.EPOLLIN)
            unpackers[fd] = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)

        # Signal readiness: send a single byte on each writer-end fd so
        # workers know the write channel is ready to receive requests.
        for fd in self._writer_fds:
            try:
                os.write(fd, b'\x01')
            except OSError as e:
                logger.warning('Failed to send ready byte on fd %d: %s', fd, e)

        # Signal internal ready event
        self._ready.set()

        try:
            while self._running:
                # Register any new fds added via add_fd()
                while self._new_fds:
                    nfd = self._new_fds.pop(0)
                    ep.register(nfd, select.EPOLLIN)
                    unpackers[nfd] = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)
                    try:
                        os.write(nfd, b'\x01')
                    except OSError as e:
                        logger.warning('Failed to send ready byte on new fd %d: %s', nfd, e)

                # Poll in executor to avoid blocking the event loop
                events = await loop.run_in_executor(None, ep.poll, 1.0)
                for fd, event in events:
                    if event & (select.EPOLLHUP | select.EPOLLERR):
                        logger.warning('Write channel fd %d closed (worker died)', fd)
                        ep.unregister(fd)
                        unpackers.pop(fd, None)
                        try:
                            os.close(fd)
                        except OSError:
                            pass
                        continue

                    if event & select.EPOLLIN:
                        try:
                            data = os.read(fd, _RECV_BUF)
                        except OSError:
                            continue
                        if not data:
                            # EOF — worker closed its end (logged above as warning)
                            logger.warning('Write channel fd %d EOF', fd)
                            ep.unregister(fd)
                            unpackers.pop(fd, None)
                            try:
                                os.close(fd)
                            except OSError:
                                pass  # close failure is benign; EOF already logged
                            continue

                        unpacker = unpackers[fd]
                        unpacker.feed(data)
                        for msg in unpacker:
                            # Dispatch each request as a concurrent task
                            loop.create_task(self._handle_request(fd, msg))
        except asyncio.CancelledError:
            pass
        finally:
            ep.close()

    async def _handle_request(self, fd, msg):
        '''Handle a single write request and stream results back.

        Args:
            fd: The writer-end fd to send responses on.
            msg: Decoded msgpack tuple: ('storm', req_id, text, opts)
        '''
        try:
            kind = msg[0]
            req_id = msg[1]
        except (IndexError, TypeError):
            logger.error('Malformed write request on fd %d: %r', fd, msg)
            return

        if kind == 'storm':
            await self._handle_storm(fd, req_id, msg)
        elif kind == 'callStorm':
            await self._handle_callStorm(fd, req_id, msg)
        else:
            logger.error('Unknown write request kind %r on fd %d', kind, fd)
            await self._send(fd, ('err', req_id, {'mesg': f'Unknown request kind: {kind}'}))

    async def _handle_storm(self, fd, req_id, msg):
        '''Execute cell.storm() and stream results back to the worker.'''
        try:
            text = msg[2]
            opts = msg[3] if len(msg) > 3 else None
        except (IndexError, TypeError):
            await self._send(fd, ('err', req_id, {'mesg': 'Malformed storm request'}))
            return

        try:
            count = 0
            async for mesg in self._cell.storm(text, opts=opts):
                await self._send(fd, ('msg', req_id, mesg))
                count += 1
            await self._send(fd, ('done', req_id))
        except Exception as e:
            excinfo = {
                'mesg': str(e),
                'err': e.__class__.__name__,
            }
            await self._send(fd, ('err', req_id, excinfo))

    async def _handle_callStorm(self, fd, req_id, msg):
        '''Execute cell.callStorm() and return the result to the worker.'''
        try:
            text = msg[2]
            opts = msg[3] if len(msg) > 3 else None
        except (IndexError, TypeError):
            await self._send(fd, ('err', req_id, {'mesg': 'Malformed callStorm request'}))
            return

        try:
            result = await self._cell.callStorm(text, opts=opts)
            await self._send(fd, ('result', req_id, result))
        except Exception as e:
            excinfo = {
                'mesg': str(e),
                'err': e.__class__.__name__,
            }
            await self._send(fd, ('err', req_id, excinfo))

    async def _send(self, fd, msg):
        '''Send a msgpack-encoded message on the given fd.

        Uses a per-fd lock to prevent interleaved partial writes from
        concurrent tasks, and run_in_executor to avoid blocking the
        event loop if the kernel buffer is full.
        '''
        data = s_msgpack.en(msg)
        lock = self._send_locks.setdefault(fd, asyncio.Lock())
        async with lock:
            await asyncio.get_running_loop().run_in_executor(None, self._sendall, fd, data)

    def _sendall(self, fd, data):
        '''Blocking write loop for use in an executor.'''
        mv = memoryview(data)
        while mv:
            try:
                sent = os.write(fd, mv)
            except OSError as e:
                logger.warning('Write channel send failed on fd %d: %s', fd, e)
                return
            mv = mv[sent:]
