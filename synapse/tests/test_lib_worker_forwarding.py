'''
Unit tests for the worker's write forwarding via socketpair RPC.

Tests the worker-side forwarding methods (_forward_write_stream,
_forward_callStorm) using socketpairs and a mock writer, without
requiring a full Cortex or fork.
'''
import os
import socket
import asyncio
import unittest
import threading

import msgpack

import synapse.exc as s_exc
import synapse.lib.msgpack as s_msgpack
from synapse.lib.worker import ReadOnlyWorker


def _mock_writer_thread(writer_fd, responses):
    '''Simulate the writer process: read requests, send canned responses.'''
    unpacker = msgpack.Unpacker(**s_msgpack.unpacker_kwargs)
    try:
        while True:
            data = os.read(writer_fd, 65536)
            if not data:
                break
            unpacker.feed(data)
            for msg in unpacker:
                req_id = msg[1]
                kind = msg[0]
                for resp in responses.get(kind, []):
                    # Replace placeholder req_id with actual
                    out = (resp[0], req_id) + resp[2:]
                    os.write(writer_fd, s_msgpack.en(out))
    except OSError:
        pass


class TestWorkerWriteForwarding(unittest.TestCase):

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_forward_write_stream(self):
        '''Worker streams storm messages from writer via socketpair.'''
        worker_end, writer_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        worker_fd = worker_end.fileno()
        writer_fd = writer_end.fileno()
        worker_end.detach()
        writer_end.detach()

        responses = {
            'storm': [
                ('msg', None, ('node', ({'ndef': ('inet:fqdn', 'test.com')},))),
                ('msg', None, ('init', {'tick': 123})),
                ('done', None),
            ]
        }

        t = threading.Thread(target=_mock_writer_thread, args=(writer_fd, responses))
        t.daemon = True
        t.start()

        worker = ReadOnlyWorker(0, '/tmp/unused', write_fd=worker_fd)

        async def _test():
            msgs = []
            async for mesg in worker._forward_write_stream('[ inet:fqdn=test.com ]', None):
                msgs.append(mesg)
            return msgs

        msgs = self._run(_test())
        os.close(worker_fd)
        os.close(writer_fd)

        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0][0], 'node')
        self.assertEqual(msgs[1][0], 'init')

    def test_forward_callStorm(self):
        '''Worker receives callStorm result from writer via socketpair.'''
        worker_end, writer_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        worker_fd = worker_end.fileno()
        writer_fd = writer_end.fileno()
        worker_end.detach()
        writer_end.detach()

        responses = {
            'callStorm': [
                ('result', None, 'test.com'),
            ]
        }

        t = threading.Thread(target=_mock_writer_thread, args=(writer_fd, responses))
        t.daemon = True
        t.start()

        worker = ReadOnlyWorker(0, '/tmp/unused', write_fd=worker_fd)

        async def _test():
            return await worker._forward_callStorm('[ inet:fqdn=test.com ] return($node.repr())', None)

        result = self._run(_test())
        os.close(worker_fd)
        os.close(writer_fd)

        self.assertEqual(result, 'test.com')

    def test_forward_write_error(self):
        '''Worker raises SynErr when writer returns error.'''
        worker_end, writer_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        worker_fd = worker_end.fileno()
        writer_fd = writer_end.fileno()
        worker_end.detach()
        writer_end.detach()

        responses = {
            'storm': [
                ('err', None, {'mesg': 'IsReadOnly', 'err': 'IsReadOnly'}),
            ]
        }

        t = threading.Thread(target=_mock_writer_thread, args=(writer_fd, responses))
        t.daemon = True
        t.start()

        worker = ReadOnlyWorker(0, '/tmp/unused', write_fd=worker_fd)

        async def _test():
            msgs = []
            async for mesg in worker._forward_write_stream('bad query', None):
                msgs.append(mesg)

        with self.assertRaises(s_exc.SynErr):
            self._run(_test())

        os.close(worker_fd)
        os.close(writer_fd)

    def test_dead_write_channel(self):
        '''Worker raises SynErr when write channel is dead.'''
        worker = ReadOnlyWorker(0, '/tmp/unused', write_fd=None)

        async def _test_stream():
            async for _ in worker._forward_write_stream('query', None):
                pass

        async def _test_call():
            await worker._forward_callStorm('query', None)

        with self.assertRaises(s_exc.SynErr):
            self._run(_test_stream())

        with self.assertRaises(s_exc.SynErr):
            self._run(_test_call())

    def test_broken_pipe_marks_dead(self):
        '''Worker marks write channel dead on BrokenPipeError.'''
        worker_end, writer_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        worker_fd = worker_end.fileno()
        writer_fd = writer_end.fileno()
        worker_end.detach()
        writer_end.detach()

        # Close writer end immediately to cause BrokenPipeError on write
        os.close(writer_fd)

        worker = ReadOnlyWorker(0, '/tmp/unused', write_fd=worker_fd)
        self.assertTrue(worker._write_alive)

        async def _test():
            async for _ in worker._forward_write_stream('query', None):
                pass

        with self.assertRaises(s_exc.SynErr):
            self._run(_test())

        self.assertFalse(worker._write_alive)
        os.close(worker_fd)


if __name__ == '__main__':
    unittest.main()
