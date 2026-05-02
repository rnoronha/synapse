'''
Tests for QueryRouter admission control and backpressure (G6.5).
'''
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import synapse.exc as s_exc
import synapse.lib.queryrouter as s_queryrouter


def _make_mock_proxy(isfini=False):
    proxy = AsyncMock()
    proxy.isfini = isfini
    return proxy


class TestQueryQueue(unittest.TestCase):

    def _make_router(self, urls=None, max_concurrent=10, queue_depth=100):
        urls = urls or ['tcp://reader1:1234', 'tcp://reader2:1234']
        router = s_queryrouter.QueryRouter(urls)
        router._max_concurrent = max_concurrent
        router._queue_depth = queue_depth
        # Rebuild semaphores with new max_concurrent
        router._reader_semaphores = {u: asyncio.Semaphore(max_concurrent) for u in urls}
        return router

    def test_queue_depth_reject(self):
        '''Reject queries when pending count reaches queue_depth.'''
        async def _test():
            router = self._make_router(queue_depth=2, max_concurrent=100)
            proxy = _make_mock_proxy()

            with patch.object(router, 'getReaderProxy', return_value=proxy):
                router._proxies = {'tcp://reader1:1234': proxy}

                # Fill up to queue_depth
                r1 = await router.route('inet:ipv4')
                r2 = await router.route('inet:ipv4')
                self.assertEqual(router._pending, 2)

                # Third should be rejected
                with self.assertRaises(s_exc.SynErr) as cm:
                    await router.route('inet:ipv4')
                self.assertIn('queue full', str(cm.exception).lower())
                self.assertEqual(router._rejected, 1)

                # Release one, then routing should work again
                router.release(r1[2])
                self.assertEqual(router._pending, 1)
                r3 = await router.route('inet:ipv4')
                self.assertEqual(router._pending, 2)
                self.assertIsNotNone(r3[0])

        asyncio.run(_test())

    def test_semaphore_timeout(self):
        '''Timeout when reader semaphore is exhausted.'''
        async def _test():
            router = self._make_router(max_concurrent=1, queue_depth=100)
            router._queue_timeout = 0.1  # 100ms for fast test
            proxy = _make_mock_proxy()

            with patch.object(router, 'getReaderProxy', return_value=proxy):
                router._proxies = {'tcp://reader1:1234': proxy}

                # Acquire the one slot
                r1 = await router.route('inet:ipv4')
                self.assertEqual(router._pending, 1)

                # Next should timeout on semaphore
                with self.assertRaises(s_exc.SynErr) as cm:
                    await router.route('inet:ipv4')
                self.assertIn('timeout', str(cm.exception).lower())
                # pending should not leak on timeout
                self.assertEqual(router._pending, 1)
                self.assertEqual(router._rejected, 1)

        asyncio.run(_test())

    def test_release_after_query(self):
        '''Release decrements pending and frees semaphore.'''
        async def _test():
            router = self._make_router(max_concurrent=2, queue_depth=100)
            proxy = _make_mock_proxy()
            url = 'tcp://reader1:1234'

            with patch.object(router, 'getReaderProxy', return_value=proxy):
                router._proxies = {url: proxy}

                r1 = await router.route('inet:ipv4')
                r2 = await router.route('inet:ipv4')
                self.assertEqual(router._pending, 2)

                sem = router._reader_semaphores[url]
                # Both slots acquired
                self.assertEqual(sem._value, 0)

                router.release(r1[2])
                self.assertEqual(router._pending, 1)
                self.assertEqual(sem._value, 1)

                router.release(r2[2])
                self.assertEqual(router._pending, 0)
                self.assertEqual(sem._value, 2)

        asyncio.run(_test())

    def test_write_queries_bypass_admission(self):
        '''Write queries skip admission control entirely.'''
        async def _test():
            router = self._make_router(queue_depth=0)  # zero depth = reject all reads
            proxy, is_local, reader_url = await router.route('[inet:ipv4=1.2.3.4]')
            self.assertTrue(is_local)
            self.assertIsNone(proxy)
            self.assertIsNone(reader_url)
            self.assertEqual(router._pending, 0)

        asyncio.run(_test())

    def test_get_stats(self):
        '''get_stats returns correct admission control metrics.'''
        async def _test():
            router = self._make_router(max_concurrent=5, queue_depth=50)
            proxy = _make_mock_proxy()
            url = 'tcp://reader1:1234'

            with patch.object(router, 'getReaderProxy', return_value=proxy):
                router._proxies = {url: proxy}
                await router.route('inet:ipv4')

            stats = router.get_stats()
            self.assertEqual(stats['pending'], 1)
            self.assertEqual(stats['rejected'], 0)
            self.assertEqual(stats['max_concurrent'], 5)
            self.assertEqual(stats['queue_depth'], 50)
            self.assertEqual(stats['per_reader_pending'][url], 1)

        asyncio.run(_test())

    def test_release_idempotent_on_unknown_url(self):
        '''Release with unknown URL does not crash.'''
        router = self._make_router()
        router._pending = 1
        router.release('tcp://unknown:9999')
        self.assertEqual(router._pending, 0)

    def test_pending_never_negative(self):
        '''Release never drives pending below zero.'''
        router = self._make_router()
        router.release('tcp://reader1:1234')
        self.assertEqual(router._pending, 0)


if __name__ == '__main__':
    unittest.main()
