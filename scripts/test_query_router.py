'''
Tests for synapse.lib.queryrouter — query classification and routing.
'''
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import synapse.lib.queryrouter as s_queryrouter

# 31 test cases: (query, expected_classification)
CLASSIFY_CASES = [
    # --- Read queries ---
    ('inet:ipv4', 'read'),
    ('inet:ipv4=1.2.3.4', 'read'),
    ('inet:fqdn=vertex.link', 'read'),
    ('inet:ipv4 +:asn=1234', 'read'),
    ('inet:fqdn | uniq', 'read'),
    ('inet:ipv4 | count', 'read'),
    ('#malware.redtree', 'read'),
    ('inet:ipv4 -> inet:dns:a -> inet:fqdn', 'read'),
    ('inet:ipv4 | limit 10', 'read'),
    ('inet:ipv4 +:loc=us', 'read'),
    ('$x = $lib.str.format("test") return($x)', 'read'),
    ('inet:ipv4 | graph --degrees 2', 'read'),
    ('inet:ipv4 $val=$node.value() | spin', 'read'),
    ('help', 'read'),
    ('inet:ipv4 | reindex --subs', 'read'),
    ('inet:ipv4 +:asn=1234 +:loc=us | sort :asn', 'read'),

    # --- Write queries ---
    ('[inet:ipv4=1.2.3.4]', 'write'),
    ('[inet:fqdn=vertex.link +#malware.redtree]', 'write'),
    ('inet:ipv4=1.2.3.4 [+#cno.infra]', 'write'),
    ('inet:ipv4=1.2.3.4 [:asn=1234]', 'write'),
    ('inet:ipv4=1.2.3.4 | delnode', 'write'),
    ('inet:ipv4=1.2.3.4 [-#malware.redtree]', 'write'),
    ('| auth.user.add visi', 'write'),
    ('| trigger.add node:add --form inet:ipv4 --query {[+#auto]}', 'write'),
    ('| cron.add --hour 1 {inet:ipv4 | count}', 'write'),
    ('| queue.add myqueue', 'write'),
    ('| pkg.load /path/to/pkg', 'write'),
    ('| service.add svc tcp://svc:1234/svc', 'write'),
    ('| macro.set mymacro {inet:ipv4}', 'write'),
    ('| feed.ingest [inet:ipv4=5.6.7.8]', 'write'),
    ('| view.add --name myview', 'write'),
]


class TestClassify(unittest.TestCase):

    def test_classify_all_cases(self):
        for query, expected in CLASSIFY_CASES:
            with self.subTest(query=query):
                result = s_queryrouter.classify(query)
                self.assertEqual(result, expected, f'classify({query!r}) = {result!r}, expected {expected!r}')

    def test_classify_count(self):
        self.assertEqual(len(CLASSIFY_CASES), 31)

    def test_classify_comment_stripping(self):
        self.assertEqual(s_queryrouter.classify('inet:ipv4 // [fake edit]'), 'read')

    def test_classify_dollar_bracket_not_edit(self):
        self.assertEqual(s_queryrouter.classify('$x=$lib.list(1,2) $y=$x[0]'), 'read')


class TestQueryRouter(unittest.TestCase):

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_route_write_is_local(self):
        router = s_queryrouter.QueryRouter(['tcp://reader1:1234/cortex'])
        proxy, is_local, reader_url = self._run(router.route('[inet:ipv4=1.2.3.4]'))
        self.assertIsNone(proxy)
        self.assertTrue(is_local)
        self._run(router.fini())

    def test_route_read_with_no_readers(self):
        router = s_queryrouter.QueryRouter([])
        proxy, is_local, reader_url = self._run(router.route('inet:ipv4'))
        self.assertIsNone(proxy)
        self.assertTrue(is_local)
        self._run(router.fini())

    @patch('synapse.lib.queryrouter.s_telepath.openurl', new_callable=AsyncMock)
    def test_route_read_to_reader(self, mock_openurl):
        mock_proxy = AsyncMock()
        mock_proxy.isfini = False
        mock_openurl.return_value = mock_proxy

        router = s_queryrouter.QueryRouter(['tcp://reader1:1234/cortex'])
        proxy, is_local, reader_url = self._run(router.route('inet:ipv4'))
        self.assertIs(proxy, mock_proxy)
        self.assertFalse(is_local)
        router.release(reader_url)
        self._run(router.fini())

    @patch('synapse.lib.queryrouter.s_telepath.openurl', new_callable=AsyncMock)
    def test_route_round_robin(self, mock_openurl):
        proxies = [AsyncMock(), AsyncMock()]
        for p in proxies:
            p.isfini = False

        async def side_effect(url):
            return proxies[0] if 'reader1' in url else proxies[1]

        mock_openurl.side_effect = side_effect

        router = s_queryrouter.QueryRouter([
            'tcp://reader1:1234/cortex',
            'tcp://reader2:1234/cortex',
        ])

        proxy1, _, url1 = self._run(router.route('inet:ipv4'))
        proxy2, _, url2 = self._run(router.route('inet:fqdn'))

        self.assertIs(proxy1, proxies[0])
        self.assertIs(proxy2, proxies[1])
        router.release(url1)
        router.release(url2)
        self._run(router.fini())

    @patch('synapse.lib.queryrouter.s_telepath.openurl', new_callable=AsyncMock)
    def test_route_fallback_on_dead_reader(self, mock_openurl):
        mock_openurl.side_effect = ConnectionRefusedError('dead')

        router = s_queryrouter.QueryRouter(['tcp://reader1:1234/cortex'])
        proxy, is_local, reader_url = self._run(router.route('inet:ipv4'))
        self.assertIsNone(proxy)
        self.assertTrue(is_local)
        self._run(router.fini())

    @patch('synapse.lib.queryrouter.s_telepath.openurl', new_callable=AsyncMock)
    def test_reconnect_after_fini(self, mock_openurl):
        dead_proxy = AsyncMock()
        dead_proxy.isfini = True

        fresh_proxy = AsyncMock()
        fresh_proxy.isfini = False

        mock_openurl.side_effect = [dead_proxy, fresh_proxy]

        router = s_queryrouter.QueryRouter(['tcp://reader1:1234/cortex'])

        # First call gets dead proxy, falls back to local
        proxy, is_local, reader_url = self._run(router.route('inet:ipv4'))
        self.assertIsNone(proxy)
        self.assertTrue(is_local)

        # Second call reconnects and gets fresh proxy
        proxy, is_local, reader_url = self._run(router.route('inet:ipv4'))
        self.assertIs(proxy, fresh_proxy)
        self.assertFalse(is_local)
        router.release(reader_url)
        self._run(router.fini())


if __name__ == '__main__':
    unittest.main()
