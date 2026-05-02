import re
import logging

import synapse.exc as s_exc
import synapse.telepath as s_telepath

logger = logging.getLogger(__name__)

# Storm operations that mutate data
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

# Regex for Storm edit bracket syntax: [ inet:ipv4=1.2.3.4 ]
# Must not match variable indexing like $x[0] or $lib.list()[0]
_re_edit_bracket = re.compile(r'(?<![a-zA-Z0-9_\)\]])\[')

# Regex for Storm commands that are write operations
_re_write_cmd = re.compile(
    r'\|\s*(' + '|'.join(re.escape(c) for c in sorted(_write_commands, key=len, reverse=True)) + r')\b'
)

# Storm keywords/patterns that indicate writes
_re_write_patterns = re.compile(
    r'\$node\.data\.set\b'
    r'|\$node\.data\.pop\b'
    r'|\$lib\.queue\.\w+\.put\b'
    r'|->>\s*\w+'  # edge add via ->> syntax
)


def classify(text):
    '''
    Classify a Storm query as 'read' or 'write'.

    Returns:
        str: 'read' or 'write'
    '''
    stripped = _strip_comments(text)

    if _re_edit_bracket.search(stripped):
        return 'write'

    if _re_write_cmd.search(stripped):
        return 'write'

    if _re_write_patterns.search(stripped):
        return 'write'

    return 'read'


def _strip_comments(text):
    '''Remove // line comments from Storm text.'''
    return re.sub(r'//[^\n]*', '', text)


class QueryRouter:
    '''
    Routes Storm queries to local Cortex or remote readers based on classification.
    '''

    def __init__(self, reader_urls):
        self.reader_urls = list(reader_urls)
        self._proxies = {}  # url -> proxy
        self._healthy = list(reader_urls)
        self._rr_index = 0

    async def fini(self):
        for proxy in self._proxies.values():
            await proxy.fini()
        self._proxies.clear()
        self._healthy.clear()

    async def _getProxy(self, url):
        proxy = self._proxies.get(url)
        if proxy is not None and not proxy.isfini:
            return proxy

        try:
            proxy = await s_telepath.openurl(url)
            if proxy.isfini:
                return None
            self._proxies[url] = proxy
            if url not in self._healthy:
                self._healthy.append(url)
            return proxy
        except Exception:
            logger.warning('Failed to connect to reader: %s', url)
            self._proxies.pop(url, None)
            if url in self._healthy:
                self._healthy.remove(url)
            return None

    async def getReaderProxy(self):
        '''Get a healthy reader proxy via round-robin. Returns None if none available.'''
        if not self._healthy:
            # Try reconnecting to all known URLs
            for url in self.reader_urls:
                if await self._getProxy(url) is not None:
                    break

        if not self._healthy:
            return None

        for _ in range(len(self._healthy)):
            url = self._healthy[self._rr_index % len(self._healthy)]
            self._rr_index += 1

            proxy = await self._getProxy(url)
            if proxy is not None:
                return proxy

        return None

    async def route(self, text, opts=None):
        '''
        Route a query to the appropriate target.

        Returns:
            (proxy_or_none, is_local): proxy is None when query should run locally.
        '''
        if classify(text) == 'write':
            return (None, True)

        proxy = await self.getReaderProxy()
        if proxy is None:
            return (None, True)

        return (proxy, False)
