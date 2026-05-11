# pragma: no cover
import sys
import asyncio

import synapse.cortex as s_cortex
import synapse.lib.forkmode as s_forkmode

if __name__ == '__main__':  # pragma: no cover
    s_forkmode.run(s_cortex.Cortex, sys.argv[1:])
