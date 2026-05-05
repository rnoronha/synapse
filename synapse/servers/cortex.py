# pragma: no cover
import sys

import synapse.cortex as s_cortex

if __name__ == '__main__':  # pragma: no cover
    # startmain supports the init → fork → serve lifecycle for multi-process mode.
    # Falls back to the normal asyncio.run(execmain) path when fork mode is not configured.
    s_cortex.Cortex.startmain(sys.argv[1:])
