'''
Fork orchestrator for multi-process Cortex read workers.

The Arbiter forks N read-only worker processes after the Cortex has fully
initialized.  Workers inherit the listening socket and accept connections
directly (prefork model).  The parent retains the arbiter role: it detects
crashed workers via SIGCHLD and respawns them, and performs orderly shutdown
on SIGTERM.
'''
import os
import signal
import logging
import time

import synapse.lib.lmdbslab as s_lmdbslab

logger = logging.getLogger(__name__)

# Seconds to wait for workers to exit after SIGTERM before sending SIGKILL.
SHUTDOWN_GRACE = 5.0

# Seconds between waitpid polls during shutdown drain.
SHUTDOWN_POLL = 0.1


def _close_all_slabs():
    '''Close every open LMDB environment so children don't inherit parent mmaps.'''
    for slab in list(s_lmdbslab.Slab.allslabs.values()):
        try:
            slab.lenv.close()
        except Exception:
            logger.warning('Failed to close slab %s pre-fork', slab.path, exc_info=True)
    s_lmdbslab.Slab.allslabs.clear()


class Arbiter:
    '''Fork orchestrator and worker lifecycle manager.'''

    def __init__(self):
        self._listen_sock = None
        self._uds_path = None
        self._worker_pids = []  # ordered list of child pids
        self._num_workers = 0
        self._worker_main = None
        self._shutdown_flag = False

    def fork_workers(self, num_workers, listen_sock, uds_path, worker_main):
        '''
        Fork *num_workers* read-only worker processes.

        Must be called after Cortex init completes and the asyncio event loop
        has been torn down (no running loop, no threads).

        Args:
            num_workers: Number of worker processes to fork.
            listen_sock: The bound/listening ``socket.socket`` workers inherit.
            uds_path: Filesystem path of the writer's UDS endpoint.
            worker_main: Callable ``worker_main(listen_sock, uds_path, worker_id)``
                         invoked in each child.  Must not return (call ``os._exit``).

        Returns:
            list[int]: PIDs of the forked workers.
        '''
        self._listen_sock = listen_sock
        self._uds_path = uds_path
        self._num_workers = num_workers
        self._worker_main = worker_main

        _close_all_slabs()

        self._install_parent_signals()

        for i in range(num_workers):
            self._fork_one(i)

        logger.info('Forked %d workers: %s', num_workers, self._worker_pids)
        return list(self._worker_pids)

    # ------------------------------------------------------------------
    # internal fork helpers
    # ------------------------------------------------------------------

    def _fork_one(self, worker_id):
        pid = os.fork()
        if pid == 0:
            # --- child ---
            self._in_child(worker_id)
            # worker_main must not return; belt-and-suspenders:
            os._exit(1)

        # --- parent ---
        self._worker_pids.append(pid)
        logger.info('Forked worker %d (pid %d)', worker_id, pid)

    def _in_child(self, worker_id):
        '''Run in the child process immediately after fork.'''
        # Reset inherited parent signal handlers.
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        # SIGTERM/SIGINT: let worker_main install its own handlers.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        self._worker_main(self._listen_sock, self._uds_path, worker_id)

    # ------------------------------------------------------------------
    # parent signal handlers
    # ------------------------------------------------------------------

    def _install_parent_signals(self):
        signal.signal(signal.SIGCHLD, self._handle_sigchld)

    def _handle_sigchld(self, signum, frame):
        '''Reap exited children and schedule respawn for crashed workers.'''
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                break

            if pid in self._worker_pids:
                idx = self._worker_pids.index(pid)
                self._worker_pids[idx] = None

                if os.WIFSIGNALED(status):
                    sig = os.WTERMSIG(status)
                    logger.error('Worker pid %d killed by signal %d', pid, sig)
                else:
                    code = os.WEXITSTATUS(status)
                    logger.error('Worker pid %d exited with code %d', pid, code)

                if not self._shutdown_flag:
                    self.restart_worker(idx)

    # ------------------------------------------------------------------
    # restart / shutdown
    # ------------------------------------------------------------------

    def restart_worker(self, idx):
        '''Respawn the worker at slot *idx*.'''
        old_pid = self._worker_pids[idx]
        if old_pid is not None:
            logger.warning('restart_worker called for slot %d but pid %d still tracked', idx, old_pid)

        pid = os.fork()
        if pid == 0:
            self._in_child(idx)
            os._exit(1)

        self._worker_pids[idx] = pid
        logger.info('Respawned worker slot %d as pid %d', idx, pid)

    def shutdown(self):
        '''Send SIGTERM to all workers, wait with timeout, SIGKILL stragglers.'''
        self._shutdown_flag = True

        alive = [p for p in self._worker_pids if p is not None]
        if not alive:
            return

        # Phase 1: SIGTERM
        for pid in alive:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

        # Phase 2: wait up to SHUTDOWN_GRACE seconds
        deadline = time.monotonic() + SHUTDOWN_GRACE
        while time.monotonic() < deadline:
            alive = [p for p in self._worker_pids if p is not None]
            if not alive:
                return
            for pid in alive:
                try:
                    rpid, _ = os.waitpid(pid, os.WNOHANG)
                    if rpid != 0:
                        idx = self._worker_pids.index(pid)
                        self._worker_pids[idx] = None
                except ChildProcessError:
                    idx = self._worker_pids.index(pid)
                    self._worker_pids[idx] = None
            time.sleep(SHUTDOWN_POLL)

        # Phase 3: SIGKILL stragglers
        for i, pid in enumerate(self._worker_pids):
            if pid is None:
                continue
            logger.warning('Worker pid %d did not exit in time, sending SIGKILL', pid)
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
            self._worker_pids[i] = None
