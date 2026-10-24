"""Explicit, one-shot borrowed native API stores; no acquisition or release of DBs.

Caller owns native construction/preflight, profile lease, connection lifetime and
readiness policy. This seam is not proof of any of those. Borrowed API listeners
remain disabled until the complete harness admission path is qualified.
"""
from __future__ import annotations
import asyncio
import os
from pathlib import Path
import sqlite3
import threading


class BorrowedStoreRejected(RuntimeError):
    pass


def require(ok, reason):
    if not ok:
        raise BorrowedStoreRejected(reason)


class BorrowedAPIStores:
    """Trusted local handles, single adapter claim; never a serialized capability.

    Does not open/close SQLite or release caller ownership. Selection retains the
    real native objects and rejects changed handles, process or profile. No global
    protection against malicious same-process code constructing another bundle.
    """
    def __init__(self, home, state, replay, responses, *, prepared_runner=None):
        from hermes_state import SessionDB
        from hermes_cli.sqlite_safe_read import TrackedConnection
        from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
        from gateway.platforms.api_server_response_store import ResponseStore
        require(type(home) in (str, Path) or isinstance(home, Path), 'borrowed_home_type')
        home = Path(home)
        require(home.is_absolute() and home == home.resolve(strict=True)
                and home.is_dir() and not home.is_symlink(), 'borrowed_home')
        require(type(state) is SessionDB and type(replay) is RunIdempotencyStore
                and type(responses) is ResponseStore, 'borrowed_native_types')
        require(state.read_only is False and state._shared_registry_owned is False,
                'borrowed_state_owner')
        require(replay.durable is True and responses._db_path is not None,
                'borrowed_memory_fallback')
        from gateway.platforms.api_server_response_state import BorrowedResponseState
        canonical = getattr(responses, '_canonical_state', None)
        require(type(canonical) is BorrowedResponseState and canonical._state is state
                and canonical._connection is state._conn, 'borrowed_response_canonical_required')
        with canonical.read():
            pass
        stores = (state, replay, responses)
        conns = tuple(s._conn for s in stores)
        require(type(conns[0]) is TrackedConnection and all(type(c) is sqlite3.Connection for c in conns[1:])
                and len({id(c) for c in conns}) == 3, 'borrowed_native_connections')
        paths = (home/'state.db', home/'runs_idempotency.db', home/'response_store.db')
        declared = (state.db_path, replay._db_path, responses._db_path)
        for connection, path, name in zip(conns, paths, declared):
            require(Path(name) == path and not connection.in_transaction, 'borrowed_path_or_transaction')
            rows = [tuple(r) for r in connection.execute('PRAGMA database_list')]
            require(len([r for r in rows if r[1]=='main' and r[2] and Path(r[2])==path]) == 1
                    and all(r[1]=='main' or (r[1]=='temp' and r[2]=='') for r in rows),
                    'borrowed_connection_path')
        self.home = home
        self.state, self.replay, self.responses = stores
        self._stores, self._connections = stores, conns
        self._actor = (os.getpid(), threading.get_ident())
        self._canonical = canonical
        self._state_lock = state._lock
        self._adapter = None
        self._stage = 'NEW'
        self._prepared_runner = self._prepared_binding = None
        self._prepared_adapter_ready = False
        if prepared_runner is not None:
            from gateway.platforms.api_server_prepared import bind_prepared_runner
            bind_prepared_runner(self, prepared_runner)

    def _check(self):
        require((os.getpid(), threading.get_ident()) == self._actor, 'borrowed_actor')
        require((self.state,self.replay,self.responses) == self._stores,
                'borrowed_store_changed')
        require(self.responses._canonical_state is self._canonical
                and self._canonical._state is self.state
                and self._canonical._connection is self._connections[0]
                and self.state._lock is self._state_lock, 'borrowed_canonical_changed')
        for store, connection in zip(self._stores,self._connections):
            require(store._conn is connection, 'borrowed_connection_changed')
            try:
                connection.in_transaction
            except sqlite3.Error:
                raise BorrowedStoreRejected('borrowed_connection_closed') from None

    def _home(self, home):
        require(Path(home) == self.home, 'borrowed_profile_mismatch')

    def claim(self, adapter, home):
        self._check();self._home(home)
        require(self._stage == 'NEW', 'borrowed_already_claimed')
        self._adapter = adapter
        self._stage = 'ATTACHED'

    def select(self, adapter, home):
        self._check();self._home(home)
        require(self._stage == 'ATTACHED' and self._adapter is adapter, 'borrowed_not_attached')
        if self._prepared_runner is None:
            require(getattr(adapter,'gateway_runner',None) is None, 'borrowed_runner_conflict')
        else:
            from gateway.platforms.api_server_prepared import check_prepared
            check_prepared(self, adapter)
        require(adapter._session_db is self.state and adapter._response_store is self.responses
                and adapter._run_idempotency_store is self.replay, 'borrowed_adapter_handles_changed')
        require(not adapter._session_dbs and not adapter._response_stores,
                'borrowed_foreign_cache')
        return self._stores

    def activate_admission(self):
        """Explicit local authority admission only; does not start a service."""
        from gateway.run_borrowed_admission import activate
        return activate(self)

    def begin_drain(self):
        """Withdraw new admission/claims; never cancel or delete existing work."""
        from gateway.run_borrowed_admission import begin_drain
        return begin_drain(self)

    def finish_drain(self):
        """Require caller/native work retirement before ordinary ordered detach."""
        from gateway.run_borrowed_admission import finish_drain
        return finish_drain(self)

    def status(self):
        require((os.getpid(), threading.get_ident()) == self._actor, 'borrowed_actor')
        return {'state':self._stage,'callerOwnsStores':True,'storesClosedByAdapter':False,
                'listenerAuthorized':False,'profileLeaseVerifiedHere':False,
                'preparedResponseSchemaVerifiedHere':False}


def _quiescent(adapter):
    from gateway.platforms.api_server_runs import api_worker_live_count
    # Native active_agent_work_count tolerates broken bookkeeping by returning0;
    # a release boundary must instead reject unknown/invalid counters.
    for name in ('_pending_agent_requests','_inflight_agent_runs'):
        value=getattr(adapter,name,None)
        require(type(value) is int and value == 0, 'borrowed_workers_active')
    count=api_worker_live_count()
    require(type(count) is int and count == 0, 'borrowed_workers_active')
    for name in ('_active_run_agents','_shutdown_interruptible_agents','_stopping_run_ids'):
        require(not getattr(adapter,name,None), 'borrowed_workers_active')
    for name in ('_background_tasks','_session_tasks','_active_run_tasks',
                 '_pending_text_batch_tasks','_detached_fatal_tasks'):
        values=getattr(adapter,name,())
        tasks=values.values() if isinstance(values,dict) else values
        require(not any(not task.done() for task in tasks), 'borrowed_tasks_active')


async def disconnect_borrowed(adapter):
    """Drain only adapter resources, keep all three caller-owned stores open.

    Incomplete cleanup is terminal UNKNOWN and cannot be blindly retried. This
    does not release a profile/image lease or authorize the caller to ignore
    unrelated writers. No HTTP listener can yet be started in borrowed mode.
    """
    bundle=adapter._borrowed_api_stores
    require((os.getpid(),threading.get_ident()) == bundle._actor, 'borrowed_actor')
    require(bundle._adapter is adapter, 'borrowed_adapter_mismatch')
    if bundle._stage == 'DETACHED':
        return
    bundle._check()
    if bundle._prepared_binding is not None:
        require(bundle._prepared_binding._stage in ('PREPARED', 'DRAINED'),
                'borrowed_admission_must_drain')
        if bundle._prepared_binding._stage == 'DRAINED':
            from gateway.run_borrowed_admission import assert_drained
            assert_drained(bundle)
    require(bundle._stage == 'ATTACHED', 'borrowed_cleanup_unknown_or_active')
    _quiescent(adapter)
    require(not any(c.in_transaction for c in bundle._connections), 'borrowed_transaction_active')
    bundle.select(adapter,bundle.home)
    bundle._stage='DRAINING'
    try:
        # Borrowed mode has no qualified native runtime-status file/writer.
        # Do not schedule that default writer outside the caller's ownership.
        adapter._running=False
        if adapter._site is not None:
            await adapter._site.stop()
            adapter._site=None
        if adapter._runner is not None:
            await adapter._runner.cleanup()
            adapter._runner=None
        await asyncio.to_thread(adapter._memory_sessions.close_all)
        _quiescent(adapter)
        bundle._check()
        require(not any(c.in_transaction for c in bundle._connections), 'borrowed_transaction_active')
        with adapter._session_db_cache_lock:
            adapter._session_db_cache_closed=True
        adapter._app=None
        if bundle._prepared_runner is not None:
            from gateway.platforms.api_server_prepared import check_prepared
            check_prepared(bundle, adapter)
            if bundle._prepared_binding._stage == 'DRAINED':
                from gateway.run_borrowed_admission import assert_drained
                assert_drained(bundle)
        bundle._stage='DETACHED'
    except BaseException:
        bundle._stage='CLOSE_UNKNOWN'
        raise
