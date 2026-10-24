"""Explicit local admission for the existing caller-owned runner/API pair.

No scheduler, listener, provider, store owner or epoch is created here. Draining
closes NEW admission/claims; existing accepted writers may still settle. Native
queued/unknown records are retained. All transitions use the owner event loop;
the native write-transaction gate is a pure identity/state read, so it can run
under the existing SQLite mutex on a worker thread without lock inversion.

This is a cooperative in-process seam, not hostile-thread protection, complete
work execution or permission to activate an unqualified harness/runtime image.
"""
import asyncio
import os
import sqlite3
from pathlib import Path
from collections import OrderedDict
from gateway.platforms.api_server_borrowed import BorrowedStoreRejected, require

RUNNER_FIELDS = (
    ('adapters', dict), ('_profile_adapters', dict), ('_background_tasks', set),
    ('_sessions', dict), ('_fatal_handler_tasks', set),
    ('_completion_notification_batch_tasks', dict),
    ('_completion_notification_batch_flush_tasks', set),
    ('_completion_notification_batches', dict), ('_completion_deliveries_inflight', set),
    ('_session_db_handles', dict), ('_startup_restore_tasks', list),
    ('_startup_restore_queue', list), ('_agent_cache', OrderedDict))
AUTHORITY_FIELDS = (
    ('sessions', dict), ('waiters', dict), ('events', dict), ('native_waiters', set),
    ('pending_results', dict), ('pending_deliveries', dict), ('adopted', dict),
    ('executing', dict), ('pending_stops', dict), ('pending_steers', dict),
    ('cancel_obligations', set))
LAZY_FIELDS = (('_turn_workers', dict), ('_managed_workers', dict),
    ('_mutation_tasks', set), ('_bot_mailbox_operations', set),
    ('_bot_receipt_tasks', set), ('_cron_cancellations', dict))


def _pair(bundle):
    from gateway.platforms.api_server_borrowed import BorrowedAPIStores
    from gateway.run_borrowed_state import CallerOwnedRunnerState
    from gateway.run import GatewayRunner
    from gateway.session_authority import SessionAuthority
    from gateway.platforms.api_server import APIServerAdapter
    require(type(bundle) is BorrowedAPIStores, 'admission_native_bundle')
    binding, runner, adapter = bundle._prepared_binding, bundle._prepared_runner, bundle._adapter
    require(type(binding) is CallerOwnedRunnerState and type(runner) is GatewayRunner
            and type(adapter) is APIServerAdapter, 'admission_native_pair')
    binding._lifecycle_actor()
    require(bundle._stage == 'ATTACHED' and binding._api_bundle is bundle
            and runner._caller_owned_state is binding and binding._runner is runner
            and bundle._prepared_adapter_ready is True and adapter._borrowed_api_stores is bundle
            and adapter.gateway_runner is runner and type(binding._authority) is SessionAuthority,
            'admission_pair_changed')
    loop = asyncio.get_running_loop()
    require(binding._admission_loop is None or binding._admission_loop is loop,
            'admission_other_loop')
    return binding, runner, adapter, loop


def _idle_work(binding, runner, adapter, *, allow_idle_sessions=False):
    from gateway.platforms.api_server_borrowed import _quiescent
    require(type(allow_idle_sessions) is bool, 'admission_idle_session_option')
    require(runner._running is False and runner._executor is None
            and runner._housekeeping_executor is None and runner._loop_heartbeat_task is None
            and runner._stop_task is None, 'admission_runner_lifecycle_active')
    for owner, fields in ((runner, RUNNER_FIELDS), (binding._authority, AUTHORITY_FIELDS)):
        for name, kind in fields:
            value = getattr(owner, name, None)
            if allow_idle_sessions and owner is binding._authority and name == 'sessions':
                require(type(value) is dict, 'admission_owned_work_retained')
            else:
                require(type(value) is kind and not value, 'admission_owned_work_retained')
    for name, kind in LAZY_FIELDS:
        value = getattr(binding._authority, name, None)
        require(value is None or type(value) is kind and not value, 'admission_owned_work_retained')
    # Do not call physical_workers(): it prunes retained records. No task.done()
    # inference, cancellation, join, eviction or session-registry mutation here.
    _quiescent(adapter)


def _idle_ledgers(bundle, binding):
    binding._check_state(quiescent=True)
    lock = bundle.replay._lock
    require(lock is bundle._prepared_replay_lock and lock.acquire(blocking=False),
            'admission_replay_busy')
    try:
        with bundle._canonical.read():
            require(not any(c.in_transaction for c in bundle._connections), 'admission_transaction_active')
            replay = bundle.replay._conn.execute("""SELECT 1 FROM run_idempotency WHERE CASE
                WHEN typeof(status_json) != 'text' OR length(CAST(status_json AS BLOB)) > 65536 THEN 1
                WHEN NOT json_valid(status_json) THEN 1
                WHEN json_type(status_json) != 'object' THEN 1
                WHEN EXISTS(SELECT 1 FROM json_tree(status_json) WHERE key IS NOT NULL
                    GROUP BY parent,key HAVING COUNT(*)>1) THEN 1
                WHEN COALESCE(json_extract(status_json,'$.status'),'') NOT IN
                    ('completed','failed','cancelled','interrupted') THEN 1 ELSE 0 END=1 LIMIT 1""").fetchone()
            require(replay is None, 'admission_replay_unsettled')
            require(bundle.responses._conn.execute(
                'SELECT 1 FROM response_keys WHERE settled_at IS NULL LIMIT 1').fetchone() is None,
                'admission_response_unsettled')
    finally:
        lock.release()


def _status(binding):
    return {'state': binding._stage, 'localAdmissionOpen': binding._stage == 'ACTIVE',
            'serviceStarted': False, 'listenerAuthorized': False,
            'callerStoresClosed': False, 'epochAdvanced': False,
            'providerAuthorized': False, 'fullHarnessAcceptance': False}


def activate(bundle):
    from gateway.platforms.api_server_prepared import check_prepared
    binding, runner, adapter, loop = _pair(bundle)
    require(binding._stage == 'PREPARED', 'admission_not_fresh_prepared')
    check_prepared(bundle, adapter)
    _idle_work(binding, runner, adapter)
    _idle_ledgers(bundle, binding)
    # No await and no callback after this point. SQL/native source gates remain
    # authoritative; this local phase change never creates a persistent lease.
    try:
        binding._admission_loop = loop
        binding._stage = 'ACTIVATING'
        binding._descriptor['state'] = 'caller_active'
        binding._authority.retiring = False
        runner._draining = False
        binding._stage = 'ACTIVE'
        check_prepared(bundle, adapter)
    except BaseException:
        binding._stage = 'ADMISSION_UNKNOWN'
        binding._authority.retiring = True
        runner._draining = True
        raise
    return _status(binding)


def begin_drain(bundle):
    binding, runner, adapter, loop = _pair(bundle)
    require(binding._stage in ('ACTIVE', 'DRAINING'), 'admission_not_active_or_draining')
    binding._check_prepared(runner)
    if binding._stage == 'ACTIVE':
        # The stage closes the transaction gate first. This does not take an SQL
        # lock, await, cancel workers or deny already admitted settlement writes.
        binding._stage = 'DRAINING'
        runner._draining = True
        binding._authority.retiring = True
        binding._descriptor['state'] = 'caller_draining'
    binding._check_prepared(runner)
    return _status(binding)


def finish_drain(bundle):
    from gateway.platforms.api_server_prepared import check_prepared
    binding, runner, adapter, loop = _pair(bundle)
    require(binding._stage in ('DRAINING', 'DRAINED'), 'admission_drain_not_started')
    check_prepared(bundle, adapter)
    _idle_work(binding, runner, adapter)
    _idle_ledgers(bundle, binding)
    binding._stage = 'DRAINED'
    binding._descriptor['state'] = 'caller_drained'
    return _status(binding)


def assert_drained(bundle):
    """Fresh release barrier, also after API cleanup and before runner detach.

    Synchronous runner detach is allowed on its original owner thread after the
    async adapter detach. No new admission can reopen DRAINED. Keep every handle
    when anything changed since finish_drain; this function never repairs it.
    """
    from gateway.platforms.api_server_prepared import check_prepared
    binding = bundle._prepared_binding
    binding._lifecycle_actor()
    require(binding._stage == 'DRAINED' and binding._api_bundle is bundle
            and bundle._stage in ('ATTACHED', 'DRAINING', 'DETACHED'),
            'admission_not_drained')
    check_prepared(bundle, bundle._adapter)
    _idle_work(binding, bundle._prepared_runner, bundle._adapter)
    _idle_ledgers(bundle, binding)


def require_open(binding, authority):
    """Pure gate, including the existing off-loop SQL admission/claim callback.

    Never acquire a mutex, open a file or query SQLite while its transaction is
    calling this guard. The original native write still checks its durable epoch.
    """
    from hermes_state_runtime import RuntimeStoreError
    from gateway.run_borrowed_state import BorrowedRunnerRejected, CallerOwnedRunnerState
    from hermes_constants import get_hermes_home
    try:
        require(type(binding) is CallerOwnedRunnerState and binding._stage == 'ACTIVE'
                and os.getpid() == binding._actor[0] and binding._admission_loop is not None
                and not binding._admission_loop.is_closed(),
                'admission_closed')
        runner, bundle = binding._runner, binding._api_bundle
        require(bundle is not None and bundle._stage == 'ATTACHED'
                and bundle._prepared_binding is binding and bundle._prepared_runner is runner
                and runner._caller_owned_state is binding and authority is binding._authority
                and authority.db is bundle.state and binding.state is bundle.state,
                'admission_owner_changed')
        require(runner.config is binding.config and binding.config.multiplex_profiles is False
                and binding.config.write_sessions_json is False and not binding.config.platforms
                and not binding.config.profile_routes and Path(get_hermes_home()) == binding.home
                and runner.session_store is binding._facade
                and binding._facade.config is binding.config
                and binding._facade._borrowed_session_state._state is binding.state,
                'admission_profile_or_configuration_changed')
        require(bundle.state.read_only is False and bundle.state._shared_registry_owned is False
                and bundle.replay.durable is True and bundle.state._lock is bundle._state_lock
                and bundle.replay._lock is bundle._prepared_replay_lock
                and bundle.responses._canonical_state is bundle._canonical
                and bundle._canonical._state is bundle.state
                and bundle._canonical._connection is bundle._connections[0]
                and bundle._canonical._lock is bundle._state_lock,
                'admission_connection_owner_changed')
        # Reading this attribute raises on a closed native handle; no SQL or lock.
        tuple(c.in_transaction for c in bundle._connections)
        require(not bundle._connections[1].in_transaction and not bundle._connections[2].in_transaction,
                'admission_other_store_transaction')
        adapter = bundle._adapter
        require(adapter is not None and adapter.gateway_runner is runner
                and adapter._borrowed_api_stores is bundle and adapter._session_db is bundle.state
                and adapter._run_idempotency_store is bundle.replay
                and adapter._response_store is bundle.responses
                and all(s._conn is c for s,c in zip(bundle._stores,bundle._connections)),
                'admission_stores_changed')
        binding._check_prepared(runner)
    except (BorrowedStoreRejected, BorrowedRunnerRejected, sqlite3.Error, AttributeError, TypeError, KeyError, ValueError):
        raise RuntimeStoreError('caller_admission_unavailable') from None
