"""Prepared-only join of existing runner and borrowed API stores.

The existing two owners remain authoritative. No new connection, worker, epoch,
service permission or caller close is introduced. This local object-identity
contract requires cooperative caller serialization; it is not a security boundary
against arbitrary in-process mutation or a cross-database atomic snapshot.
"""
from pathlib import Path
import sqlite3
import stat
from gateway.platforms.api_server_borrowed import BorrowedStoreRejected, require


def _storage_snapshot(bundle):
    from gateway.platforms.api_server_response_state import CanonicalStateUnavailable
    bundle._check()
    lock = bundle.replay._lock
    require(lock is bundle._prepared_replay_lock, 'prepared_replay_lock_changed')
    require(lock.acquire(blocking=False), 'prepared_replay_busy')
    try:
        with bundle._canonical.read():
            result = []
            paths = (bundle.home/'state.db', bundle.home/'runs_idempotency.db',
                     bundle.home/'response_store.db')
            declared = (bundle.state.db_path, bundle.replay._db_path, bundle.responses._db_path)
            for conn, path, name in zip(bundle._connections, paths, declared):
                info = path.lstat()
                require(Path(name) == path and not conn.in_transaction,
                        'prepared_path_or_transaction_changed')
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                        and path.resolve(strict=True) == path, 'prepared_file_identity')
                rows = [tuple(r) for r in conn.execute('PRAGMA database_list')]
                require(sum(r[1] == 'main' and r[2] and Path(r[2]) == path for r in rows) == 1
                        and all(r[1] == 'main' or (r[1] == 'temp' and r[2] == '') for r in rows),
                        'prepared_database_path_changed')
                app = conn.execute('PRAGMA main.application_id').fetchone()[0]
                result.append((info.st_dev, info.st_ino, app))
            return tuple(result)
    except (CanonicalStateUnavailable, sqlite3.Error, OSError, TypeError, AttributeError, ValueError):
        raise BorrowedStoreRejected('prepared_storage_unavailable') from None
    finally:
        lock.release()


def _runner_check(bundle):
    from gateway.run_borrowed_state import BorrowedRunnerRejected
    runner, binding = bundle._prepared_runner, bundle._prepared_binding
    require(runner is not None and binding is not None, 'prepared_runner_missing')
    try:
        require(runner._caller_owned_state is binding and binding._api_bundle is bundle
                and binding.state is bundle.state and binding.home == bundle.home
                and binding._stage == 'PREPARED', 'prepared_runner_binding_changed')
        require(binding.select_async(runner)._db is bundle.state, 'prepared_runner_state_changed')
    except (BorrowedRunnerRejected, AttributeError, KeyError, TypeError):
        raise BorrowedStoreRejected('prepared_runner_unavailable') from None


def bind_prepared_runner(bundle, runner):
    from gateway.run import GatewayRunner
    from gateway.run_borrowed_state import CallerOwnedRunnerState, BorrowedRunnerRejected
    require(type(runner) is GatewayRunner, 'prepared_native_runner_required')
    binding = runner._caller_owned_state
    require(type(binding) is CallerOwnedRunnerState and binding._api_bundle is None,
            'prepared_runner_already_bound_or_unowned')
    try:
        require(binding.select_async(runner)._db is bundle.state and binding._stage == 'PREPARED'
                and binding.home == bundle.home, 'prepared_same_state_home_required')
        binding._check_state(quiescent=True)
    except BorrowedRunnerRejected:
        raise BorrowedStoreRejected('prepared_runner_not_ready') from None
    bundle._prepared_replay_lock = bundle.replay._lock
    snapshot = _storage_snapshot(bundle)
    bundle._prepared_runner, bundle._prepared_binding = runner, binding
    bundle._prepared_files = snapshot
    binding._api_bundle = bundle  # One-shot pair; caller retains an unclaimed/failed construction.


def check_prepared(bundle, adapter=None):
    _runner_check(bundle)
    require(_storage_snapshot(bundle) == bundle._prepared_files, 'prepared_store_generation_changed')
    if adapter is not None:
        require(bundle._adapter is adapter and bundle._prepared_adapter_ready
                and adapter.gateway_runner is bundle._prepared_runner
                and adapter._borrowed_api_stores is bundle, 'prepared_adapter_changed')
        from gateway.platforms.api_server_borrowed import _quiescent
        require(adapter._running is False and adapter._app is None
                and adapter._site is None and adapter._runner is None,
                'prepared_api_service_state_changed')
        _quiescent(adapter)


def finish_adapter(bundle, adapter):
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.platforms.api_server_borrowed import _quiescent
    require(type(adapter) is APIServerAdapter and bundle._stage == 'ATTACHED'
            and bundle._adapter is adapter and not bundle._prepared_adapter_ready,
            'prepared_adapter_construction')
    require(adapter.gateway_runner is None and adapter._session_db is bundle.state
            and adapter._response_store is bundle.responses
            and adapter._run_idempotency_store is bundle.replay
            and not adapter._session_dbs and not adapter._response_stores,
            'prepared_adapter_stores')
    check_prepared(bundle)
    _quiescent(adapter)
    require(adapter._app is None and adapter._site is None and adapter._runner is None
            and not adapter._running, 'prepared_listener_or_service_present')
    adapter.gateway_runner = bundle._prepared_runner
    bundle._prepared_adapter_ready = True


def require_adapter_detached(bundle, runner, binding):
    from gateway.platforms.api_server_borrowed import _quiescent
    require(bundle._prepared_runner is runner and bundle._prepared_binding is binding
            and bundle._stage == 'DETACHED', 'prepared_api_must_detach_first')
    adapter = bundle._adapter
    check_prepared(bundle, adapter)
    require(adapter._session_db_cache_closed is True and adapter._app is None
            and adapter._site is None and adapter._runner is None and not adapter._running,
            'prepared_adapter_cleanup_incomplete')
    _quiescent(adapter)
