"""Optional fixed-home SessionStore binding to caller-owned native state.

This reference does not acquire/open/repair/close a SessionDB or grant runtime
admission. The caller retains preflight, writer quiescence and profile lifetime.
Default multiplexed SessionStore behavior is unchanged when this is not supplied.
"""
from pathlib import Path
import os
import sqlite3
import stat


class BorrowedSessionRejected(RuntimeError):
    """Bounded error, with no SQL, payload or private filesystem path."""


def require(ok, reason):
    if not ok:
        raise BorrowedSessionRejected(reason)


class CallerOwnedSessionState:
    def __init__(self, sessions_dir, config, state):
        from hermes_state import SessionDB
        from hermes_cli.sqlite_safe_read import TrackedConnection
        from gateway.runtime_ownership import process_ownership, canonical_home
        require(type(state) is SessionDB and state.read_only is False
                and state._shared_registry_owned is False, 'session_borrow_native_owner')
        require(getattr(config, 'multiplex_profiles', False) is False
                and getattr(config, 'write_sessions_json', True) is False,
                'session_borrow_single_home_sqlite_only')
        self._state, self._conn, self._lock = state, state._conn, state._lock
        require(type(self._conn) is TrackedConnection, 'session_borrow_connection_type')
        self._path = Path(state.db_path)
        require(self._path.is_absolute() and self._path.name == 'state.db'
                and self._path == self._path.resolve(strict=True), 'session_borrow_state_path')
        self.home = self._path.parent
        require(Path(sessions_dir) == self.home / 'sessions'
                and Path(sessions_dir).resolve() == Path(sessions_dir), 'session_borrow_sessions_path')
        self._config = config
        self._identity = state._db_file_identity
        self._app_id = state._db_file_application_id
        self._pid = os.getpid()
        self._closed = False
        self._store = None
        self._ownership = process_ownership
        self._home_key = canonical_home(self.home)
        with process_ownership._mutex:
            self._profile_handle = process_ownership._handles.get(self._home_key)
        require(self._profile_handle is not None, 'session_borrow_profile_not_owned')
        self.select()

    def attach(self, store):
        require(self._store is None, 'session_borrow_already_attached')
        self._store = store
        self.select()

    def _identity_check(self):
        from hermes_constants import get_hermes_home
        require(not self._closed and os.getpid() == self._pid, 'session_borrow_detached_or_actor')
        require(Path(get_hermes_home()) == self.home, 'session_borrow_profile_changed')
        if self._store is not None:
            require(self._store._borrowed_session_state is self
                    and self._store.config is self._config
                    and Path(self._store.sessions_dir) == self.home / 'sessions'
                    and self._store._write_sessions_json is False,
                    'session_borrow_facade_changed')
        require(self._state._conn is self._conn and self._state._lock is self._lock
                and Path(self._state.db_path) == self._path
                and self._state._db_file_identity == self._identity
                and self._state.read_only is False and self._state._shared_registry_owned is False,
                'session_borrow_state_changed')
        require(getattr(self._config, 'multiplex_profiles', False) is False
                and getattr(self._config, 'write_sessions_json', True) is False,
                'session_borrow_configuration_changed')
        with self._ownership._mutex:
            require(self._ownership._handles.get(self._home_key) is self._profile_handle,
                    'session_borrow_profile_owner_changed')
        info = self._path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                and (info.st_dev, info.st_ino) == self._identity, 'session_borrow_file_changed')

    def select(self, path=None, key=None):
        try:
            self._identity_check()
            require(path is None or Path(path) == self._path, 'session_borrow_foreign_path')
            require(key is None or (type(key) is str and key.startswith('agent:main:')
                    and len(key) > len('agent:main:')), 'session_borrow_foreign_key')
            require(self._lock.acquire(blocking=False), 'session_borrow_state_busy')
            try:
                require(not self._conn.in_transaction, 'session_borrow_transaction_active')
                rows = [tuple(row) for row in self._conn.execute('PRAGMA database_list')]
                require(sum(row[1] == 'main' and row[2] and Path(row[2]) == self._path for row in rows) == 1
                        and all(row[1] == 'main' or (row[1] == 'temp' and row[2] == '') for row in rows),
                        'session_borrow_database_changed')
                require(self._conn.execute('PRAGMA main.application_id').fetchone()[0] == self._app_id,
                        'session_borrow_generation_changed')
            finally:
                self._lock.release()
            return self._state
        except BorrowedSessionRejected:
            raise
        except (OSError, sqlite3.Error, ValueError, TypeError, AttributeError):
            raise BorrowedSessionRejected('session_borrow_unavailable') from None

    def detach(self, store):
        if self._closed:
            require(os.getpid() == self._pid, 'session_borrow_detached_or_actor')
            return
        self.select()
        require(not store._db_handles and not store._inflight_sessions
                and not any(store._dirty_transcripts.values()), 'session_borrow_facade_busy')
        # Only this facade becomes unusable; the caller state and profile stay owned.
        self._closed = True

    def load_routing(self, store):
        self.select()
        if store._loaded:
            return
        require(store._load_routing_rows_locked(), 'session_borrow_routing_unavailable')
        store._loaded = True
        store._routing_db_loaded = True
        store._routing_fallback_baseline = None
