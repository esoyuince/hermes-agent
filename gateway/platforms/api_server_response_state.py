"""Read canonical response settlement from a caller-owned live SessionDB.

This optional binding does not open, acquire, repair or close a canonical store.
The caller owns its profile lease and verified SessionDB lifecycle. The binding
is process/thread-local and deliberately rejects a busy or replaced connection;
it is not admission authority or a replacement for startup qualification.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import stat
import threading


class CanonicalStateUnavailable(RuntimeError):
    """Bounded diagnostic; no SQL, response content or private paths."""


def _require(ok, reason):
    if not ok:
        raise CanonicalStateUnavailable(reason)


class BorrowedResponseState:
    def __init__(self, state, response_path):
        from hermes_state import SessionDB
        from hermes_cli.sqlite_safe_read import TrackedConnection
        _require(type(state) is SessionDB, 'response_canonical_native_state')
        _require(response_path is not None and str(response_path) != ':memory:',
                 'response_canonical_disk_required')
        self._state = state
        self._connection = state._conn
        self._lock = state._lock
        self._actor = (os.getpid(), threading.get_ident())
        _require(type(self._connection) is TrackedConnection and state.read_only is False,
                 'response_canonical_live_connection')
        try:
            self._path = Path(state.db_path).resolve(strict=True)
            response = Path(response_path)
            _require(response.is_absolute() and response.name == 'response_store.db'
                     and response.parent.resolve(strict=True) == self._path.parent
                     and response == response.resolve() and not response.is_symlink(),
                     'response_canonical_profile_mismatch')
            self._identity = state._db_file_identity
            self._application_id = state._db_file_application_id
            with self.read():
                pass
        except CanonicalStateUnavailable:
            raise
        except (OSError, ValueError, TypeError, sqlite3.Error):
            raise CanonicalStateUnavailable('response_canonical_unavailable') from None

    def _check(self):
        _require((os.getpid(), threading.get_ident()) == self._actor,
                 'response_canonical_other_actor')
        _require(self._state._conn is self._connection and self._state._lock is self._lock
                 and Path(self._state.db_path) == self._path
                 and self._state._db_file_identity == self._identity,
                 'response_canonical_connection_changed')
        _require(not self._connection.in_transaction, 'response_canonical_transaction_active')
        info = self._path.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                 and (info.st_dev, info.st_ino) == self._identity,
                 'response_canonical_file_changed')
        attached = [tuple(r) for r in self._connection.execute('PRAGMA database_list')]
        _require(sum(r[1] == 'main' for r in attached) == 1
                 and all((r[1] == 'main' and bool(r[2]) and Path(r[2]) == self._path)
                         or (r[1] == 'temp' and r[2] == '') for r in attached),
                 'response_canonical_wrong_database')
        _require(self._connection.execute('PRAGMA main.application_id').fetchone()[0]
                 == self._application_id, 'response_canonical_generation_changed')

    @contextmanager
    def read(self):
        _require((os.getpid(), threading.get_ident()) == self._actor,
                 'response_canonical_other_actor')
        _require(self._lock.acquire(blocking=False), 'response_canonical_busy')
        try:
            self._check()
            changes = self._connection.total_changes
            version = self._connection.execute('PRAGMA main.data_version').fetchone()[0]
            yield self._connection
            self._check()
            _require(self._connection.total_changes == changes
                     and self._connection.execute('PRAGMA main.data_version').fetchone()[0] == version,
                     'response_canonical_changed_during_read')
        except CanonicalStateUnavailable:
            raise
        except (OSError, sqlite3.Error, ValueError, TypeError, AttributeError):
            raise CanonicalStateUnavailable('response_canonical_unavailable') from None
        finally:
            self._lock.release()
