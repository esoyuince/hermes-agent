"""Turn-scoped state borrowing for goals; no registry/cache ownership transfer.

Copied execution contexts share a revocable lifetime marker. Expired or invalid
borrows fail without falling back to a new connection. Ordinary callers keep the
existing registry path when no caller-owned execution scope is installed.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Callable

@dataclass
class _Borrow:
    resolve: Callable[[], object]
    active: bool = True

_current: ContextVar[_Borrow | None] = ContextVar('goals_borrowed_state', default=None)

def current_session_db():
    borrow = _current.get()
    if borrow is None:
        return None
    if not borrow.active:
        raise RuntimeError('goals_borrowed_scope_expired')
    state = borrow.resolve()
    if state is None:
        raise RuntimeError('goals_borrowed_state_unavailable')
    return state

@contextmanager
def session_db_scope(resolve):
    if not callable(resolve):
        raise TypeError('goals_borrowed_resolver_required')
    borrow = _Borrow(resolve)
    token = _current.set(borrow)
    try:
        yield
    finally:
        borrow.active = False
        _current.reset(token)
