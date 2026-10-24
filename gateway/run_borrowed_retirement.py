"""Explicit memory-only retirement for the original caller-owned API authority.

Not deletion, FIFO execution, recovery, agent eviction or profile release. The
existing native owners must first settle work and release workers/caches. A
completed dispatcher is never used as proof that its physical workers ended.
This cooperative owner-loop operation has no await and retains all connections,
rows, routing metadata and transcript history. Other surfaces/compression chains
remain unqualified and are retained rather than guessed safe.
"""
import asyncio
import json
import threading
import sqlite3
from _thread import LockType
from gateway.platforms.api_server_borrowed import BorrowedStoreRejected, require


def _empty(owner, name, kind, *, optional=False):
    value = getattr(owner, name, None)
    require(optional and value is None or type(value) is kind and not value,
            'retirement_live_obligation')


def _take(lock, acquired):
    require(type(lock) in (LockType, type(threading.RLock())), 'retirement_lock_type')
    require(all(lock is not held for held in acquired), 'retirement_lock_alias')
    require(lock.acquire(blocking=False), 'retirement_busy')
    acquired.append(lock)


def _entry(authority, store, sid, live, loop, acquired):
    from gateway.config import Platform
    from gateway.session import SessionEntry, SessionSource
    from gateway.session_authority import LiveSession
    from gateway.session_events import SessionEvents
    from gateway.session_pending_controls import PendingControls
    from tui_gateway.transport import FanoutTransport
    require(type(sid) is str and type(live) is LiveSession
            and type(live.source) is SessionSource and live.source.platform is Platform.API_SERVER
            and live.source.chat_id == sid and type(live.route) is str,
            'retirement_api_session_required')
    task = live.task
    require(task is None or type(task) is asyncio.Task and task.get_loop() is loop
            and task.done() and not task.cancelled() and task.exception() is None
            and task.result() is None, 'retirement_dispatcher_unsettled')
    require(type(live.mutation_lock) is asyncio.Lock and type(live.claim_gate) is asyncio.Lock
            and not live.mutation_lock.locked() and not live.claim_gate.locked(),
            'retirement_session_lock_held')
    _empty(live, 'subscribers', dict)
    _empty(live, 'settling', dict)
    events, controls = live.event_stream, live.controls
    require(type(events) is SessionEvents and type(controls) is PendingControls
            and controls.events is events and type(events.fanout) is FanoutTransport,
            'retirement_event_identity')
    _take(events.lock, acquired)
    _take(events.fanout._lock, acquired)
    _empty(events, 'execution', dict)
    _empty(events, 'observers', set)
    _empty(events.fanout, '_peers', list)  # Includes detached writers, not only subscribers.
    _empty(controls, 'pending', dict)
    _empty(controls, 'remote_responders', dict)
    entry = store._entries.get(live.route)
    require(type(entry) is SessionEntry and entry.session_id == sid
            and entry.session_key == live.route, 'retirement_route_cache_changed')
    require(entry.active_turn_token is None and entry.active_turn_started_at is None,
            'retirement_turn_marker_retained')
    require(type(entry.origin) is SessionSource and entry.origin.to_dict() == live.source.to_dict(),
            'retirement_live_source_changed')
    hint = store._session_owner_hints.get(sid)
    require(hint is None or hint == live.route, 'retirement_owner_hint_changed')
    return sid, live, entry


def _durable(authority, plan):
    from hermes_state_local import API_BINDING_PREFIX
    conn = authority.db._conn
    require(not conn.in_transaction, 'retirement_transaction_active')
    for table in ('session_admissions', 'worker_executions'):
        require(conn.execute('SELECT 1 FROM main.' + table +
                " WHERE status IS NULL OR status!='terminal' LIMIT 1").fetchone() is None,
                'retirement_unresolved_work')
    for sid, live, entry in plan:
        row = conn.execute('SELECT source,session_key,chat_id,user_id FROM main.sessions WHERE id=?',
                           (sid,)).fetchone()
        require(row is not None and tuple(row) == ('api_server', live.route, sid, live.source.user_id),
                'retirement_durable_session_changed')
        saved = conn.execute('SELECT value FROM main.state_meta WHERE key=?',
                             (API_BINDING_PREFIX + sid,)).fetchone()
        require(saved is not None, 'retirement_binding_missing')
        binding = json.loads(saved[0])
        require(binding['profile_id'] == authority.profile_id and binding['session_id'] == sid
                and binding['route'] == live.route and binding['entry']['session_id'] == sid,
                'retirement_binding_changed')
        require(binding['entry'].get('origin') == live.source.to_dict(),
                'retirement_binding_source_changed')
        route = conn.execute("SELECT entry_json FROM main.gateway_routing WHERE scope='' AND session_key=?",
                             (live.route,)).fetchone()
        require(route is not None, 'retirement_durable_route_changed')
        saved_entry = json.loads(route[0])
        require(saved_entry['session_id'] == sid
                and saved_entry.get('origin') == live.source.to_dict(),
                'retirement_durable_route_changed')
        require(saved_entry.get('active_turn_token') is None
                and saved_entry.get('active_turn_started_at') is None,
                'retirement_durable_turn_marker')


def _retire_idle_api_sessions(bundle):
    from gateway.run_borrowed_admission import _pair, _idle_work, _idle_ledgers
    from gateway.platforms.api_server_prepared import check_prepared
    binding, runner, adapter, loop = _pair(bundle)
    require(binding._stage == 'DRAINING', 'retirement_requires_sealed_admission')
    check_prepared(bundle, adapter)
    _idle_work(binding, runner, adapter, allow_idle_sessions=True)
    _idle_ledgers(bundle, binding)
    authority, store = binding._authority, binding._facade
    for name in ('api_observers', 'waiter_replies'):
        _empty(authority, name, dict, optional=True)
    require(type(store._entries) is dict and type(store._session_owner_hints) is dict,
            'retirement_facade_maps')
    acquired = []
    try:
        for name in ('_save_lock', '_lock', '_inflight_lock', '_transcript_retry_lock',
                     '_transcript_drain_lock'):
            _take(getattr(store, name), acquired)
        for name in ('_inflight_sessions', '_db_handles', '_fast_persisted_entries'):
            _empty(store, name, dict)
        require(type(store._dirty_transcripts) is dict
                and all(type(v) is list and not v for v in store._dirty_transcripts.values()),
                'retirement_dirty_transcripts')
        require(store._routing_generation == store._persisted_routing_generation,
                'retirement_routing_not_persisted')
        pairs = tuple(authority.sessions.items())
        require(len({id(v) for _, v in pairs}) == len(pairs)
                and len({v.route for _, v in pairs}) == len(pairs), 'retirement_alias')
        plan = tuple(_entry(authority, store, sid, live, loop, acquired) for sid, live in pairs)
        _take(authority.db._lock, acquired)
        _durable(authority, plan)
        # All checks precede effects. No callback, SQL write or await below.
        binding._stage = 'RETIRING_SESSIONS'
        try:
            for sid, live, entry in plan:
                require(authority.sessions.get(sid) is live and store._entries.get(live.route) is entry,
                        'retirement_memory_changed')
                del authority.sessions[sid]
                del store._entries[live.route]
                store._session_owner_hints.pop(sid, None)
            binding._stage = 'DRAINING'
        except BaseException:
            binding._stage = 'ADMISSION_UNKNOWN'
            raise
    finally:
        for lock in reversed(acquired):
            lock.release()
    return {'state': 'IDLE_API_SESSIONS_RETIRED', 'retiredCount': len(plan),
            'durableDataChanged': False, 'callerStoresClosed': False,
            'profileReleased': False, 'queuedWorkExecuted': False,
            'drainFinished': False, 'runtimeAdmissionAuthorized': False}


def retire_idle_api_sessions(bundle):
    """Bound observation failures; never turn a partial effect into retry authority."""
    try:
        return _retire_idle_api_sessions(bundle)
    except (sqlite3.Error, AttributeError, KeyError, TypeError, ValueError):
        raise BorrowedStoreRejected('retirement_observation_unavailable') from None
