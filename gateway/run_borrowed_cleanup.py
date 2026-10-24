"""Explicit retirement of idle caller-owned finite execution resources.

Admission must already be sealed, native dispatchers/physical work/SQL settled.
Only an idle native turn executor and idle API-owned cache entries are retired.
No queued work cancellation, registry sweep, DB close, or best-effort success.
Agent close is awaited off-loop; cancellation/partial effects retain UNKNOWN.
"""
import asyncio
from collections import OrderedDict
from gateway.platforms.api_server_borrowed import require


def runtime_plan(binding, runner):
    from gateway.turn_executor import _UnboundedThreadExecutor
    from gateway.session_state import SessionState, TurnState
    authority = binding._authority
    require(type(runner._sessions) is dict and type(runner._agent_cache) is OrderedDict,
            'runtime_cleanup_registry_type')
    routes = {live.route: sid for sid, live in authority.sessions.items()}
    pool = runner._executor
    if pool is not None:
        require(type(pool) is _UnboundedThreadExecutor, 'runtime_cleanup_foreign_executor')
        require(pool._lock.acquire(blocking=False), 'runtime_cleanup_executor_busy')
        try:
            require(type(pool._threads) is set and not pool._threads,
                    'runtime_cleanup_physical_worker_live')
        finally:
            pool._lock.release()
    states = tuple(runner._sessions.items())
    for key, state in states:
        require(type(state) is SessionState and (key in routes or key == '*'),
                'runtime_cleanup_foreign_session')
        require(state.turn == TurnState() and not state.conversation.queued_events
                and not state.conversation.sidecar_notes and state.conversation.one_turn_restore is None
                and state.persistent.approvals is None and not state.persistent.update_prompt_pending
                and not state.persistent.native_image_paths and not state.persistent.pending_command_text,
                'runtime_cleanup_session_obligation')
    entries = tuple(runner._agent_cache.items())
    agents = []
    for key, entry in entries:
        require(key in routes and type(entry) is tuple and len(entry) >= 1,
                'runtime_cleanup_foreign_agent')
        agent = entry[0]
        require(getattr(agent, 'session_id', None) == routes[key]
                and callable(getattr(agent, 'close', None))
                and getattr(agent, '_memory_manager', None) is None,
                'runtime_cleanup_agent_lifecycle_unqualified')
        require(all(agent is not old for old in agents), 'runtime_cleanup_agent_alias')
        agents.append(agent)
    return pool, states, entries, tuple(agents)


def _same_plan(binding, runner, plan):
    pool, states, entries, agents = plan
    require(runner._executor is pool, 'runtime_cleanup_executor_replaced')
    require(len(runner._sessions) == len(states)
            and all(runner._sessions.get(key) is value for key, value in states),
            'runtime_cleanup_session_replaced')
    require(len(runner._agent_cache) == len(entries)
            and all(runner._agent_cache.get(key) is value for key, value in entries),
            'runtime_cleanup_cache_replaced')
    runtime_plan(binding, runner)


def _close_agents(agents):
    for agent in agents:
        require(agent.close() is None, 'runtime_cleanup_agent_close_unconfirmed')


def _sessions_idle(binding, loop):
    from gateway.run_borrowed_retirement import _entry, _durable, _take, _empty
    for name in ('api_observers', 'waiter_replies'):
        _empty(binding._authority, name, dict, optional=True)
    acquired = []
    try:
        plan = tuple(_entry(binding._authority, binding._facade, sid, live, loop, acquired)
                     for sid, live in binding._authority.sessions.items())
        _take(binding.state._lock, acquired)
        _durable(binding._authority, plan)
    finally:
        for lock in reversed(acquired):lock.release()


async def retire_idle_runtime(bundle):
    from gateway.run_borrowed_admission import _pair, _idle_work, _idle_ledgers
    from gateway.platforms.api_server_prepared import check_prepared
    binding, runner, adapter, loop = _pair(bundle)
    require(binding._stage == 'DRAINING', 'runtime_cleanup_requires_drain')
    require(getattr(binding, '_runtime_cleanup_state', 'NEW') in ('NEW', 'RETIRED'),
            'runtime_cleanup_outcome_unknown')
    check_prepared(bundle, adapter)
    _idle_work(binding, runner, adapter, allow_idle_sessions=True, runtime_cleanup=True)
    _idle_ledgers(bundle, binding)
    plan = runtime_plan(binding, runner)
    _sessions_idle(binding, loop)
    started = False
    try:
        require(runner._executor_lock.acquire(blocking=False), 'runtime_cleanup_pool_owner_busy')
        try:
            _same_plan(binding, runner, plan)
            started = True  # Everything below may have observable partial effects.
            binding._runtime_cleanup_state = 'CLOSING'
            binding._runtime_cleanup_plan = plan
            runner._executor_closing = True
            binding._stage = 'RETIRING_RUNTIME'
        finally:
            runner._executor_lock.release()
        pool, states, entries, agents = plan
        if pool is not None:
            # Existing native executor owns its shutdown and submission gate.
            pool.shutdown(wait=False)
            require(not pool._threads, 'runtime_cleanup_worker_started')
        task = loop.create_task(asyncio.to_thread(_close_agents, agents))
        binding._runtime_cleanup_task = task
        await asyncio.shield(task)
        _same_plan(binding, runner, plan)
        require(binding._stage == 'RETIRING_RUNTIME' and runner._executor_closing is True,
                'runtime_cleanup_phase_changed')
        # Native identity checks require the sealed known phase; no await follows.
        binding._stage = 'DRAINING'
        binding._runtime_cleanup_state = 'RETIRED'  # Tentative, rechecked before releasing maps.
        check_prepared(bundle, adapter)
        _idle_work(binding, runner, adapter, allow_idle_sessions=True, runtime_cleanup=True)
        _idle_ledgers(bundle, binding)
        _sessions_idle(binding, loop)
        for key, entry in entries:
            require(runner._agent_cache.get(key) is entry, 'runtime_cleanup_cache_changed')
            del runner._agent_cache[key]
        for key, state in states:
            require(runner._sessions.get(key) is state, 'runtime_cleanup_session_changed')
            del runner._sessions[key]
        require(runner._executor is pool, 'runtime_cleanup_executor_changed')
        runner._executor = None
        binding._runtime_cleanup_state = 'RETIRED'
        binding._runtime_cleanup_plan = None
        binding._runtime_cleanup_task = None
    except BaseException:
        if started:
            binding._runtime_cleanup_state = 'UNKNOWN'
            binding._stage = 'ADMISSION_UNKNOWN'
        raise
    return {'state': 'IDLE_RUNTIME_RETIRED', 'agentCount': len(agents),
            'runnerSessionCount': len(states), 'callerStoresClosed': False,
            'durableDataChanged': False, 'profileReleased': False,
            'nativeSessionRetirementRequired': True}
