"""One-shot loopback HTTP on the original caller-owned prepared API pair.

This owns transport resources and handler lifetimes, never the profile, epoch or
SQLite connections. Ordinary adapter/runner startup remains unqualified. The
embedding host installs its admission boundary before the router is frozen; raw
run submission, multiplex routes and background service tasks are not mounted.
All checks are cooperative object-identity contracts, not protection from hostile
Python code in the same process.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import os
import socket
import sqlite3
import threading
from types import MappingProxyType

from gateway.platforms.api_server_borrowed import BorrowedStoreRejected, require


@dataclass(slots=True)
class _Handler:
    outer: asyncio.Task
    inner: asyncio.Task
    request: object
    outer_done: bool = False
    inner_done: bool = False


def _same(values, expected):
    return len(values) == len(expected) and all(a is b for a, b in zip(values, expected))


def _prepared_profile_scopes(home):
    from agent.secret_scope import current_secret_scope, current_secret_scope_home
    from tools.terminal_scope import build_profile_terminal_scope, TerminalPolicyUnavailable
    secrets = current_secret_scope()
    if secrets is not None:
        require(current_secret_scope_home() == str(home), 'borrowed_transport_foreign_secret_scope')
        secrets = dict(secrets)
    else:
        secrets = {}
    try:
        terminal = build_profile_terminal_scope(home)
    except TerminalPolicyUnavailable:
        raise BorrowedStoreRejected('borrowed_transport_terminal_policy_unavailable') from None
    return MappingProxyType(secrets), MappingProxyType(terminal)


class BorrowedHTTPTransport:
    """Explicit local opt-in; a failed effect is retained UNKNOWN, never retried.

    ``configure_app(app, adapter, transport)`` is synchronous and mounts the real
    embedding host's admission/control boundary. It must not add lifecycle hooks
    or start work. ``check_binding`` is the pure guard for native worker threads;
    ``check`` additionally verifies the owner event loop and prepared stores.
    """

    def __init__(self, bundle, *, configure_app):
        from gateway.run_borrowed_admission import _pair
        from gateway.platforms.api_server_prepared import check_prepared

        binding, runner, adapter, loop = _pair(bundle)
        require(binding._stage in ('PREPARED', 'ACTIVE'), 'borrowed_transport_phase')
        require(bundle._publication_state == 'PUBLISHED', 'borrowed_transport_unpublished')
        require(bundle._borrowed_http_transport is None, 'borrowed_transport_already_owned')
        require(loop.get_task_factory() is None, 'borrowed_transport_task_factory')
        require(callable(configure_app), 'borrowed_transport_configurator')
        check_prepared(bundle, adapter)
        require(adapter._host == '127.0.0.1', 'borrowed_transport_loopback_required')
        require(type(adapter._port) is int and 0 <= adapter._port <= 65535,
                'borrowed_transport_port')
        extra = adapter.config.extra
        require(type(extra) is dict and type(extra.get('key')) is str
                and extra['key'] == adapter._api_key, 'borrowed_transport_explicit_key')
        require(adapter._api_key_passes_startup_guard(), 'borrowed_transport_key_invalid')
        self._secret_scope, self._terminal_scope = _prepared_profile_scopes(bundle.home)
        self._bundle, self._binding, self._runner, self._adapter = bundle, binding, runner, adapter
        self._authority, self._loop = binding._authority, loop
        self._actor = (os.getpid(), threading.get_ident())
        self._stores, self._connections = bundle._stores, bundle._connections
        self._state_owners = (binding._facade, binding._wrapper, binding._gate,
                              binding._facade._borrowed_session_state)
        self._profile_handle = binding._gate._profile_handle
        self._epoch = (binding.epoch, binding.instance_id)
        self._home, self._config = bundle.home, adapter.config
        self._host, self._port, self._key = adapter._host, adapter._port, adapter._api_key
        self._configure = configure_app
        self._phase, self._fault = 'NEW', None
        self._app = self._http_runner = self._site = self._socket = None
        self._server = self._request_server = None
        self._address = None
        self._handlers = {}
        self._routes = self._middlewares = self._hooks = None
        bundle._borrowed_http_transport = self

    @property
    def adapter(self):
        return self._adapter

    @property
    def app(self):
        return self._app

    def _owner_loop(self):
        require((os.getpid(), threading.get_ident()) == self._actor,
                'borrowed_transport_actor')
        require(asyncio.get_running_loop() is self._loop, 'borrowed_transport_other_loop')

    def _pair_identity(self):
        b, binding, runner, adapter = self._bundle, self._binding, self._runner, self._adapter
        require(os.getpid() == self._actor[0] and not self._loop.is_closed(),
                'borrowed_transport_process_or_loop')
        require(b._borrowed_http_transport is self, 'borrowed_transport_replaced')
        require(_same((b._prepared_binding, b._prepared_runner, b._adapter),
                      (binding, runner, adapter)), 'borrowed_transport_pair_changed')
        require(_same((binding._api_bundle, runner._caller_owned_state, adapter._borrowed_api_stores,
                       adapter.gateway_runner), (b, binding, b, runner)),
                'borrowed_transport_owner_changed')
        require(binding._authority is self._authority
                and (binding.epoch, binding.instance_id) == self._epoch,
                'borrowed_transport_authority_changed')
        require(b.home == self._home and binding.home == self._home,
                'borrowed_transport_profile_changed')
        binding._check_prepared(runner)
        from gateway.run_borrowed_publication import check_publication
        check_publication(b)

    def _store_identity(self):
        b, adapter = self._bundle, self._adapter
        require(_same((b.state, b.replay, b.responses), self._stores)
                and _same(tuple(s._conn for s in self._stores), self._connections),
                'borrowed_transport_stores_changed')
        tuple(connection.in_transaction for connection in self._connections)
        require(_same((adapter._session_db, adapter._run_idempotency_store,
                       adapter._response_store), self._stores),
                'borrowed_transport_adapter_stores_changed')
        require(b.state._lock is b._state_lock and b.replay._lock is b._prepared_replay_lock,
                'borrowed_transport_store_lock_changed')
        require(b.responses._canonical_state is b._canonical
                and b._canonical._state is b.state
                and b._canonical._connection is self._connections[0],
                'borrowed_transport_canonical_changed')

    def _configuration_identity(self):
        adapter, runner, binding = self._adapter, self._runner, self._binding
        require(adapter.config is self._config and runner.config is binding.config,
                'borrowed_transport_config_replaced')
        require((adapter._host, adapter._port, adapter._api_key) ==
                (self._host, self._port, self._key), 'borrowed_transport_address_or_key_changed')
        config = binding.config
        require(config.multiplex_profiles is False and config.write_sessions_json is False
                and not config.platforms and not config.profile_routes,
                'borrowed_transport_configuration_changed')
        require(self._bundle._publication_loop is self._loop,
                'borrowed_transport_publication_loop_changed')
        require(self._loop.get_task_factory() is None, 'borrowed_transport_task_factory')

    def _state_owner_identity(self):
        binding, state = self._binding, self._stores[0]
        require(_same((binding._facade, binding._wrapper, binding._gate,
                       binding._facade._borrowed_session_state), self._state_owners),
                'borrowed_transport_state_owner_replaced')
        facade, wrapper, gate, facade_gate = self._state_owners
        # facade._db is a property that runs the native lock/SQL selector. Its
        # exact captured gate is the pure identity path inside a writer txn.
        require(_same((binding.state, self._authority.db, wrapper._db,
                       gate._state, facade_gate._state), (state,) * 5),
                'borrowed_transport_authority_store_changed')
        require(self._runner.session_store is facade and facade.config is binding.config,
                'borrowed_transport_facade_changed')
        require(gate._store is None and facade_gate._store is facade,
                'borrowed_transport_session_gate_owner_changed')
        self._session_gate_identity(gate)
        self._session_gate_identity(facade_gate)

    def _session_gate_identity(self, gate):
        require(gate._conn is self._connections[0] and gate._lock is self._bundle._state_lock
                and gate._closed is False, 'borrowed_transport_session_gate_changed')
        require(gate._profile_handle is self._profile_handle
                and gate._ownership._handles.get(gate._home_key) is self._profile_handle,
                'borrowed_transport_profile_owner_changed')

    def _handle_identity(self):
        adapter = self._adapter
        require(_same((adapter._app, adapter._runner, adapter._site),
                      (self._app, self._http_runner, self._site)),
                'borrowed_transport_handles_changed')
        require(self._phase in ('NEW', 'BINDING', 'READY', 'RETIRING', 'RETIRED'),
                'borrowed_transport_outcome_unknown')
        require(adapter._running is (self._phase in ('READY', 'RETIRING')),
                'borrowed_transport_running_changed')
        if self._phase in ('NEW', 'RETIRED'):
            require(all(x is None for x in (self._app, self._http_runner, self._site,
                                            self._socket, self._server, self._request_server)),
                    'borrowed_transport_resources_retained')
        if self._phase == 'READY':
            self._live_identity()

    def _live_identity(self):
        from aiohttp import web
        require(type(self._app) is web.Application and type(self._http_runner) is web.AppRunner
                and type(self._site) is web.SockSite, 'borrowed_transport_native_handles')
        require(self._http_runner.app is self._app and self._site._runner is self._http_runner
                and self._site._server is self._server
                and self._http_runner.server is self._request_server,
                'borrowed_transport_server_changed')
        require(self._server is not None and self._server.is_serving(),
                'borrowed_transport_listener_unavailable')
        require(self._app.frozen and tuple(self._app.router.routes()) == self._routes
                and tuple(self._app.middlewares) == self._middlewares,
                'borrowed_transport_routes_changed')
        require(self._app.get('api_server_adapter') is self._adapter
                and self._app.get('gateway_runner') is self._runner,
                'borrowed_transport_app_owner_changed')

    def _check_identity(self):
        from gateway.run_borrowed_state import BorrowedRunnerRejected
        try:
            self._pair_identity()
            self._store_identity()
            self._state_owner_identity()
            self._configuration_identity()
            self._handle_identity()
        except (BorrowedRunnerRejected, sqlite3.Error, AttributeError, KeyError, TypeError, ValueError):
            raise BorrowedStoreRejected('borrowed_transport_identity_unavailable') from None

    def check_binding(self, *, for_admission=False, app=None, adapter=None):
        """Pure object/phase check; legal inside an off-loop native SQL guard."""
        require(type(for_admission) is bool, 'borrowed_transport_admission_option')
        require(app is None or app is self._app, 'borrowed_transport_foreign_app')
        require(adapter is None or adapter is self._adapter, 'borrowed_transport_foreign_adapter')
        self._check_identity()
        require(self._phase == 'READY' and self._bundle._stage == 'ATTACHED',
                'borrowed_transport_not_ready')
        require(self._binding._stage in ('ACTIVE', 'DRAINING'),
                'borrowed_transport_runtime_phase')
        if for_admission:
            require(self._binding._stage == 'ACTIVE', 'borrowed_transport_admission_closed')
        return self._authority

    def check(self, *, for_admission=False, app=None, adapter=None):
        self._owner_loop()
        authority = self.check_binding(for_admission=for_admission, app=app, adapter=adapter)
        from gateway.platforms.api_server_prepared import check_prepared
        check_prepared(self._bundle, self._adapter)
        try:
            require(self._socket.fileno() >= 0 and self._socket.getsockname() == self._address,
                    'borrowed_transport_socket_changed')
        except OSError:
            raise BorrowedStoreRejected('borrowed_transport_socket_unavailable') from None
        return authority

    def _unknown(self, reason):
        # No retry or caller-store release can prove an interrupted effect ended.
        self._phase = 'UNKNOWN'
        if self._fault is None:
            self._fault = reason

    def _handler_done(self, record, which, task):
        if getattr(record, which) is not task or self._handlers.get(record.outer) is not record:
            self._unknown('borrowed_transport_handler_identity')
            return
        setattr(record, which + '_done', True)
        if which == 'inner':
            if task.cancelled():
                self._unknown('borrowed_transport_handler_cancelled')
                return
            task.exception()  # Also retrieve errors after an observer disconnected.
        if record.outer_done and record.inner_done and not record.inner.cancelled():
            del self._handlers[record.outer]

    async def _invoke_handler(self, request, handler):
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from agent.secret_scope import set_secret_scope, reset_secret_scope
        from tools.terminal_scope import set_terminal_scope, reset_terminal_scope
        from gateway.platforms.api_server import _api_request_profile
        home_token = secret_token = terminal_token = profile_token = None
        try:
            home_token = set_hermes_home_override(str(self._home))
            secret_token = set_secret_scope(dict(self._secret_scope), profile_home=str(self._home))
            terminal_token = set_terminal_scope(dict(self._terminal_scope))
            profile_token = _api_request_profile.set(None)
            # Requests install the already-prepared owning scopes. Observation
            # and unauthenticated traffic never hydrate external secret sources.
            return await handler(request)
        finally:
            if profile_token is not None:
                _api_request_profile.reset(profile_token)
            if terminal_token is not None:
                reset_terminal_scope(terminal_token)
            if secret_token is not None:
                reset_secret_scope(secret_token)
            if home_token is not None:
                reset_hermes_home_override(home_token)

    def _tracking_middleware(self):
        from aiohttp import web

        @web.middleware
        async def tracked(request, handler):
            try:
                self._owner_loop()
                self.check_binding(app=request.app)
            except BorrowedStoreRejected:
                return web.json_response({'error': {'code': 'borrowed_transport_unavailable'}}, status=503)
            outer = asyncio.current_task()
            require(type(outer) is asyncio.Task and outer not in self._handlers,
                    'borrowed_transport_handler_task')
            inner = self._loop.create_task(self._invoke_handler(request, handler))
            record = _Handler(outer, inner, request)
            self._handlers[outer] = record
            outer.add_done_callback(lambda done: self._handler_done(record, 'outer', done))
            inner.add_done_callback(lambda done: self._handler_done(record, 'inner', done))
            # A disconnected observer must not cancel a ledger worker awaiting
            # to_thread, or make that physical writer disappear from retirement.
            return await asyncio.shield(inner)

        return tracked

    def _native_control(self, handler):
        async def authenticated(request):
            if len(request.headers.getall('Authorization', ())) != 1:
                return self._adapter._auth_failed_response()
            error = self._adapter._check_auth(request)
            return error if error is not None else await handler(request)
        return authenticated

    @staticmethod
    def _lifecycle_hooks(app):
        return tuple(tuple(signal) for signal in (app.on_startup, app.on_shutdown,
                                                  app.on_cleanup, app.cleanup_ctx))

    def _prepare_app(self):
        from aiohttp import web
        from gateway.platforms.api_server import (
            MAX_REQUEST_BYTES, body_limit_middleware, cors_middleware, security_headers_middleware)
        app = self._app = web.Application(client_max_size=MAX_REQUEST_BYTES,
            middlewares=[cors_middleware, body_limit_middleware, security_headers_middleware])
        self._adapter._app = app
        app['api_server_adapter'], app['gateway_runner'] = self._adapter, self._runner
        for method, path, handler in (
            ('GET', '/v1/runs/{run_id}', self._adapter._handle_get_run),
            ('GET', '/v1/runs/{run_id}/events', self._adapter._handle_run_events),
            ('POST', '/v1/runs/{run_id}/stop', self._adapter._handle_stop_run),
        ):
            app.router.add_route(method, path, self._native_control(handler))
        hooks = self._lifecycle_hooks(app)
        result = self._configure(app, self._adapter, self)
        if inspect.iscoroutine(result):
            result.close()
        require(result is None, 'borrowed_transport_configurator_must_be_synchronous')
        require(self._lifecycle_hooks(app) == hooks, 'borrowed_transport_unowned_lifecycle_hooks')
        require(not app.frozen, 'borrowed_transport_app_frozen_early')
        app.middlewares.insert(0, self._tracking_middleware())
        self._routes, self._middlewares, self._hooks = (
            tuple(app.router.routes()), tuple(app.middlewares), hooks)

    def _bind_socket(self):
        sock = self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.set_inheritable(False)
        if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        sock.bind((self._host, self._port))
        sock.listen(socket.SOMAXCONN)
        sock.setblocking(False)
        self._address = sock.getsockname()

    def _require_idle(self, *, retiring):
        from gateway.run_borrowed_admission import _idle_work, _idle_ledgers
        from gateway.run_borrowed_cleanup import _sessions_idle
        from gateway.run_borrowed_state import BorrowedRunnerRejected
        try:
            _idle_work(self._binding, self._runner, self._adapter,
                       allow_idle_sessions=retiring, runtime_cleanup=retiring)
            _idle_ledgers(self._bundle, self._binding)
            if retiring:
                _sessions_idle(self._binding, self._loop)
        except BorrowedRunnerRejected as exc:
            # This guard precedes every transport effect. A retained queued row
            # is a typed refusal, so the caller may still observe/stop during drain.
            raise BorrowedStoreRejected(str(exc)) from None

    async def start(self):
        from aiohttp import web
        from gateway.platforms.api_server_prepared import check_prepared
        self._owner_loop()
        self._check_identity()
        require(self._phase == 'NEW' and self._binding._stage == 'ACTIVE',
                'borrowed_transport_not_fresh_active')
        check_prepared(self._bundle, self._adapter)
        self._require_idle(retiring=False)
        self._phase = 'BINDING'
        try:
            self._prepare_app()
            self._http_runner = web.AppRunner(self._app, handler_cancellation=False)
            self._adapter._runner = self._http_runner
            await self._http_runner.setup()
            self._request_server = self._http_runner.server
            self._bind_socket()
            self._site = web.SockSite(self._http_runner, self._socket)
            self._adapter._site = self._site
            await self._site.start()
            self._server = self._site._server
            self._adapter._running = True
            self._phase = 'READY'
            self.check(for_admission=True)
        except BaseException:
            self._unknown('borrowed_transport_start_unknown')
            raise
        return self.status()

    def begin_drain(self):
        self._owner_loop()
        self.check_binding()
        self._bundle.begin_drain()
        return self.status()

    async def retire_idle(self):
        self._owner_loop()
        self.check()
        require(self._binding._stage == 'DRAINING', 'borrowed_transport_requires_drain')
        require(not self._handlers, 'borrowed_transport_handlers_active')
        self._require_idle(retiring=True)
        # Close the entry gate in the same loop step as the last idle snapshot.
        self._phase = 'RETIRING'
        try:
            await self._site.stop()
            await self._http_runner.cleanup()
            await self._server.wait_closed()
            require(not self._handlers and not self._server.is_serving(),
                    'borrowed_transport_cleanup_incomplete')
            require(self._socket.fileno() == -1, 'borrowed_transport_socket_retained')
            require(not self._http_runner.sites and self._http_runner.server is None,
                    'borrowed_transport_runner_retained')
            self._adapter._running = False
            self._adapter._app = self._adapter._runner = self._adapter._site = None
            self._app = self._http_runner = self._site = self._socket = None
            self._server = self._request_server = None
            self._phase = 'RETIRED'
            self._check_identity()
        except BaseException:
            self._unknown('borrowed_transport_retirement_unknown')
            raise
        return self.status()

    def status(self):
        self._owner_loop()
        state = self._phase
        if state == 'READY':
            self.check_binding()
        if state == 'READY' and self._binding._stage == 'DRAINING':
            state = 'DRAINING'
        port = self._address[1] if self._address is not None else self._port
        return {'state': state, 'host': self._host, 'port': port,
                'endpoint': f'http://{self._host}:{port}' if self._address is not None else None,
                'activeHandlers': len(self._handlers), 'listenerAuthorized': state in ('READY', 'DRAINING'),
                'callerStoresClosed': False, 'epochAdvanced': False, 'reason': self._fault}


def check_prepared_transport(bundle, adapter):
    """Only the typed, one-shot opt-in may replace the listener-free invariant."""
    transport = bundle._borrowed_http_transport
    require(type(transport) is BorrowedHTTPTransport and transport._bundle is bundle
            and transport._adapter is adapter, 'borrowed_transport_binding_changed')
    transport._check_identity()


def listener_authorized(bundle):
    transport = bundle._borrowed_http_transport
    require(type(transport) is BorrowedHTTPTransport and transport._bundle is bundle,
            'borrowed_transport_binding_changed')
    if transport._phase != 'READY':
        return False
    transport.check_binding()
    return True


def require_transport_retired(bundle):
    transport = bundle._borrowed_http_transport
    if transport is not None:
        check_prepared_transport(bundle, bundle._adapter)
        require(transport._phase in ('NEW', 'RETIRED') and not transport._handlers,
                'borrowed_transport_must_retire_first')
