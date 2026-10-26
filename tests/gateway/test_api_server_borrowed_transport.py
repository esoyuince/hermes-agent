"""Borrowed HTTP ownership with real stores, loopback I/O and physical workers.

The fixture owns a temporary profile. No provider, model, ordinary gateway
startup, reconnect watcher or personal profile participates in these tests.
"""

import asyncio
from pathlib import Path
import socket
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from aiohttp import ClientSession, web
from aiohttp.test_utils import make_mocked_request
import pytest
import pytest_asyncio

from gateway.config import GatewayConfig, PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.api_server_borrowed import BorrowedAPIStores, BorrowedStoreRejected
from gateway.platforms.api_server_borrowed_transport import BorrowedHTTPTransport
from gateway.platforms.api_server_response_store import ResponseStore
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from gateway.run import GatewayRunner
from gateway.run_borrowed_state import CallerOwnedRunnerState
from gateway.run_runtime import initialize_gateway_runtime
from gateway.runtime_ownership import process_ownership
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch
from tests.waiting import async_wait_until


API_KEY = "6edc5ea18cb807dcbda7cf899f4a522cc01cb0397bd96ff50f9efca36e5c8b32"
AUTH = {"Authorization": "Bearer " + API_KEY}


@pytest_asyncio.fixture
async def owner(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    process_ownership.reserve([home])
    state = replay = responses = adapter = None
    try:
        state = SessionDB(db_path=home / "state.db")
        epoch = begin_runtime_epoch(state, instance_id="borrowed-http-test")
        replay = RunIdempotencyStore(str(home / "runs_idempotency.db"))
        responses = ResponseStore(db_path=str(home / "response_store.db"), canonical_state=state)
        config = GatewayConfig(sessions_dir=home / "sessions", multiplex_profiles=False, write_sessions_json=False)
        binding = CallerOwnedRunnerState(config, state, epoch=epoch, instance_id="borrowed-http-test")
        runner = GatewayRunner(config, caller_state=binding)
        await initialize_gateway_runtime(runner)
        bundle = BorrowedAPIStores(home, state, replay, responses, prepared_runner=runner)
        adapter = APIServerAdapter(
            PlatformConfig(enabled=True, extra={"host": "127.0.0.1", "port": 0, "key": API_KEY}),
            borrowed_stores=bundle,
        )
        bundle.publish_api_adapter()
        bundle.activate_admission()
        yield SimpleNamespace(
            home=home, state=state, replay=replay, responses=responses, runner=runner,
            binding=binding, bundle=bundle, adapter=adapter, epoch=epoch,
        )
    finally:
        # Test-only resource disposal also handles deliberately UNKNOWN fixtures.
        # It does not retry the product lifecycle or claim a clean owner receipt.
        if adapter is not None:
            site, app_runner = adapter._site, adapter._runner
            if site is not None and app_runner is not None and site in app_runner.sites:
                await web.SockSite.stop(site)
            if app_runner is not None:
                await web.AppRunner.cleanup(app_runner)
            transport = getattr(adapter._borrowed_api_stores, "_borrowed_http_transport", None)
            if transport is not None and transport._socket is not None:
                transport._socket.close()
            await asyncio.to_thread(adapter._memory_sessions.close_all)
        for store in (responses, replay, state):
            if store is not None:
                store.close()
        process_ownership.release(home)


def _transport(owner, configure=None):
    return BorrowedHTTPTransport(owner.bundle, configure_app=configure or (lambda app, adapter, transport: None))


def _assert_owned(owner):
    assert process_ownership.owns(owner.home)
    for store in (owner.state, owner.replay, owner.responses):
        assert store._conn.execute("SELECT 1").fetchone()[0] == 1
    assert tuple(owner.state._conn.execute(
        "SELECT epoch,instance_id FROM runtime_epoch WHERE singleton=1",
    ).fetchone()) == (owner.epoch, "borrowed-http-test")


async def _finish_native(owner, transport):
    if owner.binding._stage == "ACTIVE":
        transport.begin_drain()
    await transport.retire_idle()
    await owner.bundle.retire_idle_runtime()
    owner.bundle.retire_idle_api_sessions()
    owner.bundle.finish_drain()
    await owner.adapter.disconnect()
    owner.binding.detach(owner.runner)
    _assert_owned(owner)


@pytest.mark.asyncio
async def test_default_prepared_adapter_cannot_start_ordinary_listener(owner):
    with pytest.raises(BorrowedStoreRejected, match="borrowed_listener_not_qualified"):
        await owner.adapter.connect()
    assert owner.adapter._app is owner.adapter._runner is owner.adapter._site is None
    assert owner.runner._running is False
    _assert_owned(owner)


@pytest.mark.asyncio
async def test_real_listener_exposes_only_selected_routes_and_retires_before_stores(owner):
    transport = _transport(owner)
    await transport.start()
    assert transport.check() is owner.runner.session_authority
    assert owner.runner._running is False
    assert owner.runner._loop_heartbeat_task is None
    assert not owner.runner._background_tasks
    assert not owner.adapter._background_tasks
    endpoint = transport.status()["endpoint"]
    assert endpoint.startswith("http://127.0.0.1:")
    async with ClientSession() as client:
        for method, path in (
            ("POST", "/v1/runs"), ("GET", "/api/sessions"),
            ("GET", "/p/default/v1/runs/run_" + "1" * 32),
            ("POST", "/v1/runs/run_" + "1" * 32 + "/resolve-unknown"),
            ("GET", "/api/cron"), ("POST", "/api/gateway/start"),
        ):
            async with client.request(method, endpoint + path, headers=AUTH) as response:
                assert response.status == 404, (method, path, await response.text())
        async with client.get(endpoint + "/v1/runs/run_" + "1" * 32) as response:
            assert response.status == 401
        run_url = endpoint + "/v1/runs/run_" + "1" * 32
        async with client.get(run_url, headers={"Authorization": "HermesRoom fixture"}) as response:
            assert response.status == 401
        async with client.get(run_url + "?token=" + API_KEY) as response:
            assert response.status == 401
        duplicate = [("Authorization", "Bearer " + API_KEY), ("Authorization", "Bearer invalid")]
        async with client.get(run_url, headers=duplicate) as response:
            assert response.status == 401
        async with client.get(endpoint + "/v1/runs/run_" + "1" * 32, headers=AUTH) as response:
            assert response.status == 404
        await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
        # Keep the real HTTP client's idle keepalive connection open: listener
        # retirement must drain that protocol before waiting for server closure.
        await asyncio.wait_for(_finish_native(owner, transport), 5)
    assert transport.status()["state"] == "RETIRED"
    assert owner.adapter._site is owner.adapter._runner is owner.adapter._app is None
    assert owner.binding._stage == owner.bundle._stage == "DETACHED"


@pytest.mark.asyncio
async def test_one_shot_start_and_foreign_loop_cannot_create_another_listener(owner):
    transport = _transport(owner)
    await transport.start()
    site, endpoint = owner.adapter._site, transport.status()["endpoint"]
    with pytest.raises(BorrowedStoreRejected):
        await transport.start()
    with pytest.raises(BorrowedStoreRejected):
        _transport(owner)

    async def foreign_start():
        await transport.start()

    with pytest.raises(BorrowedStoreRejected):
        await asyncio.to_thread(lambda: asyncio.run(foreign_start()))
    assert owner.adapter._site is site
    assert transport.status()["endpoint"] == endpoint
    _assert_owned(owner)
    await _finish_native(owner, transport)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["authority", "epoch", "connection", "adapter", "authority_store"])
async def test_binding_check_refuses_changed_owner_without_repair(owner, monkeypatch, changed):
    transport = _transport(owner)
    await transport.start()
    replacement = SimpleNamespace(db_path=owner.state.db_path)
    mutations = {
        "authority": ((owner.runner, "session_authority", object()),),
        "epoch": ((owner.binding._authority, "epoch", owner.epoch + 1),),
        "connection": ((owner.state, "_conn", object()),),
        "adapter": ((owner.bundle, "_adapter", object()),),
        "authority_store": ((owner.binding, "state", replacement),
                            (owner.binding._authority, "db", replacement)),
    }
    with monkeypatch.context() as patch:
        for target, name, value in mutations[changed]:
            patch.setattr(target, name, value)
        with pytest.raises(BorrowedStoreRejected):
            transport.check_binding()
    assert transport.check_binding() is owner.binding._authority
    _assert_owned(owner)
    await _finish_native(owner, transport)


@pytest.mark.asyncio
async def test_drain_seals_new_admission_but_worker_identity_check_stays_available(owner):
    transport = _transport(owner)
    await transport.start()
    assert await asyncio.to_thread(transport.check_binding, for_admission=True) is owner.binding._authority

    def guarded_write(conn):
        assert conn.in_transaction
        assert transport.check_binding(for_admission=True) is owner.binding._authority
        conn.execute("INSERT INTO state_meta(key,value) VALUES('borrowed-admission-guard','accepted')")

    await asyncio.to_thread(owner.state._execute_write, guarded_write)
    transport.begin_drain()
    assert await asyncio.to_thread(transport.check_binding, for_admission=False) is owner.binding._authority
    with pytest.raises(BorrowedStoreRejected):
        await asyncio.to_thread(transport.check_binding, for_admission=True)
    with pytest.raises(BorrowedStoreRejected):
        await asyncio.to_thread(owner.state._execute_write, guarded_write)
    assert owner.state._conn.execute(
        "SELECT COUNT(*) FROM state_meta WHERE key='borrowed-admission-guard'",
    ).fetchone()[0] == 1
    assert owner.binding._stage == "DRAINING" and owner.binding._authority.retiring
    await _finish_native(owner, transport)


@pytest.mark.asyncio
async def test_native_retirement_order_keeps_live_transport_ahead_of_runtime_and_detach(owner):
    transport = _transport(owner)
    await transport.start()
    transport.begin_drain()
    site = owner.adapter._site
    with pytest.raises(BorrowedStoreRejected):
        await owner.bundle.retire_idle_runtime()
    with pytest.raises(BorrowedStoreRejected):
        owner.bundle.retire_idle_api_sessions()
    with pytest.raises(BorrowedStoreRejected):
        owner.bundle.finish_drain()
    assert owner.binding._stage == "DRAINING"
    assert owner.adapter._site is site
    _assert_owned(owner)
    await _finish_native(owner, transport)


@pytest.mark.asyncio
async def test_real_queued_run_can_be_observed_and_cancelled_while_draining(owner):
    from gateway.session_api_turn import admit_api_turn

    transport = _transport(owner)
    await transport.start()
    run_id = "run_" + "3" * 32
    request = make_mocked_request("POST", "/v1/runs", headers=AUTH)
    authority, _ref, row = admit_api_turn(
        owner.adapter, session_id=run_id, request_id=run_id,
        user_message="This queued fixture must never execute.", conversation_history=[],
        run_owner_scope=owner.adapter._run_idempotency_scope(request),
    )
    assert row["status"] == "queued" and row["generation"] is None
    transport.begin_drain()
    with pytest.raises(BorrowedStoreRejected):
        await transport.retire_idle()
    endpoint = transport.status()["endpoint"]
    async with ClientSession(headers=AUTH) as client:
        async with client.get(endpoint + "/v1/runs/" + run_id) as response:
            assert response.status == 200
            assert (await response.json())["status"] == "queued"
        async with client.post(endpoint + "/v1/runs/" + run_id + "/stop") as response:
            assert response.status == 200
            stopped = await response.json()
            assert stopped["status"] == "cancelled"
            assert stopped["admission_id"] == row["admission_id"]
            assert stopped["execution_generation"] is None
        async with client.get(endpoint + "/v1/runs/" + run_id) as response:
            assert response.status == 200
            assert (await response.json())["status"] == "cancelled"
    await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
    assert not owner.adapter._stopping_run_ids
    assert not authority.waiters and not authority.cancel_obligations
    await _finish_native(owner, transport)


@pytest.mark.asyncio
async def test_retained_native_observer_blocks_listener_retirement_until_detached(owner):
    from gateway.platforms.api_server_authority_runs import stop_run
    from gateway.session_api_turn import admit_api_turn
    from gateway.session_contract import Principal

    transport = _transport(owner)
    await transport.start()
    run_id = "run_" + "7" * 32
    request = make_mocked_request("POST", "/v1/runs", headers=AUTH)
    authority, ref, _row = admit_api_turn(
        owner.adapter, session_id=run_id, request_id=run_id,
        user_message="Retained observer fixture", conversation_history=[],
        run_owner_scope=owner.adapter._run_idempotency_scope(request),
    )
    transport.begin_drain()
    assert (await stop_run(owner.adapter, run_id))["status"] == "cancelled"
    live = authority.sessions[ref.session_id]
    if live.task is not None:
        await asyncio.wait_for(asyncio.shield(live.task), 5)
    actor = Principal("api", authority.profile_id, frozenset({"session:read"}), "retained-observer")
    snapshot = await authority.attach(actor, ref)
    endpoint, site = transport.status()["endpoint"], owner.adapter._site
    try:
        assert snapshot.subscription_id in live.subscribers
        assert transport.status()["activeHandlers"] == 0
        with pytest.raises(BorrowedStoreRejected, match="retirement_live_obligation"):
            await transport.retire_idle()
        assert transport.status()["state"] == "DRAINING"
        assert owner.adapter._site is site and site._server.is_serving()
        _assert_owned(owner)
        async with ClientSession(headers=AUTH) as client:
            async with client.get(endpoint + "/v1/runs/" + run_id) as response:
                assert response.status == 200
                assert (await response.json())["status"] == "cancelled"
    finally:
        await authority.detach(actor, snapshot.subscription_id)
    assert not live.subscribers
    await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
    await _finish_native(owner, transport)
    assert transport.status()["state"] == "RETIRED"


@pytest.mark.asyncio
async def test_running_stop_during_drain_targets_claim_and_does_not_report_terminal(owner):
    from gateway.session_api_turn import admit_api_turn
    from hermes_state_runtime import claim_session_input

    stopped = []

    class ClaimedAgent:
        def hard_interrupt(self):
            stopped.append("hard-interrupt")

    transport = _transport(owner)
    await transport.start()
    run_id = "run_" + "5" * 32
    request = make_mocked_request("POST", "/v1/runs", headers=AUTH)
    authority, ref, row = admit_api_turn(
        owner.adapter, session_id=run_id, request_id=run_id,
        user_message="Claimed control fixture", conversation_history=[],
        run_owner_scope=owner.adapter._run_idempotency_scope(request),
    )
    claimed = claim_session_input(owner.state, epoch=owner.epoch, session_id=ref.session_id)
    generation = claimed["generation"]
    authority.adopted[ref.session_id] = (generation, ClaimedAgent())
    transport.begin_drain()
    endpoint = transport.status()["endpoint"]
    async with ClientSession(headers=AUTH) as client:
        async with client.post(endpoint + "/v1/runs/" + run_id + "/stop") as response:
            assert response.status == 200
            acknowledgement = await response.json()
            assert acknowledgement["status"] == "stopping"
            assert acknowledgement["admission_id"] == row["admission_id"]
        async with client.get(endpoint + "/v1/runs/" + run_id) as response:
            assert response.status == 200
            projection = await response.json()
            assert projection["status"] == "running"
            assert projection["execution_generation"] == generation
            assert "execution_proof" not in projection
    await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
    assert stopped == ["hard-interrupt"]
    assert authority.delivered_stops[ref.session_id] == generation
    with pytest.raises(BorrowedStoreRejected):
        await transport.retire_idle()
    _assert_owned(owner)


@pytest.mark.asyncio
async def test_cancelled_request_retains_its_physical_sql_handler_until_finished(owner):
    entered, release = threading.Event(), threading.Event()
    observed = {}

    def physical_write():
        entered.set()
        assert release.wait(5), "test did not release the SQL handler"
        owner.state._execute_write(lambda conn: conn.execute(
            "INSERT INTO state_meta(key,value) VALUES('borrowed-http-handler','settled')",
        ))

    def configure(app, adapter, transport):
        async def blocked(request):
            observed["outer"] = request.task
            await asyncio.to_thread(physical_write)
            return web.json_response({"settled": True})
        app.router.add_post("/fixture/blocked", blocked)

    transport = _transport(owner, configure)
    await transport.start()
    try:
        async with ClientSession(headers=AUTH) as client:
            call = asyncio.create_task(client.post(transport.status()["endpoint"] + "/fixture/blocked"))
            assert await asyncio.to_thread(entered.wait, 5)
            transport.begin_drain()
            observed["outer"].cancel()
            await asyncio.gather(call, return_exceptions=True)
            assert transport.status()["activeHandlers"] > 0
            with pytest.raises(BorrowedStoreRejected):
                await transport.retire_idle()
            _assert_owned(owner)
            assert owner.state._conn.execute(
                "SELECT 1 FROM state_meta WHERE key='borrowed-http-handler'",
            ).fetchone() is None
            release.set()
            await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
        assert owner.state._conn.execute(
            "SELECT value FROM state_meta WHERE key='borrowed-http-handler'",
        ).fetchone()[0] == "settled"
        await _finish_native(owner, transport)
    finally:
        release.set()


@pytest.mark.asyncio
async def test_completed_dispatcher_does_not_prove_physical_worker_has_stopped(owner):
    from gateway.turn_executor import _UnboundedThreadExecutor

    entered, release = threading.Event(), threading.Event()

    def physical_work():
        entered.set()
        assert release.wait(5), "test did not release the native worker"

    transport = _transport(owner)
    await transport.start()
    pool = _UnboundedThreadExecutor("borrowed-http-test")
    owner.runner._executor = pool

    async def dispatch_without_joining_worker():
        return pool.submit(physical_work)

    dispatcher = asyncio.create_task(dispatch_without_joining_worker())
    future = await dispatcher
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert dispatcher.done()
        transport.begin_drain()
        site = owner.adapter._site
        with pytest.raises(BorrowedStoreRejected, match="physical_worker_live"):
            await transport.retire_idle()
        assert owner.adapter._site is site
        _assert_owned(owner)
        release.set()
        await asyncio.wait_for(asyncio.wrap_future(future), 5)
        await async_wait_until(lambda: not pool._threads, timeout=5)
        await _finish_native(owner, transport)
    finally:
        release.set()
        await asyncio.to_thread(pool.shutdown, True)


@pytest.mark.asyncio
async def test_bind_failure_is_one_shot_unknown_and_keeps_original_owner(owner):
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        owner.adapter._port = occupied.getsockname()[1]
        owner.adapter.config.extra["port"] = owner.adapter._port
        transport = _transport(owner)
        with pytest.raises(OSError):
            await transport.start()
        assert transport.status()["state"] == "UNKNOWN"
        app_runner = owner.adapter._runner
        assert app_runner is not None
        _assert_owned(owner)
        with pytest.raises(BorrowedStoreRejected):
            await transport.start()
        with pytest.raises(BorrowedStoreRejected):
            await transport.retire_idle()
        assert owner.adapter._runner is app_runner


@pytest.mark.asyncio
async def test_cancelled_setup_retains_partial_resources_and_seals_native_admission(owner, monkeypatch):
    from hermes_state_runtime import RuntimeStoreError

    entered, release = asyncio.Event(), asyncio.Event()
    setup_original = web.AppRunner.setup

    async def setup_then_wait(app_runner):
        await setup_original(app_runner)
        entered.set()
        await release.wait()

    transport = _transport(owner)
    with monkeypatch.context() as patch:
        patch.setattr(web.AppRunner, "setup", setup_then_wait)
        start = asyncio.create_task(transport.start())
        await asyncio.wait_for(entered.wait(), 5)
        start.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start
    assert transport.status()["state"] == "UNKNOWN"
    assert owner.adapter._runner is not None
    _assert_owned(owner)
    with pytest.raises(RuntimeStoreError):
        owner.binding._authority._require_admission_open()
    with pytest.raises(BorrowedStoreRejected):
        await transport.start()


@pytest.mark.asyncio
async def test_cancelled_inner_handler_is_unknown_and_never_looks_idle(owner):
    entered = asyncio.Event()
    observed = {}

    def configure(app, adapter, transport):
        async def blocked(request):
            observed["inner"] = asyncio.current_task()
            entered.set()
            await asyncio.Event().wait()
        app.router.add_post("/fixture/cancelled", blocked)

    transport = _transport(owner, configure)
    await transport.start()
    async with ClientSession(headers=AUTH) as client:
        call = asyncio.create_task(client.post(transport.status()["endpoint"] + "/fixture/cancelled"))
        await asyncio.wait_for(entered.wait(), 5)
        observed["inner"].cancel()
        await asyncio.gather(call, return_exceptions=True)
    assert transport.status()["state"] == "UNKNOWN"
    assert transport.status()["activeHandlers"] > 0
    _assert_owned(owner)
    with pytest.raises(BorrowedStoreRejected):
        await transport.retire_idle()


@pytest.mark.asyncio
async def test_partial_retirement_never_reports_release_or_retries_cleanup(owner, monkeypatch):
    transport = _transport(owner)
    await transport.start()
    transport.begin_drain()
    site, app_runner = owner.adapter._site, owner.adapter._runner
    original_stop = site.stop
    calls = []

    async def stop_then_fail(current_site):
        assert current_site is site
        calls.append("stopped")
        await original_stop()
        raise RuntimeError("fixture cleanup acknowledgement lost")

    with monkeypatch.context() as patch:
        patch.setattr(web.SockSite, "stop", stop_then_fail)
        with pytest.raises(RuntimeError, match="acknowledgement lost"):
            await transport.retire_idle()
        assert transport.status()["state"] == "UNKNOWN"
        assert owner.adapter._site is site and owner.adapter._runner is app_runner
        _assert_owned(owner)
        with pytest.raises(BorrowedStoreRejected):
            await transport.retire_idle()
    assert calls == ["stopped"]


@pytest.mark.asyncio
async def test_metrics_do_not_start_an_unowned_default_status_writer(owner, monkeypatch):
    from gateway import status

    publish = Mock(side_effect=AssertionError("borrowed mode must not publish default status"))
    monkeypatch.setattr(status, "publish_runtime_status", publish)
    transport = _transport(owner)
    await transport.start()
    owner.adapter._record_api_metrics({"input_tokens": 2, "output_tokens": 3, "total_tokens": 5}, 0.01)
    assert owner.adapter._metrics_requests_today == 1
    assert owner.adapter._metrics_tokens_today == 5
    await _finish_native(owner, transport)
    publish.assert_not_called()


@pytest.mark.asyncio
async def test_requests_use_prepared_profile_scopes_without_hydrating_secrets(owner, monkeypatch):
    from agent.secret_scope import (
        current_secret_scope, current_secret_scope_home, reset_secret_scope, set_secret_scope,
    )
    from gateway import run
    from hermes_cli import env_loader
    from hermes_constants import get_hermes_home
    from tools.terminal_scope import get_terminal_scope

    hydrate = Mock(side_effect=AssertionError("HTTP observation must not hydrate secret sources"))
    load = Mock(side_effect=AssertionError("HTTP observation must not reload secret sources"))
    monkeypatch.setattr(env_loader, "hydrate_profile_secret_sources", hydrate)
    monkeypatch.setattr(run, "_load_profile_secret_scope", load)

    def configure(app, adapter, transport):
        async def scopes(request):
            return web.json_response({
                "homeMatches": get_hermes_home() == owner.home,
                "secretHomeMatches": current_secret_scope_home() == str(owner.home),
                "preparedValueMatches": current_secret_scope().get("FIXTURE_SECRET") == "prepared",
                "terminalScopeInstalled": isinstance(get_terminal_scope(), dict),
            })
        app.router.add_get("/fixture/scopes", scopes)

    prepared = {"FIXTURE_SECRET": "prepared"}
    token = set_secret_scope(prepared, profile_home=str(owner.home))
    try:
        transport = _transport(owner, configure)
        prepared["FIXTURE_SECRET"] = "later mutation"
    finally:
        reset_secret_scope(token)
    await transport.start()
    endpoint = transport.status()["endpoint"]
    async with ClientSession() as client:
        async with client.get(endpoint + "/v1/runs/run_" + "6" * 32) as response:
            assert response.status == 401
        async with client.get(endpoint + "/not-a-route", headers=AUTH) as response:
            assert response.status == 404
        async with client.get(endpoint + "/fixture/scopes", headers=AUTH) as response:
            assert response.status == 200
            assert all((await response.json()).values())
    await async_wait_until(lambda: transport.status()["activeHandlers"] == 0, timeout=5)
    await _finish_native(owner, transport)
    hydrate.assert_not_called()
    load.assert_not_called()


@pytest.mark.asyncio
async def test_constructor_refuses_a_prepared_secret_scope_from_another_profile(owner):
    from agent.secret_scope import reset_secret_scope, set_secret_scope

    token = set_secret_scope({"FIXTURE_SECRET": "foreign"}, profile_home=str(owner.home.parent / "foreign"))
    try:
        with pytest.raises(BorrowedStoreRejected, match="foreign_secret_scope"):
            _transport(owner)
    finally:
        reset_secret_scope(token)
    assert owner.bundle._borrowed_http_transport is None
    _assert_owned(owner)
