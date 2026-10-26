"""A queued Stop must not resurrect a live marker after its producer retires."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms import api_server
from gateway.platforms.api_server_authority_runs import run_projection, stop_run
from gateway.platforms.api_server_borrowed import _quiescent
from gateway.platforms.api_server_runs import _RunLaunch, _RunStream, _execute_run
from gateway.session_authority import LiveSession, SessionAuthority
from gateway.session_contract import Principal, SessionRef, Submission
from gateway.runtime_ownership import process_ownership
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, get_session_admission
from tests.waiting import async_wait_until


def _queued_owner(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    process_ownership.reserve([tmp_path])
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("queued-session", source="api_server")
    epoch = begin_runtime_epoch(db, instance_id="queued-stop-test")
    runner = SimpleNamespace(
        _draining=False,
        config=SimpleNamespace(multiplex_profiles=False),
        _adapter_for_source=lambda source: None,
    )
    authority = SessionAuthority(
        runner, profile_id="queued-profile", instance_id="queued-stop-test", db=db, epoch=epoch,
    )
    runner.session_authority = authority
    authority.sessions["queued-session"] = LiveSession(
        SimpleNamespace(platform=Platform.API_SERVER, user_id="api"), "queued-route",
    )
    # Hold the native FIFO before a claim; the cancellation still uses real SQL
    # admission, cancellation, observer settlement and projection implementations.
    monkeypatch.setattr(authority, "_schedule", lambda ref: None)
    adapter = api_server.APIServerAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    monkeypatch.setattr(adapter, "_publish_runtime_status", lambda: None)
    actor = Principal("api", "queued-profile", frozenset({"session:submit", "session:control"}), "queued-client")
    return db, authority, adapter, actor, SessionRef("queued-profile", "queued-session")


async def _admit(authority, actor, ref, run_id):
    receipt = await authority.submit(
        actor, Submission(request_id=run_id, ref=ref, payload={"text": "queued text"}, intent="queue"),
    )
    row = get_session_admission(authority.db, admission_id=receipt.admission_id)
    assert row["status"] == "queued" and row["generation"] is None
    return row


def _expected_queued_proof(authority, ref, row):
    return {
        "schema": 1, "kind": "canonical_queued_cancellation",
        "run_id": row["request_id"], "admission_id": row["admission_id"],
        "session_id": ref.session_id, "authority_epoch": authority.epoch,
        "instance_id": authority.instance_id, "execution_generation": None,
        "no_worker_started_verified": True,
    }


def _launch(adapter, authority, ref, row):
    run_id = row["request_id"]
    stream = _RunStream()
    adapter._run_streams[run_id] = stream
    run = _RunLaunch(
        owner=adapter, run_id=run_id, queue=stream, session_id=ref.session_id,
        gateway_session_key=None, declared_selected=False, user_message="queued text",
        conversation_history=[], session_history_delivery=False, agent_kwargs={},
        request_profile=None, browser_control_principal=None, browser_control_transport_family=None,
        admission=(authority, ref, row),
    )
    task = asyncio.create_task(_execute_run(adapter, run, _api_server=api_server))
    adapter._active_run_tasks[run_id] = task
    return task


@pytest.mark.asyncio
async def test_queued_stop_after_producer_retirement_leaves_no_live_marker(tmp_path, monkeypatch):
    from gateway import session_ingress_media

    db, authority, adapter, actor, ref = _queued_owner(tmp_path, monkeypatch)
    media_entered, release_media = threading.Event(), threading.Event()
    release_original = session_ingress_media.release_admission_media

    def delayed_media_release(*args, **kwargs):
        # cancel_queued publishes the terminal receipt and wakes the producer
        # before this separate, off-loop media transaction finishes.
        media_entered.set()
        assert release_media.wait(5), "test did not release the media worker"
        return release_original(*args, **kwargs)

    monkeypatch.setattr(session_ingress_media, "release_admission_media", delayed_media_release)
    producer = stop = None
    try:
        run_id = "run_" + "1" * 32
        row = await _admit(authority, actor, ref, run_id)
        producer = _launch(adapter, authority, ref, row)
        await async_wait_until(lambda: row["admission_id"] in authority.waiters, timeout=5)
        stop = asyncio.create_task(stop_run(adapter, run_id))
        assert await asyncio.to_thread(media_entered.wait, 5)
        await asyncio.wait_for(asyncio.shield(producer), 5)
        assert run_id not in adapter._active_run_tasks
        assert run_id not in adapter._stopping_run_ids
        assert not stop.done(), "the Stop still owns its media cleanup obligation"

        release_media.set()
        projection = await asyncio.wait_for(stop, 5)
        assert projection["status"] == "cancelled"
        assert projection["admission_id"] == row["admission_id"]
        assert projection["execution_generation"] is None
        assert not authority.cancel_obligations
        assert run_id not in adapter._stopping_run_ids
        _quiescent(adapter)
        assert projection["execution_proof"] == _expected_queued_proof(authority, ref, row)
    finally:
        release_media.set()
        await asyncio.gather(*(task for task in (producer, stop) if task is not None), return_exceptions=True)
        await adapter.disconnect()
        db.close()
        process_ownership.release(tmp_path)


@pytest.mark.asyncio
async def test_unobserved_queued_stop_during_drain_remains_terminal_and_idempotent(tmp_path, monkeypatch):
    db, authority, adapter, actor, ref = _queued_owner(tmp_path, monkeypatch)
    try:
        run_id = "run_" + "2" * 32
        row = await _admit(authority, actor, ref, run_id)
        authority.retiring = True
        authority.runner._draining = True

        first = await stop_run(adapter, run_id)
        second = await stop_run(adapter, run_id)
        assert first == second
        assert first["status"] == "cancelled"
        assert first["admission_id"] == row["admission_id"]
        assert first["execution_generation"] is None
        assert not authority.waiters and not authority.cancel_obligations
        assert not adapter._stopping_run_ids
        _quiescent(adapter)
        assert first["execution_proof"] == _expected_queued_proof(authority, ref, row)
        assert db._conn.execute(
            "SELECT COUNT(*) FROM worker_executions WHERE session_id=?", (ref.session_id,),
        ).fetchone()[0] == 0
    finally:
        await adapter.disconnect()
        db.close()
        process_ownership.release(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("contradiction", ["new_epoch", "exact_worker", "started_turn"])
async def test_queued_proof_requires_current_epoch_and_no_claim_or_exact_worker(tmp_path, monkeypatch, contradiction):
    from hermes_state_runtime import (
        claim_session_input, register_worker_execution, settle_session_input,
    )

    db, authority, adapter, actor, ref = _queued_owner(tmp_path, monkeypatch)
    try:
        run_id = "run_" + "4" * 32
        row = await _admit(authority, actor, ref, run_id)
        if contradiction == "started_turn":
            claimed = claim_session_input(db, epoch=authority.epoch, session_id=ref.session_id)
            settle_session_input(
                db, epoch=authority.epoch, admission_id=row["admission_id"],
                generation=claimed["generation"], outcome="interrupted",
            )
        else:
            stopped = await stop_run(adapter, run_id)
            assert "execution_proof" in stopped
            if contradiction == "new_epoch":
                begin_runtime_epoch(db, instance_id="replacement-owner")
            else:
                register_worker_execution(
                    db, epoch=authority.epoch,
                    execution_id="admission-worker:" + row["admission_id"],
                    session_id=ref.session_id, generation=0, kind="compute",
                    adoption_secret="fixture-worker-secret",
                )
        projection = run_projection(adapter, run_id)
        assert projection["status"] == "cancelled"
        assert projection["admission_id"] == row["admission_id"]
        assert "execution_proof" not in projection
    finally:
        await adapter.disconnect()
        db.close()
        process_ownership.release(tmp_path)
