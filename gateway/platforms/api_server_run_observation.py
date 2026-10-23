"""Opt-in synchronous observations from the native run producer.

Notifications are diagnostics, not execution admission or executor-liveness proof.
A broken observer latches GAP without retrying, cancelling or replaying work. The
channel never reads databases, request bodies, credentials or task exceptions.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
import inspect
import os
import threading


@dataclass(frozen=True, slots=True)
class RunBinding:
    run_id: str
    session_id: str
    scope: str = field(repr=False)
    key: str | None = field(repr=False)
    fingerprint: str | None = field(repr=False)
    execution_role: str
    admission_id: str | None
    admission_epoch: int | None
    admission_generation: int | None
    canonical_session_id: str | None


@dataclass(frozen=True, slots=True)
class RunProducerEvent:
    source: RunLifecycleChannel = field(repr=False)
    sequence: int
    phase: str
    binding: RunBinding
    task: asyncio.Task | None = field(repr=False)
    task_done: bool | None
    task_cancelled: bool | None
    native_status: str | None
    worker_count: int


class RunLifecycleChannel:
    """One explicit local sink, attached while the adapter is quiescent.

    No environment/config auto-enablement and no detach/replacement API. A native
    API Task may watch a different executor; execution_role keeps that distinction.
    """
    def __init__(self, adapter, sink):
        from gateway.platforms.api_server import APIServerAdapter
        if type(adapter) is not APIServerAdapter or not callable(sink):
            raise ValueError('run_observation_adapter_or_sink')
        loop = asyncio.get_running_loop()
        if (getattr(adapter, '_run_observation_channel', None) is not None
                or adapter._active_run_tasks or adapter._background_tasks
                or adapter._pending_agent_requests != 0 or adapter._inflight_agent_runs != 0
                or loop.get_task_factory() is not None):
            raise ValueError('run_observation_not_quiescent')
        self._adapter = adapter
        self._sink = sink
        self._loop = loop
        self._actor = (os.getpid(), threading.get_ident())
        self._sequence = 0
        self._fault = None
        self._emitting = False
        adapter._run_observation_channel = self

    def status(self):
        return {'state': 'GAP' if self._fault else 'OBSERVING',
                'reason': self._fault, 'sequence': self._sequence,
                'executorHealthVerified': False, 'runtimeAdmissionAuthorized': False}

    def _gap(self, reason):
        if self._fault is None:
            self._fault = reason

    def _emit(self, phase, binding, task=None):
        if self._fault is not None:
            return
        try:
            if ((os.getpid(), threading.get_ident()) != self._actor
                    or asyncio.get_running_loop() is not self._loop
                    or self._adapter._run_observation_channel is not self
                    or self._emitting):
                self._gap('run_observation_identity_or_reentrancy')
                return
            from gateway.platforms.api_server_runs import api_worker_live_count
            self._sequence += 1
            status = self._adapter._run_statuses.get(binding.run_id, {}).get('status')
            event = RunProducerEvent(self, self._sequence, phase, binding, task,
                task.done() if task is not None else None,
                task.cancelled() if task is not None else None,
                status if type(status) is str else None, api_worker_live_count())
            self._emitting = True
            value = self._sink(event)
            if value is not None:
                if inspect.iscoroutine(value):
                    value.close()
                self._gap('run_observation_sink_must_be_synchronous')
            if self._adapter._run_observation_channel is not self:
                self._gap('run_observation_replaced')
        except (Exception, asyncio.CancelledError):
            self._gap('run_observation_sink_failed')
        finally:
            self._emitting = False


class _RunObservation:
    def __init__(self, channel, binding):
        self.channel, self.binding = channel, binding
        self.task = None
        channel._emit('ACCEPTED', binding)

    def created(self, task):
        a = self.channel._adapter
        if (type(task) is not asyncio.Task or task.done()
                or task.get_loop() is not self.channel._loop
                or a._active_run_tasks.get(self.binding.run_id) is not task
                or task not in a._background_tasks):
            self.channel._gap('run_observation_task_publication_gap')
            return
        self.task = task
        self.channel._emit('CREATED', self.binding, task)
        task.add_done_callback(self._done)

    def emit_current(self, phase):
        task = asyncio.current_task()
        if task is None or task is not self.task:
            self.channel._gap('run_observation_task_identity_gap')
            return
        self.channel._emit(phase, self.binding, task)

    def _done(self, task):
        # Native retirement and the existing background discard callback precede
        # this callback. A pre-start cancellation may have skipped that cleanup.
        a = self.channel._adapter
        if (task is not self.task or not task.done()
                or self.binding.run_id in a._active_run_tasks
                or task in a._background_tasks):
            self.channel._gap('run_observation_native_retirement_gap')
            return
        self.channel._emit('DONE', self.binding, task)


def prepare(adapter, launch, scope, key, fingerprint, *, external_owner):
    channel = getattr(adapter, '_run_observation_channel', None)
    if channel is None:
        return None
    if type(channel) is not RunLifecycleChannel or channel._adapter is not adapter:
        # A replaced channel is never accepted as an equivalent source.
        return None
    if channel._fault is not None:
        return None
    try:
        if asyncio.get_running_loop().get_task_factory() is not None:
            channel._gap('run_observation_eager_factory_unqualified')
            return None
        admission = launch.admission
        row = admission[2] if admission is not None else {}
        role = ('external_owner_observer' if external_owner else
                'canonical_authority_observer' if admission is not None else 'executor_thread_carrier')
        binding = RunBinding(launch.run_id, launch.session_id, scope or adapter._run_owners[launch.run_id],
            key or None, fingerprint or None, role, row.get('admission_id'), row.get('owner_epoch'),
            row.get('generation'), row.get('target_session_id'))
        return _RunObservation(channel, binding)
    except Exception:
        channel._gap('run_observation_binding_unavailable')
        return None


def created(launch, task):
    if launch.observation is not None:
        launch.observation.created(task)


def notify(launch, phase):
    if launch.observation is not None:
        launch.observation.emit_current(phase)
