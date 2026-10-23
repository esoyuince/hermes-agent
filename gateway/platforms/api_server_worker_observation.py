"""Source-bound native executor lifetime, separate from its asyncio carrier.

This bounded diagnostic book neither schedules work nor closes/cancels it. A
carrier may finish while the submitted function still runs. Completion is recorded
only by that function's native finally, after releasing the native worker count.
No worker-thread callback into an observer is made, and no payload/result/error is
retained. Positive service health and canonical admission remain separate gates.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
import os
import threading
import time

class WorkerObservationRejected(RuntimeError):
    pass

class WorkerObservationUnavailable(RuntimeError):
    pass

@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    binding: object = field(repr=False)
    submission: int
    state: str
    owner_pid: int
    thread_id: int | None
    native_thread_id: int | None
    submitted_ns: int
    started_ns: int | None
    finished_ns: int | None
    carrier_done: bool
    carrier_cancelled: bool
    carrier_released: bool

@dataclass(slots=True)
class _Record:
    binding: object
    task: object
    submitted_ns: int
    state: str = 'SUBMITTED'
    thread: object = None
    thread_id: int | None = None
    native_thread_id: int | None = None
    started_ns: int | None = None
    finished_ns: int | None = None
    carrier_done: bool = False
    carrier_cancelled: bool = False

@dataclass(frozen=True, slots=True)
class _Ticket:
    book: object = field(repr=False)
    number: int

class WorkerBook:
    def __init__(self, channel):
        self._channel = channel
        self._lock = threading.Lock()
        self._records = {}
        self._revision = 0
        self._next = 0
        self._fault = None

    def _gap(self):
        self._fault = 'worker_observation_gap'

    def _actor(self):
        ch = self._channel
        if ((os.getpid(), threading.get_ident()) != ch._actor
                or asyncio.get_running_loop() is not ch._loop
                or ch._adapter._run_observation_channel is not ch
                or ch._fault is not None or self._fault is not None):
            raise WorkerObservationRejected('worker_source_or_actor_changed')

    def submit(self, observation):
        self._actor()
        task = asyncio.current_task()
        ch = self._channel
        if (observation.channel is not ch or observation.task is not task
                or type(task) is not asyncio.Task or task.done()
                or task.get_loop() is not ch._loop
                or observation.binding.execution_role != 'executor_thread_carrier'
                or ch._adapter._active_run_tasks.get(observation.binding.run_id) is not task
                or task not in ch._adapter._background_tasks
                or getattr(observation, '_worker_ticket', None) is not None):
            raise WorkerObservationRejected('worker_native_submission_identity')
        with self._lock:
            if len(self._records) >= 256:
                raise WorkerObservationRejected('worker_inventory_limit')
            self._next += 1
            ticket = _Ticket(self, self._next)
            self._records[ticket.number] = _Record(observation.binding, task, time.monotonic_ns())
            self._revision += 1
            observation._worker_ticket = ticket
        return ticket

    def transition(self, ticket, state):
        with self._lock:
            if type(ticket) is not _Ticket or ticket.book is not self:
                raise WorkerObservationRejected('worker_ticket_changed')
            r = self._records[ticket.number]
            now = time.monotonic_ns()
            if state == 'RUNNING':
                if r.state != 'SUBMITTED' or os.getpid() != self._channel._actor[0]:
                    raise WorkerObservationRejected('worker_start_changed')
                r.thread = threading.current_thread()
                r.thread_id, r.native_thread_id = threading.get_ident(), threading.get_native_id()
                r.started_ns = now
            elif state in ('RETURNED', 'RAISED'):
                if (r.state != 'RUNNING' or r.thread is not threading.current_thread()
                        or r.thread_id != threading.get_ident() or now < r.started_ns):
                    raise WorkerObservationRejected('worker_exit_changed')
                r.finished_ns = now
            elif state in ('SUBMIT_FAILED', 'CANCELLED_BEFORE_START'):
                if r.state != 'SUBMITTED' or (os.getpid(), threading.get_ident()) != self._channel._actor:
                    raise WorkerObservationRejected('worker_submission_outcome_changed')
                r.finished_ns = now
            else:
                raise WorkerObservationRejected('worker_transition_invalid')
            r.state = state
            self._revision += 1
            if r.carrier_done and r.finished_ns is not None:
                del self._records[ticket.number]

    def release_carrier(self, observation, task):
        ticket = getattr(observation, '_worker_ticket', None)
        if ticket is None:
            return
        with self._lock:
            r = self._records[ticket.number]
            if (ticket.book is not self or r.binding is not observation.binding
                    or r.task is not task or not task.done()):
                raise WorkerObservationRejected('worker_carrier_changed')
            r.carrier_done, r.carrier_cancelled = True, task.cancelled()
            r.task = None
            self._revision += 1
            if r.finished_ns is not None:
                del self._records[ticket.number]

    def capture(self):
        self._actor()
        if not self._lock.acquire(blocking=False):
            raise WorkerObservationUnavailable('worker_snapshot_contended')
        try:
            values = []
            for number, r in self._records.items():
                if r.state == 'RUNNING' and not r.thread.is_alive():
                    self._gap()
                    raise WorkerObservationRejected('worker_thread_disappeared')
                done = r.carrier_done or r.task.done()
                cancelled = r.carrier_cancelled if r.carrier_done else r.task.cancelled()
                values.append(WorkerSnapshot(r.binding, number, r.state, self._channel._actor[0],
                    r.thread_id, r.native_thread_id, r.submitted_ns, r.started_ns, r.finished_ns,
                    done, cancelled, r.carrier_done))
            return self._revision, tuple(values)
        finally:
            self._lock.release()

# These source-only calls cannot let an observation error unwind business work.
def submitted(observation):
    if observation is None:
        return None
    try:
        return observation.channel._workers.submit(observation)
    except (Exception, asyncio.CancelledError):
        observation.channel._workers._gap()
        return None

def report(ticket, state):
    if ticket is None:
        return
    try:
        ticket.book.transition(ticket, state)
    except (Exception, asyncio.CancelledError):
        ticket.book._gap()

def carrier_done(observation, task):
    try:
        observation.channel._workers.release_carrier(observation, task)
    except (Exception, asyncio.CancelledError):
        observation.channel._workers._gap()
