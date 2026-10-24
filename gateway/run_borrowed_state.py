"""Opt-in construction using caller-owned native state and an existing epoch.

This is a local, one-shot construction contract, not a listener/work capability.
It never recovers, advances epochs, opens or closes caller SQLite, or releases a
profile. Ordinary service startup/shutdown is deliberately not qualified here.
"""
from pathlib import Path
import os
import threading


class BorrowedRunnerRejected(RuntimeError):
    pass


def require(ok, reason):
    if not ok:
        raise BorrowedRunnerRejected(reason)


def refuse_ordinary_service(runner):
    if getattr(runner, '_caller_owned_state', None) is not None:
        raise BorrowedRunnerRejected('runner_borrow_service_lifecycle_unqualified')


class CallerOwnedRunnerState:
    """Caller retains state, epoch, profile and all-three-store lifecycle.

    The real GatewayRunner constructor consumes this reference exactly once.
    Construction failure leaves it consumed, never silently reusable. Native
    configuration loading remains real; no agent, adapter or service is started.
    """
    def __init__(self, config, state, *, epoch, instance_id):
        from gateway.config import GatewayConfig
        from gateway.session_borrowed_state import CallerOwnedSessionState
        require(type(config) is GatewayConfig, 'runner_borrow_explicit_config')
        require(type(epoch) is int and epoch > 0 and type(instance_id) is str
                and 0 < len(instance_id) <= 256, 'runner_borrow_epoch_identity')
        require(not config.platforms and not config.profile_routes,
                'runner_borrow_transport_configuration')
        self.config, self.state = config, state
        self.epoch, self.instance_id = epoch, instance_id
        self._expected_epoch = (epoch, instance_id)
        self._gate = CallerOwnedSessionState(config.sessions_dir, config, state)
        self.home = self._gate.home
        self._actor = (os.getpid(), threading.get_ident())
        self._runner = self._facade = self._wrapper = self._authority = None
        self._registry = self._descriptor = None
        self._local_epochs = self._api_bundle = None
        self._admission_loop = None
        self._stage = 'NEW'
        self._check_state(quiescent=True)

    def _lifecycle_actor(self):
        require((os.getpid(), threading.get_ident()) == self._actor,
                'runner_borrow_actor')

    def _check_state(self, *, quiescent=False):
        from gateway.session_borrowed_state import BorrowedSessionRejected
        import sqlite3
        require(os.getpid() == self._actor[0] and self._stage != 'DETACHED',
                'runner_borrow_detached_or_process')
        require((self.epoch, self.instance_id) == self._expected_epoch,
                'runner_borrow_expectation_changed')
        require(self._gate._state is self.state and self._gate.home == self.home
                and not self.config.platforms and not self.config.profile_routes,
                'runner_borrow_binding_changed')
        try:
            self._gate.select()
            if self._facade is not None:
                require(self._facade.config is self.config
                        and self._facade._borrowed_session_state._state is self.state,
                        'runner_borrow_facade_changed')
                require(self._facade._db is self.state, 'runner_borrow_facade_state')
            require(self.state._lock.acquire(blocking=False), 'runner_borrow_state_busy')
            try:
                conn = self.state._conn
                require(not conn.in_transaction, 'runner_borrow_transaction')
                rows = [tuple(r) for r in conn.execute(
                    'SELECT epoch,instance_id FROM main.runtime_epoch WHERE singleton=1')]
                require(rows == [self._expected_epoch], 'runner_borrow_epoch_changed')
                if quiescent:
                    for table in ('session_admissions', 'worker_executions'):
                        require(conn.execute('SELECT 1 FROM main.' + table +
                            " WHERE status!='terminal' LIMIT 1").fetchone() is None,
                            'runner_borrow_unresolved_work')
            finally:
                self.state._lock.release()
        except BorrowedRunnerRejected:
            raise
        except (BorrowedSessionRejected, sqlite3.Error, AttributeError, TypeError, ValueError):
            raise BorrowedRunnerRejected('runner_borrow_state_unavailable') from None
        if self._wrapper is not None:
            require(self._wrapper._db is self.state, 'runner_borrow_wrapper_changed')

    def claim(self, runner):
        self._lifecycle_actor()
        require(self._stage == 'NEW' and runner.config is self.config,
                'runner_borrow_already_claimed_or_config')
        self._check_state(quiescent=True)
        self._runner = runner
        self._stage = 'CONSTRUCTING'

    def state_for_constructor(self, runner):
        require(runner is self._runner and self._stage == 'CONSTRUCTING',
                'runner_borrow_construction_owner')
        self._check_state(quiescent=True)
        return self.state

    def attach_session_store(self, runner):
        from gateway.session import SessionStore
        require(runner is self._runner and self._facade is None
                and type(runner.session_store) is SessionStore, 'runner_borrow_session_store')
        self._facade = runner.session_store
        self._check_state(quiescent=True)

    def initialize_search(self, runner):
        from hermes_state import AsyncSessionDB
        require(runner is self._runner and self._wrapper is None, 'runner_borrow_search_repeated')
        self._check_state(quiescent=True)
        self._wrapper = AsyncSessionDB(self.state)

    def finish_construction(self, runner):
        require(runner is self._runner and self._stage == 'CONSTRUCTING'
                and self._wrapper is not None, 'runner_borrow_incomplete')
        self._check_state(quiescent=True)
        self._stage = 'CONSTRUCTED'

    def _check_prepared(self, runner):
        if self._authority is None:
            require(getattr(runner, 'session_authority', None) is None
                    and getattr(runner, 'session_authorities', None) is None,
                    'runner_borrow_unowned_authority')
            return
        try:
            a, registry, descriptor = self._authority, self._registry, self._descriptor
            phases = {'PREPARED': ('caller_prepared', True),
                      'ACTIVE': ('caller_active', False),
                      'DRAINING': ('caller_draining', True),
                      'DRAINED': ('caller_drained', True)}
            require(self._stage in phases, 'runner_borrow_phase_unknown')
            description, retiring = phases[self._stage]
            require(runner.session_authority is a and runner.session_authorities is registry
                    and registry.launch is a and registry.for_home(self.home) is a
                    and len(registry) == 1 and registry.multiplexed is False,
                    'runner_borrow_authority_registry_changed')
            require(a.runner is runner and a.db is self.state and a.epoch == self.epoch
                    and a.instance_id == self.instance_id and a.profile_id == str(self.home)
                    and a.retiring is retiring and a.retired is False,
                    'runner_borrow_authority_identity_changed')
            require(runner._running is False and runner._draining is retiring,
                    'runner_borrow_not_prepared_only')
            require(runner.session_runtime_descriptor is descriptor and type(descriptor) is dict
                    and type(descriptor.get('authority_epoch')) is int
                    and type(descriptor.get('runtime_protocol')) is int
                    and descriptor == {'instance_id': self.instance_id,
                        'authority_epoch': self.epoch, 'runtime_protocol': 1,
                        'state': description, 'capabilities': [], 'served_profiles': []},
                    'runner_borrow_descriptor_changed')
            require(self._facade._local_authority_epochs is self._local_epochs
                    and type(self._local_epochs) is dict
                    and self._local_epochs == {Path(self.state.db_path): self.epoch}
                    and all(type(v) is int for v in self._local_epochs.values())
                    and type(self._facade._local_authority_epoch) is int
                    and self._facade._local_authority_epoch == self.epoch,
                    'runner_borrow_local_epoch_mapping_changed')
        except (AttributeError, KeyError, TypeError):
            raise BorrowedRunnerRejected('runner_borrow_prepared_identity_unavailable') from None

    def select_async(self, runner):
        require(runner is self._runner and runner._caller_owned_state is self
                and runner.config is self.config and runner.session_store is self._facade
                and self._stage in ('CONSTRUCTED', 'PREPARED', 'ACTIVE', 'DRAINING', 'DRAINED'),
                'runner_borrow_owner_changed')
        self._check_state()
        self._check_prepared(runner)
        return self._wrapper

    def initialize_authority(self, runner):
        from gateway.session_authority import SessionAuthority
        from gateway.session_authorities import SessionAuthorities
        self._lifecycle_actor()
        self.select_async(runner)
        require(self._stage == 'CONSTRUCTED'
                and getattr(runner, 'session_authority', None) is None
                and getattr(runner, 'session_authorities', None) is None,
                'runner_borrow_authority_already_present')
        self._check_state(quiescent=True)
        authority = SessionAuthority(runner, profile_id=str(self.home),
            instance_id=self.instance_id, db=self.state, epoch=self.epoch)
        # No admission before shared stores/profile/mount/lifecycle are qualified.
        authority.retiring = True
        registry = SessionAuthorities(self.home, multiplexed=False)
        registry.add(self.home, authority)
        self._check_state(quiescent=True)
        require('_local_authority_epoch' not in self._facade.__dict__
                and '_local_authority_epochs' not in self._facade.__dict__,
                'runner_borrow_local_epoch_already_present')
        self._local_epochs = {Path(self.state.db_path): self.epoch}
        self._facade._local_authority_epoch = self.epoch
        self._facade._local_authority_epochs = self._local_epochs
        runner.session_authority, runner.session_authorities = authority, registry
        runner.session_runtime_descriptor = {'instance_id': self.instance_id,
            'authority_epoch': self.epoch, 'runtime_protocol': 1,
            'state': 'caller_prepared', 'capabilities': [], 'served_profiles': []}
        runner._draining = True
        self._authority, self._registry = authority, registry
        self._descriptor = runner.session_runtime_descriptor
        self._stage = 'PREPARED'
        return authority

    def detach(self, runner):
        self._lifecycle_actor()
        require(runner is self._runner, 'runner_borrow_detach_owner')
        if self._stage == 'DETACHED':
            return
        require(self._stage in ('CONSTRUCTED', 'PREPARED', 'DRAINED'),
                'runner_borrow_admission_must_drain')
        self.select_async(runner)
        self._check_state(quiescent=True)
        # This stage never started workers/transports. Unknown work is retained,
        # not cancelled/joined/pruned to manufacture a successful detach.
        for name in ('adapters', '_profile_adapters', '_background_tasks', '_sessions',
                     '_fatal_handler_tasks', '_completion_notification_batch_tasks',
                     '_completion_notification_batch_flush_tasks', '_session_db_handles',
                     '_startup_restore_tasks', '_startup_restore_queue', '_agent_cache'):
            require(not getattr(runner, name), 'runner_borrow_work_or_cache_present')
        require(not runner._running and runner._executor is None
                and runner._housekeeping_executor is None
                and runner._loop_heartbeat_task is None and runner._stop_task is None,
                'runner_borrow_lifecycle_active')
        if self._authority is not None:
            a = self._authority
            require(runner.session_authority is a and runner.session_authorities is self._registry
                    and self._registry.launch is a and a.db is self.state
                    and a.epoch == self.epoch and a.retiring is True,
                    'runner_borrow_authority_changed')
            for name in ('sessions', 'waiters', 'events', 'native_waiters', 'pending_results',
                         'pending_deliveries', 'adopted', 'executing', '_turn_workers',
                         '_managed_workers', '_mutation_tasks', 'pending_stops', 'pending_steers',
                         'cancel_obligations'):
                require(not getattr(a, name, None), 'runner_borrow_authority_busy')
        if self._api_bundle is not None:
            from gateway.platforms.api_server_prepared import require_adapter_detached
            require_adapter_detached(self._api_bundle, runner, self)
        try:
            self._facade.close_all_db_handles()
            if self._authority is not None:
                self._authority.retired = True
                self._descriptor['state'] = 'caller_detached'
            self._stage = 'DETACHED'
        except BaseException:
            if self._facade._borrowed_session_state._closed:
                self._stage = 'DETACH_UNKNOWN'
            raise

    def status(self):
        self._lifecycle_actor()
        return {'state': self._stage, 'callerOwnsState': True,
            'existingEpochReused': self._stage == 'PREPARED',
            'storesClosedHere': False, 'profileReleasedHere': False,
            'serviceLifecycleQualified': False, 'runtimeAdmissionAuthorized': False,
            'allThreeStoresIntegrated': False}
