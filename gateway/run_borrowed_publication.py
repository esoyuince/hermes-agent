"""Explicit publication in the existing native primary-adapter registry.

No alternate resolver, transport startup, recovery, voice callback or SQL effect.
The caller-owned object pair owns one registry entry until successful adapter
cleanup. All phase checks are cooperative in-process identity validation, not a
security boundary against arbitrary code changing Python objects.
"""
import asyncio
from gateway.config import Platform
from gateway.platforms.api_server_borrowed import require


def check_publication(bundle):
    """Pure identity check, safe inside the native SQL admission guard."""
    runner = bundle._prepared_runner
    require(runner is not None, 'publication_runner_missing')
    registry, profiles = runner.adapters, runner._profile_adapters
    require(type(registry) is dict and type(profiles) is dict and not profiles,
            'publication_registry_shape_or_foreign_profile')
    phase = bundle._publication_state
    if phase == 'NEW':
        require(not registry, 'publication_unowned_registry_entry')
        return
    require(phase in ('PUBLISHED', 'WITHDRAWN'), 'publication_outcome_unknown')
    require(registry is bundle._publication_registry and profiles is bundle._publication_profiles,
            'publication_registry_replaced')
    loop = bundle._publication_loop
    require(loop is not None and not loop.is_closed(), 'publication_loop_unavailable')
    adapter = bundle._adapter
    require(adapter.config is bundle._publication_config and adapter.platform is Platform.API_SERVER
            and adapter._owner_profile is None and adapter._shared_listener_profile is None,
            'publication_adapter_profile_or_config_changed')
    binding = bundle._prepared_binding
    require(binding._admission_loop is None or binding._admission_loop is loop,
            'publication_admission_loop_changed')
    if phase == 'PUBLISHED':
        require(len(registry) == 1 and next(iter(registry)) is Platform.API_SERVER
                and registry[Platform.API_SERVER] is bundle._adapter,
                'publication_adapter_identity_changed')
    else:
        require(not registry and bundle._stage in ('DRAINING', 'DETACHED')
                and binding._stage in ('PREPARED', 'DRAINED'),
                'publication_withdrawal_changed')


def require_owner_loop(bundle):
    """Lifecycle-only check, not called by the off-loop SQL admission guard."""
    if bundle._publication_state == 'PUBLISHED':
        require(asyncio.get_running_loop() is bundle._publication_loop,
                'publication_other_owner_loop')


def _unknown(bundle):
    # No rollback/registry pruning can prove an interrupted publication succeeded.
    bundle._publication_state = 'UNKNOWN'
    binding = bundle._prepared_binding
    binding._stage = 'ADMISSION_UNKNOWN'
    binding._authority.retiring = True
    bundle._prepared_runner._draining = True


def publish(bundle, runner, platform, adapter):
    from gateway.run_borrowed_admission import _pair, _idle_work, _idle_ledgers
    from gateway.platforms.api_server_prepared import check_prepared
    binding, owned_runner, owned_adapter, loop = _pair(bundle)
    require(runner is owned_runner and adapter is owned_adapter and platform is Platform.API_SERVER,
            'publication_foreign_owner_or_platform')
    require(binding._stage == 'PREPARED' and bundle._publication_state == 'NEW',
            'publication_not_fresh_prepared')
    check_prepared(bundle, adapter)
    require(adapter.platform is Platform.API_SERVER and adapter._owner_profile is None
            and adapter._shared_listener_profile is None, 'publication_primary_adapter_required')
    _idle_work(binding, runner, adapter)
    _idle_ledgers(bundle, binding)
    try:
        bundle._publication_registry = runner.adapters
        bundle._publication_profiles = runner._profile_adapters
        bundle._publication_loop = loop
        bundle._publication_config = adapter.config
        bundle._publication_state = 'PUBLISHING'
        runner.adapters[Platform.API_SERVER] = adapter
        bundle._publication_state = 'PUBLISHED'
        check_publication(bundle)
    except BaseException:
        _unknown(bundle)
        raise
    return {'state': 'CANONICAL_API_PUBLISHED', 'sameAdapter': True,
            'nativeRecoveryStarted': False, 'listenerAuthorized': False,
            'providerAuthorized': False, 'runtimeAdmissionAuthorized': False}


def withdraw(bundle):
    """Called after the native adapter cleanup, never before an awaited operation."""
    if bundle._prepared_runner is None:
        return
    check_publication(bundle)
    if bundle._publication_state == 'NEW':
        return
    binding, runner, adapter = bundle._prepared_binding, bundle._prepared_runner, bundle._adapter
    binding._lifecycle_actor()
    require(asyncio.get_running_loop() is bundle._publication_loop,
            'publication_other_cleanup_loop')
    require(bundle._stage == 'DRAINING' and binding._stage in ('PREPARED', 'DRAINED')
            and bundle._publication_state == 'PUBLISHED', 'publication_not_detachable')
    from gateway.run_borrowed_admission import _idle_work, _idle_ledgers
    _idle_work(binding, runner, adapter)
    _idle_ledgers(bundle, binding)
    try:
        bundle._publication_state = 'WITHDRAWING'
        del runner.adapters[Platform.API_SERVER]
        bundle._publication_state = 'WITHDRAWN'
        check_publication(bundle)
    except BaseException:
        _unknown(bundle)
        raise
