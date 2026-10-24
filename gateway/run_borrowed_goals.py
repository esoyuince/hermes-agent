"""Compose native profile scope with the existing caller-owned goals state."""
from contextlib import contextmanager

@contextmanager
def execution_scope(authority, profile_scope):
    # Enter the native profile first: validation must see the owning home.
    with profile_scope:
        runner = authority.runner
        binding = getattr(runner, '_caller_owned_state', None)
        if binding is None:
            yield
            return
        from gateway.run_borrowed_state import CallerOwnedRunnerState, require
        from hermes_cli.goals_borrowed import session_db_scope
        require(type(binding) is CallerOwnedRunnerState, 'goals_borrowed_binding_required')
        def resolve():
            require(binding._authority is authority and authority.db is binding.state
                    and binding._stage in ('ACTIVE', 'DRAINING'),
                    'goals_borrowed_authority_changed')
            return binding.select_async(runner)._db
        resolve()
        with session_db_scope(resolve):
            yield
