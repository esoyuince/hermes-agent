"""Runtime-status publication; borrowed adapters have no owned status writer."""
import time


def publish_runtime_status(adapter):
    # Counters still accumulate in memory. Caller-owned stores/profile leases do
    # not qualify the ordinary gateway_status file or its deferred base writer.
    if getattr(adapter, '_borrowed_api_stores', None) is not None:
        return
    adapter._metrics_last_heartbeat_at = time.time()
    adapter._write_runtime_status_safe(
        'api_server_heartbeat',
        platform_state='connected' if adapter.is_connected else 'disconnected',
        platform_metrics=adapter._api_server_status_payload(),
    )
