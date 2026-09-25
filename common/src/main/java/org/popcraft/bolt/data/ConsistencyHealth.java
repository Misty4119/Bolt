package org.popcraft.bolt.data;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe fail-closed health policy for shared protection data.
 * READ_ONLY diagnostics remain available while a shared transport recovers;
 * all authorization-affecting operations require HEALTHY.
 */
public final class ConsistencyHealth {
    private final AtomicReference<Snapshot> state;

    public ConsistencyHealth(final HealthState initial) {
        state = new AtomicReference<>(new Snapshot(initial == null ? HealthState.BLOCKED : initial, null, 0L));
    }

    public Snapshot snapshot() {
        return state.get();
    }

    public boolean allows(final SensitiveOperation operation) {
        if (operation == SensitiveOperation.READ_ONLY) {
            return true;
        }
        return state.get().state() == HealthState.HEALTHY;
    }

    public void markFailure(final String reason) {
        state.updateAndGet(previous -> new Snapshot(
                HealthState.DEGRADED,
                reason == null || reason.isBlank() ? "unknown failure" : reason,
                previous.lastSuccessfulProbeMillis()
        ));
    }

    public void markRecovering() {
        state.updateAndGet(previous -> new Snapshot(HealthState.RECOVERING, previous.lastError(), previous.lastSuccessfulProbeMillis()));
    }

    public void markHealthy(final long now) {
        state.set(new Snapshot(HealthState.HEALTHY, null, Math.max(0L, now)));
    }

    public void block(final String reason) {
        state.set(new Snapshot(HealthState.BLOCKED, reason, state.get().lastSuccessfulProbeMillis()));
    }

    public record Snapshot(HealthState state, String lastError, long lastSuccessfulProbeMillis) {
    }
}
