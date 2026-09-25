package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ConsistencyHealthTest {
    @Test
    void degradedHealthKeepsDiagnosticsButBlocksSensitiveOperations() {
        final ConsistencyHealth health = new ConsistencyHealth(HealthState.HEALTHY);

        health.markFailure("redis timeout");

        assertTrue(health.allows(SensitiveOperation.READ_ONLY));
        assertFalse(health.allows(SensitiveOperation.AUTHORIZATION));
        assertFalse(health.allows(SensitiveOperation.PORTAL));
        assertFalse(health.allows(SensitiveOperation.HOPPER));
    }

    @Test
    void recoveryRestoresSensitiveOperationsOnlyAfterHealthyProbe() {
        final ConsistencyHealth health = new ConsistencyHealth(HealthState.HEALTHY);
        health.markFailure("connection lost");
        health.markRecovering();

        assertFalse(health.allows(SensitiveOperation.AUTHORIZATION));

        health.markHealthy(100L);

        assertTrue(health.allows(SensitiveOperation.AUTHORIZATION));
        assertTrue(health.allows(SensitiveOperation.PROTECTION_MUTATION));
        assertTrue(health.snapshot().lastSuccessfulProbeMillis() == 100L);
    }
}
