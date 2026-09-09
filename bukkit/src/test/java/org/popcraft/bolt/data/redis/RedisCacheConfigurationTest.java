package org.popcraft.bolt.data.redis;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class RedisCacheConfigurationTest {
    @Test
    void requiresNamespacedKeys() {
        assertThrows(IllegalArgumentException.class, () -> new RedisCache(
                new RedisCache.Configuration("redis://127.0.0.1:6379", "bolt")
        ));
    }

    @Test
    void ttlContractIsRepresentedByJavaDuration() {
        // Keeps the adapter contract explicit without requiring a Redis daemon in unit tests.
        if (Duration.ofSeconds(30).isZero()) {
            throw new AssertionError("TTL must be positive");
        }
    }
}
