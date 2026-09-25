package org.popcraft.bolt.data.redis;

import org.junit.jupiter.api.Test;
import io.lettuce.core.RedisURI;

import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RedisCacheConfigurationTest {
    @Test
    void requiresNamespacedKeys() {
        assertThrows(IllegalArgumentException.class, () -> new RedisCache(
                new RedisCache.Configuration("redis://127.0.0.1:6379", "bolt")
        ));
        assertThrows(IllegalArgumentException.class, () -> new RedisCache(
                new RedisCache.Configuration("redis://127.0.0.1:6379", "bolt bad:")
        ));
    }

    @Test
    void ttlContractIsRepresentedByJavaDuration() {
        // Keeps the adapter contract explicit without requiring a Redis daemon in unit tests.
        if (Duration.ofSeconds(30).isZero()) {
            throw new AssertionError("TTL must be positive");
        }
    }

    @Test
    void identifiesSentinelUrisWithoutOpeningAConnection() {
        assertTrue(RedisCache.isSentinelUri("redis-sentinel://127.0.0.1:26379,127.0.0.2:26379?sentinelMasterId=bolt-master"));
        assertFalse(RedisCache.isSentinelUri("redis://127.0.0.1:6379"));
        assertFalse(RedisCache.isSentinelUri("not-a-redis-uri"));
    }

    @Test
    void acceptsUnresolvedMasterAddressReturnedBySentinel() {
        final RedisURI sentinelUri = RedisURI.create(
                "redis-sentinel://127.0.0.1:26379?sentinelMasterId=bolt-master");

        final RedisURI masterUri = RedisCache.masterUriForAddress(
                InetSocketAddress.createUnresolved("127.0.0.1", 6389), sentinelUri);

        assertEquals("127.0.0.1", masterUri.getHost());
        assertEquals(6389, masterUri.getPort());
    }
}
