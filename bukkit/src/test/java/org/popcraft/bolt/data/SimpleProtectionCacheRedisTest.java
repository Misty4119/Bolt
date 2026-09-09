package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;
import org.popcraft.bolt.data.redis.CacheInvalidationMessage;
import org.popcraft.bolt.data.redis.CacheKeys;
import org.popcraft.bolt.data.redis.CacheTransport;
import org.popcraft.bolt.data.redis.ProtectionCacheCodec;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.util.BlockLocation;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SimpleProtectionCacheRedisTest {
    @Test
    void invalidationRefillsL1FromRedisWithoutLosingLocationIndex() {
        final MemoryStore backing = new MemoryStore();
        final FakeCacheTransport redis = new FakeCacheTransport();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing, redis, "network", "server-a");
        final BlockLocation location = new BlockLocation("world", 4, 65, 9);
        final UUID id = UUID.randomUUID();
        final BlockProtection first = protection(id, "private");

        cache.saveBlockProtection(first);
        final BlockProtection updated = protection(id, "public");
        redis.put(CacheKeys.block(location), new ProtectionCacheCodec().encode(updated), Duration.ofSeconds(30));
        redis.emit(new CacheInvalidationMessage("network", "server-b", "block", id.toString(), 2,
                CacheInvalidationMessage.Operation.INVALIDATE));

        assertEquals("public", cache.loadBlockProtection(location).join().getType());
    }

    private static BlockProtection protection(final UUID id, final String type) {
        return new BlockProtection(id, UUID.randomUUID(), type, 1, 1, new HashMap<>(), "world", 4, 65, 9, "CHEST");
    }

    private static final class FakeCacheTransport implements CacheTransport {
        private final Map<String, String> values = new HashMap<>();
        private Consumer<CacheInvalidationMessage> listener;

        @Override
        public String get(final String key) {
            return values.get(key);
        }

        @Override
        public void put(final String key, final String value, final Duration ttl) {
            values.put(key, value);
        }

        @Override
        public void invalidate(final String key) {
            values.remove(key);
        }

        @Override
        public void publish(final String channel, final CacheInvalidationMessage message) {
            if (listener != null) {
                listener.accept(message);
            }
        }

        @Override
        public void subscribe(final String channel, final Consumer<CacheInvalidationMessage> consumer) {
            listener = consumer;
        }

        @Override
        public boolean ping() {
            return true;
        }

        @Override
        public void close() {
        }

        private void emit(final CacheInvalidationMessage message) {
            listener.accept(message);
        }
    }
}
