package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;
import org.popcraft.bolt.access.AccessList;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SimpleProtectionCacheRedisTest {
    @Test
    void networkModeUsesSqlAuthorityAfterInvalidation() {
        final MemoryStore backing = new MemoryStore();
        final FakeCacheTransport redis = new FakeCacheTransport();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing, redis, "network", "server-a");
        final BlockLocation location = new BlockLocation("world", 4, 65, 9);
        final UUID id = UUID.randomUUID();
        final BlockProtection first = protection(id, "private");

        cache.saveBlockProtection(first);
        final BlockProtection updated = protection(id, "public");
        backing.saveBlockProtection(updated);
        redis.put(CacheKeys.block(location), new ProtectionCacheCodec().encode(updated), Duration.ofSeconds(30));
        redis.emit(new CacheInvalidationMessage("network", "server-b", "block", id.toString(), 2,
                CacheInvalidationMessage.Operation.INVALIDATE));

        assertEquals("public", cache.loadBlockProtection(location).join().getType());
    }

    @Test
    void standaloneModeCanHydrateFromRedisL2() {
        final MemoryStore backing = new MemoryStore();
        final FakeCacheTransport redis = new FakeCacheTransport();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing, redis, "default", "standalone");
        final BlockLocation location = new BlockLocation("world", 4, 65, 9);
        final BlockProtection cached = protection(UUID.randomUUID(), "public");

        redis.put(CacheKeys.block(location), new ProtectionCacheCodec().encode(cached), Duration.ofSeconds(30));

        assertEquals("public", cache.loadBlockProtection(location).join().getType());
    }

    @Test
    void collectionReadsReconcileProtectionsCreatedByAnotherServer() {
        final MemoryStore backing = new MemoryStore();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing, null, "network", "server-b");
        final BlockProtection protection = protection(UUID.randomUUID(), "private");

        backing.saveBlockProtection(protection);

        assertEquals(1, cache.loadBlockProtections().join().size());
        assertEquals(protection.getId(), cache.loadBlockProtection(new BlockLocation("world", 4, 65, 9)).join().getId());
    }

    @Test
    void singleReadsFallBackToBackingStoreWhenRedisIsDisabled() {
        final MemoryStore backing = new MemoryStore();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing);
        final UUID owner = UUID.randomUUID();
        final BlockProtection block = protection(UUID.randomUUID(), "private");
        final var entity = new org.popcraft.bolt.protection.EntityProtection(
                UUID.randomUUID(), owner, "private", 1, 1, new HashMap<>(), "ARMOR_STAND");
        final var group = new org.popcraft.bolt.util.Group("builders", owner, java.util.List.of(owner));
        final var accessList = new AccessList(owner, new HashMap<>());

        backing.saveBlockProtection(block);
        backing.saveEntityProtection(entity);
        backing.saveGroup(group);
        backing.saveAccessList(accessList);

        assertEquals(block.getId(), cache.loadBlockProtection(BlockLocation.fromProtection(block)).join().getId());
        assertEquals(entity.getId(), cache.loadEntityProtection(entity.getId()).join().getId());
        assertEquals(group.getName(), cache.loadGroup(group.getName()).join().getName());
        assertEquals(owner, cache.loadAccessList(owner).join().getOwner());
    }

    @Test
    void sharedHealthRefusesMutationsAfterRedisPublishFailure() {
        final MemoryStore backing = new MemoryStore();
        final FakeCacheTransport redis = new FakeCacheTransport();
        final SimpleProtectionCache cache = new SimpleProtectionCache(backing, redis, "network", "server-a");
        redis.failPublish = true;

        assertThrows(RuntimeException.class, () -> cache.publishOutboxEvent(OutboxEvent.pending(
                UUID.randomUUID(), "protection.updated", "block", "one", 1L, "{}", 1L)));
        assertThrows(IllegalStateException.class, () -> cache.saveBlockProtection(protection(UUID.randomUUID(), "private")));
    }

    private static BlockProtection protection(final UUID id, final String type) {
        return new BlockProtection(id, UUID.randomUUID(), type, 1, 1, new HashMap<>(), "world", 4, 65, 9, "CHEST");
    }

    private static final class FakeCacheTransport implements CacheTransport {
        private final Map<String, String> values = new HashMap<>();
        private Consumer<CacheInvalidationMessage> listener;
        private boolean failPublish;

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
            if (failPublish) {
                throw new IllegalStateException("redis unavailable");
            }
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
