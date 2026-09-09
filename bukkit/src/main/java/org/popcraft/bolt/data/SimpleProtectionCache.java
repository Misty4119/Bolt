package org.popcraft.bolt.data;

import org.popcraft.bolt.access.AccessList;
import org.popcraft.bolt.data.redis.CacheInvalidationMessage;
import org.popcraft.bolt.data.redis.CacheKeys;
import org.popcraft.bolt.data.redis.CacheTransport;
import org.popcraft.bolt.data.redis.ProtectionCacheCodec;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.protection.EntityProtection;
import org.popcraft.bolt.util.BlockLocation;
import org.popcraft.bolt.util.Group;
import org.popcraft.bolt.util.Metrics;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleProtectionCache implements Store {
    private static final Logger LOGGER = Logger.getLogger(SimpleProtectionCache.class.getName());
    private static final Duration REDIS_TTL = Duration.ofSeconds(30);
    private final Map<BlockLocation, UUID> cachedBlockLocationId = new ConcurrentHashMap<>();
    private final Map<UUID, BlockLocation> cachedBlockIdLocation = new ConcurrentHashMap<>();
    private final Map<UUID, BlockProtection> cachedBlocks = new ConcurrentHashMap<>();
    private final Map<UUID, EntityProtection> cachedEntities = new ConcurrentHashMap<>();
    private final Map<String, Group> cachedGroups = new ConcurrentHashMap<>();
    private final Map<UUID, AccessList> cachedAccessLists = new ConcurrentHashMap<>();
    private final Store backingStore;
    private final CacheTransport redis;
    private final ProtectionCacheCodec codec;
    private final String networkId;
    private final String serverId;

    public SimpleProtectionCache(final Store backingStore) {
        this(backingStore, null, "default", "standalone");
    }

    public SimpleProtectionCache(final Store backingStore, final CacheTransport redis,
                                  final String networkId, final String serverId) {
        this.backingStore = backingStore;
        this.redis = redis;
        this.codec = new ProtectionCacheCodec();
        this.networkId = requireIdentifier(networkId, "networkId");
        this.serverId = requireIdentifier(serverId, "serverId");
        backingStore.loadBlockProtections().join().forEach(blockProtection -> {
            cacheBlock(blockProtection);
        });
        backingStore.loadEntityProtections().join().forEach(entityProtection -> cachedEntities.putIfAbsent(entityProtection.getId(), entityProtection));
        backingStore.loadGroups().join().forEach(group -> cachedGroups.putIfAbsent(group.getName(), group));
        backingStore.loadAccessLists().join().forEach(accessList -> cachedAccessLists.putIfAbsent(accessList.getOwner(), accessList));
        if (redis != null) {
            redis.subscribe(CacheKeys.invalidationChannel(), this::handleInvalidation);
        }
    }

    @Override
    public CompletableFuture<BlockProtection> loadBlockProtection(BlockLocation location) {
        final UUID id = cachedBlockLocationId.get(location);
        final BlockProtection blockProtection = id == null ? null : cachedBlocks.get(id);
        if (blockProtection != null || redis == null) {
            Metrics.recordProtectionAccess(blockProtection != null);
            return CompletableFuture.completedFuture(blockProtection);
        }
        return CompletableFuture.supplyAsync(() -> readRedisBlock(location))
                .thenCompose(cached -> cached != null ? CompletableFuture.completedFuture(cached) : backingStore.loadBlockProtection(location))
                .thenApply(loaded -> {
                    if (loaded != null) {
                        cacheBlock(loaded);
                        putRedisSafely(CacheKeys.block(location), codec.encode(loaded));
                    }
                    Metrics.recordProtectionAccess(loaded != null);
                    return loaded;
                });
    }

    @Override
    public CompletableFuture<Collection<BlockProtection>> loadBlockProtections() {
        return CompletableFuture.completedFuture(cachedBlocks.values());
    }

    @Override
    public void saveBlockProtection(BlockProtection protection) {
        cacheBlock(protection);
        backingStore.saveBlockProtection(protection);
        afterSqlCommit(() -> {
            final BlockLocation location = BlockLocation.fromProtection(protection);
            putRedisSafely(CacheKeys.block(location), codec.encode(protection));
            putRedisSafely(CacheKeys.block(protection.getId()), codec.encode(protection));
            publish(CacheInvalidationMessage.Operation.UPSERT, "block", protection.getId().toString());
        });
    }

    @Override
    public void removeBlockProtection(BlockProtection protection) {
        final UUID id = protection.getId();
        final BlockLocation blockLocation = BlockLocation.fromProtection(protection);
        cachedBlockLocationId.remove(blockLocation);
        cachedBlockIdLocation.remove(id);
        cachedBlocks.remove(id);
        backingStore.removeBlockProtection(protection);
        afterSqlCommit(() -> {
            invalidateRedisSafely(CacheKeys.block(blockLocation));
            invalidateRedisSafely(CacheKeys.block(id));
            publish(CacheInvalidationMessage.Operation.DELETE, "block", id.toString());
        });
    }

    @Override
    public CompletableFuture<EntityProtection> loadEntityProtection(UUID id) {
        final EntityProtection entityProtection = cachedEntities.get(id);
        Metrics.recordProtectionAccess(entityProtection != null);
        return CompletableFuture.completedFuture(entityProtection);
    }

    @Override
    public CompletableFuture<Collection<EntityProtection>> loadEntityProtections() {
        return CompletableFuture.completedFuture(cachedEntities.values());
    }

    @Override
    public void saveEntityProtection(EntityProtection protection) {
        cachedEntities.put(protection.getId(), protection);
        backingStore.saveEntityProtection(protection);
        afterSqlCommit(() -> {
            putRedisSafely(CacheKeys.entity(protection.getId()), codec.encode(protection));
            publish(CacheInvalidationMessage.Operation.UPSERT, "entity", protection.getId().toString());
        });
    }

    @Override
    public void removeEntityProtection(EntityProtection protection) {
        cachedEntities.remove(protection.getId());
        backingStore.removeEntityProtection(protection);
        afterSqlCommit(() -> {
            invalidateRedisSafely(CacheKeys.entity(protection.getId()));
            publish(CacheInvalidationMessage.Operation.DELETE, "entity", protection.getId().toString());
        });
    }

    @Override
    public CompletableFuture<Group> loadGroup(String group) {
        return CompletableFuture.completedFuture(cachedGroups.get(group));
    }

    @Override
    public CompletableFuture<Collection<Group>> loadGroups() {
        return CompletableFuture.completedFuture(cachedGroups.values());
    }

    @Override
    public void saveGroup(Group group) {
        cachedGroups.put(group.getName(), group);
        backingStore.saveGroup(group);
        afterSqlCommit(() -> {
            putRedisSafely("group:" + group.getName(), codec.encode(group));
            publish(CacheInvalidationMessage.Operation.UPSERT, "group", group.getName());
        });
    }

    @Override
    public void removeGroup(Group group) {
        cachedGroups.remove(group.getName());
        backingStore.removeGroup(group);
        afterSqlCommit(() -> {
            invalidateRedisSafely("group:" + group.getName());
            publish(CacheInvalidationMessage.Operation.DELETE, "group", group.getName());
        });
    }

    @Override
    public CompletableFuture<AccessList> loadAccessList(UUID owner) {
        return CompletableFuture.completedFuture(cachedAccessLists.get(owner));
    }

    @Override
    public CompletableFuture<Collection<AccessList>> loadAccessLists() {
        return CompletableFuture.completedFuture(cachedAccessLists.values());
    }

    @Override
    public void saveAccessList(AccessList accessList) {
        cachedAccessLists.put(accessList.getOwner(), accessList);
        backingStore.saveAccessList(accessList);
        afterSqlCommit(() -> {
            putRedisSafely("access-list:" + accessList.getOwner(), codec.encode(accessList));
            publish(CacheInvalidationMessage.Operation.UPSERT, "access-list", accessList.getOwner().toString());
        });
    }

    @Override
    public void removeAccessList(AccessList accessList) {
        cachedAccessLists.remove(accessList.getOwner());
        backingStore.removeAccessList(accessList);
        afterSqlCommit(() -> {
            invalidateRedisSafely("access-list:" + accessList.getOwner());
            publish(CacheInvalidationMessage.Operation.DELETE, "access-list", accessList.getOwner().toString());
        });
    }

    @Override
    public long pendingSave() {
        return backingStore.pendingSave();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return backingStore.flush();
    }

    private void cacheBlock(final BlockProtection protection) {
        final UUID id = protection.getId();
        final BlockLocation oldBlockLocation = cachedBlockIdLocation.remove(id);
        if (oldBlockLocation != null) {
            cachedBlockLocationId.remove(oldBlockLocation);
        }
        final BlockLocation blockLocation = BlockLocation.fromProtection(protection);
        cachedBlockLocationId.put(blockLocation, id);
        cachedBlockIdLocation.put(id, blockLocation);
        cachedBlocks.put(id, protection);
    }

    private BlockProtection readRedisBlock(final BlockLocation location) {
        try {
            final String payload = redis.get(CacheKeys.block(location));
            return payload == null ? null : codec.decodeBlock(payload);
        } catch (RuntimeException exception) {
            LOGGER.log(Level.FINE, "Redis protection read failed; falling back to SQL", exception);
            return null;
        }
    }

    private void afterSqlCommit(final Runnable action) {
        backingStore.flush().whenComplete((ignored, exception) -> {
            if (exception != null) {
                LOGGER.log(Level.SEVERE, "SQL commit could not be confirmed; Redis cache was not updated", exception);
                return;
            }
            try {
                action.run();
            } catch (RuntimeException redisException) {
                LOGGER.log(Level.WARNING, "Redis cache update failed after SQL commit", redisException);
            }
        });
    }

    private void putRedisSafely(final String key, final String payload) {
        if (redis == null) {
            return;
        }
        redis.put(key, payload, REDIS_TTL);
    }

    private void invalidateRedisSafely(final String key) {
        if (redis != null) {
            redis.invalidate(key);
        }
    }

    private void publish(final CacheInvalidationMessage.Operation operation, final String aggregateType, final String aggregateId) {
        if (redis != null) {
            redis.publish(CacheKeys.invalidationChannel(), new CacheInvalidationMessage(networkId, serverId, aggregateType, aggregateId, 1, operation));
        }
    }

    private void handleInvalidation(final CacheInvalidationMessage message) {
        if (!networkId.equals(message.networkId()) || serverId.equals(message.serverId())) {
            return;
        }
        try {
            switch (message.aggregateType()) {
                case "block" -> invalidateBlock(UUID.fromString(message.aggregateId()), message.operation());
                case "entity" -> invalidateEntity(UUID.fromString(message.aggregateId()), message.operation());
                case "group" -> {
                    if (message.operation() == CacheInvalidationMessage.Operation.DELETE) {
                        cachedGroups.remove(message.aggregateId());
                    } else {
                        cachedGroups.remove(message.aggregateId());
                    }
                }
                case "access-list" -> cachedAccessLists.remove(UUID.fromString(message.aggregateId()));
                default -> LOGGER.fine("Ignoring unknown Bolt cache aggregate: " + message.aggregateType());
            }
        } catch (RuntimeException exception) {
            LOGGER.log(Level.WARNING, "Ignoring malformed Bolt cache invalidation", exception);
        }
    }

    private void invalidateBlock(final UUID id, final CacheInvalidationMessage.Operation operation) {
        final BlockLocation location = cachedBlockIdLocation.get(id);
        if (operation == CacheInvalidationMessage.Operation.DELETE) {
            cachedBlocks.remove(id);
            cachedBlockIdLocation.remove(id);
            if (location != null) {
                cachedBlockLocationId.remove(location);
            }
        } else {
            // Retain the location index so a subsequent read can refill from L2/SQL.
            cachedBlocks.remove(id);
        }
    }

    private void invalidateEntity(final UUID id, final CacheInvalidationMessage.Operation operation) {
        cachedEntities.remove(id);
    }

    private static String requireIdentifier(final String value, final String field) {
        if (value == null || value.isBlank() || value.indexOf(':') >= 0 || value.indexOf(' ') >= 0) {
            throw new IllegalArgumentException(field + " must be a non-empty identifier");
        }
        return value;
    }
}
