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

public class SimpleProtectionCache implements Store, VersionedStore, AuditStore {
    private static final Logger LOGGER = Logger.getLogger(SimpleProtectionCache.class.getName());
    private static final Duration REDIS_TTL = Duration.ofSeconds(30);
    private final Map<BlockLocation, UUID> cachedBlockLocationId = new ConcurrentHashMap<>();
    private final Map<UUID, BlockLocation> cachedBlockIdLocation = new ConcurrentHashMap<>();
    private final Map<UUID, BlockProtection> cachedBlocks = new ConcurrentHashMap<>();
    private final Map<UUID, EntityProtection> cachedEntities = new ConcurrentHashMap<>();
    private final Map<String, Group> cachedGroups = new ConcurrentHashMap<>();
    private final Map<UUID, AccessList> cachedAccessLists = new ConcurrentHashMap<>();
    private final Map<String, Long> latestInvalidationVersions = new ConcurrentHashMap<>();
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
        if (backingStore instanceof VersionedStore versionedStore) {
            final BlockProtection previous = cachedBlocks.get(protection.getId());
            cacheBlock(protection);
            versionedStore.saveBlockProtectionVersioned(protection).whenComplete((version, exception) -> {
                if (exception != null) {
                    restoreBlock(previous, protection);
                    return;
                }
                publishAndCacheBlock(protection, version);
            });
            return;
        }
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
        if (backingStore instanceof VersionedStore versionedStore) {
            final BlockProtection previous = cachedBlocks.get(protection.getId());
            removeCachedBlock(protection);
            versionedStore.removeBlockProtectionVersioned(protection).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    restoreBlock(previous, protection);
                    return;
                }
                invalidateRedisSafely(CacheKeys.block(BlockLocation.fromProtection(protection)));
                invalidateRedisSafely(CacheKeys.block(protection.getId()));
                publish(CacheInvalidationMessage.Operation.DELETE, "block", protection.getId().toString(), protection.getVersion());
            });
            return;
        }
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
        if (backingStore instanceof VersionedStore versionedStore) {
            final EntityProtection previous = cachedEntities.put(protection.getId(), protection);
            versionedStore.saveEntityProtectionVersioned(protection).whenComplete((version, exception) -> {
                if (exception != null) {
                    restoreEntity(previous, protection);
                    return;
                }
                putRedisSafely(CacheKeys.entity(protection.getId()), codec.encode(protection));
                publish(CacheInvalidationMessage.Operation.UPSERT, "entity", protection.getId().toString(), version);
            });
            return;
        }
        cachedEntities.put(protection.getId(), protection);
        backingStore.saveEntityProtection(protection);
        afterSqlCommit(() -> {
            putRedisSafely(CacheKeys.entity(protection.getId()), codec.encode(protection));
            publish(CacheInvalidationMessage.Operation.UPSERT, "entity", protection.getId().toString());
        });
    }

    @Override
    public void removeEntityProtection(EntityProtection protection) {
        if (backingStore instanceof VersionedStore versionedStore) {
            final EntityProtection previous = cachedEntities.remove(protection.getId());
            versionedStore.removeEntityProtectionVersioned(protection).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    restoreEntity(previous, protection);
                    return;
                }
                invalidateRedisSafely(CacheKeys.entity(protection.getId()));
                publish(CacheInvalidationMessage.Operation.DELETE, "entity", protection.getId().toString(), protection.getVersion());
            });
            return;
        }
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
        if (backingStore instanceof VersionedStore versionedStore) {
            final Group previous = cachedGroups.put(group.getName(), group);
            versionedStore.saveGroupVersioned(group).whenComplete((version, exception) -> {
                if (exception != null) {
                    restoreGroup(previous, group);
                    return;
                }
                putRedisSafely("group:" + group.getName(), codec.encode(group));
                publish(CacheInvalidationMessage.Operation.UPSERT, "group", group.getName(), version);
            });
            return;
        }
        cachedGroups.put(group.getName(), group);
        backingStore.saveGroup(group);
        afterSqlCommit(() -> {
            putRedisSafely("group:" + group.getName(), codec.encode(group));
            publish(CacheInvalidationMessage.Operation.UPSERT, "group", group.getName());
        });
    }

    @Override
    public void removeGroup(Group group) {
        if (backingStore instanceof VersionedStore versionedStore) {
            final Group previous = cachedGroups.remove(group.getName());
            versionedStore.removeGroupVersioned(group).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    restoreGroup(previous, group);
                    return;
                }
                invalidateRedisSafely("group:" + group.getName());
                publish(CacheInvalidationMessage.Operation.DELETE, "group", group.getName(), group.getVersion());
            });
            return;
        }
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
        if (backingStore instanceof VersionedStore versionedStore) {
            final AccessList previous = cachedAccessLists.put(accessList.getOwner(), accessList);
            versionedStore.saveAccessListVersioned(accessList).whenComplete((version, exception) -> {
                if (exception != null) {
                    restoreAccessList(previous, accessList);
                    return;
                }
                putRedisSafely("access-list:" + accessList.getOwner(), codec.encode(accessList));
                publish(CacheInvalidationMessage.Operation.UPSERT, "access-list", accessList.getOwner().toString(), version);
            });
            return;
        }
        cachedAccessLists.put(accessList.getOwner(), accessList);
        backingStore.saveAccessList(accessList);
        afterSqlCommit(() -> {
            putRedisSafely("access-list:" + accessList.getOwner(), codec.encode(accessList));
            publish(CacheInvalidationMessage.Operation.UPSERT, "access-list", accessList.getOwner().toString());
        });
    }

    @Override
    public void removeAccessList(AccessList accessList) {
        if (backingStore instanceof VersionedStore versionedStore) {
            final AccessList previous = cachedAccessLists.remove(accessList.getOwner());
            versionedStore.removeAccessListVersioned(accessList).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    restoreAccessList(previous, accessList);
                    return;
                }
                invalidateRedisSafely("access-list:" + accessList.getOwner());
                publish(CacheInvalidationMessage.Operation.DELETE, "access-list", accessList.getOwner().toString(), accessList.getVersion());
            });
            return;
        }
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
    public void appendAuditEvent(final AuditEvent event) {
        if (backingStore instanceof AuditStore auditStore) {
            auditStore.appendAuditEvent(event);
        }
    }

    @Override
    public CompletableFuture<Collection<AuditEvent>> loadRecentAuditEvents(final UUID protectionId, final int limit) {
        if (backingStore instanceof AuditStore auditStore) {
            return auditStore.loadRecentAuditEvents(protectionId, limit);
        }
        return CompletableFuture.completedFuture(java.util.List.of());
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
        publish(operation, aggregateType, aggregateId, 1);
    }

    private void publish(final CacheInvalidationMessage.Operation operation, final String aggregateType, final String aggregateId, final long version) {
        if (redis != null) {
            redis.publish(CacheKeys.invalidationChannel(), new CacheInvalidationMessage(networkId, serverId, aggregateType, aggregateId, version, operation));
        }
    }

    private void handleInvalidation(final CacheInvalidationMessage message) {
        if (!networkId.equals(message.networkId()) || serverId.equals(message.serverId())) {
            return;
        }
        final String versionKey = message.aggregateType() + ':' + message.aggregateId();
        final long previousVersion = latestInvalidationVersions.getOrDefault(versionKey, -1L);
        if (message.version() <= previousVersion) {
            return;
        }
        latestInvalidationVersions.put(versionKey, message.version());
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

    @Override
    public CompletableFuture<Long> saveBlockProtectionVersioned(final BlockProtection protection) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveBlockProtectionVersioned(protection);
        }
        saveBlockProtection(protection);
        return CompletableFuture.completedFuture(protection.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeBlockProtectionVersioned(final BlockProtection protection) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeBlockProtectionVersioned(protection);
        }
        removeBlockProtection(protection);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveEntityProtectionVersioned(final EntityProtection protection) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveEntityProtectionVersioned(protection);
        }
        saveEntityProtection(protection);
        return CompletableFuture.completedFuture(protection.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeEntityProtectionVersioned(final EntityProtection protection) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeEntityProtectionVersioned(protection);
        }
        removeEntityProtection(protection);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveGroupVersioned(final Group group) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveGroupVersioned(group);
        }
        saveGroup(group);
        return CompletableFuture.completedFuture(group.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeGroupVersioned(final Group group) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeGroupVersioned(group);
        }
        removeGroup(group);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveAccessListVersioned(final AccessList accessList) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveAccessListVersioned(accessList);
        }
        saveAccessList(accessList);
        return CompletableFuture.completedFuture(accessList.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeAccessListVersioned(final AccessList accessList) {
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeAccessListVersioned(accessList);
        }
        removeAccessList(accessList);
        return CompletableFuture.completedFuture(true);
    }

    private void publishAndCacheBlock(final BlockProtection protection, final long version) {
        putRedisSafely(CacheKeys.block(BlockLocation.fromProtection(protection)), codec.encode(protection));
        putRedisSafely(CacheKeys.block(protection.getId()), codec.encode(protection));
        publish(CacheInvalidationMessage.Operation.UPSERT, "block", protection.getId().toString(), version);
    }

    private void removeCachedBlock(final BlockProtection protection) {
        final UUID id = protection.getId();
        final BlockLocation blockLocation = cachedBlockIdLocation.remove(id);
        cachedBlocks.remove(id);
        if (blockLocation != null) {
            cachedBlockLocationId.remove(blockLocation);
        } else {
            cachedBlockLocationId.remove(BlockLocation.fromProtection(protection));
        }
    }

    private void restoreBlock(final BlockProtection previous, final BlockProtection attempted) {
        removeCachedBlock(attempted);
        if (previous != null) {
            cacheBlock(previous);
        }
    }

    private void restoreEntity(final EntityProtection previous, final EntityProtection attempted) {
        if (previous == null) {
            cachedEntities.remove(attempted.getId());
        } else {
            cachedEntities.put(attempted.getId(), previous);
        }
    }

    private void restoreGroup(final Group previous, final Group attempted) {
        if (previous == null) {
            cachedGroups.remove(attempted.getName());
        } else {
            cachedGroups.put(attempted.getName(), previous);
        }
    }

    private void restoreAccessList(final AccessList previous, final AccessList attempted) {
        if (previous == null) {
            cachedAccessLists.remove(attempted.getOwner());
        } else {
            cachedAccessLists.put(attempted.getOwner(), previous);
        }
    }

    private static String requireIdentifier(final String value, final String field) {
        if (value == null || value.isBlank() || value.indexOf(':') >= 0 || value.indexOf(' ') >= 0) {
            throw new IllegalArgumentException(field + " must be a non-empty identifier");
        }
        return value;
    }
}
