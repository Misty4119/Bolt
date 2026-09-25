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
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleProtectionCache implements Store, VersionedStore, AuditStore, OutboxStore, WorldRegistryStore, WatermarkStore, BulkBlockLookupStore, ConsistencyAwareStore, AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(SimpleProtectionCache.class.getName());
    private static final Duration REDIS_TTL = Duration.ofSeconds(30);
    private static final int MAX_INVALIDATION_VERSION_ENTRIES = 100_000;
    private static final int INVALIDATION_VERSION_PRUNE_BATCH = 10_000;
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
    private final boolean strictConsistency;
    private final ConsistencyHealth consistencyHealth;
    private final AtomicBoolean redisHealthy;
    private final AtomicBoolean sqlHealthy = new AtomicBoolean(true);
    private final AtomicReference<String> redisFailure = new AtomicReference<>();
    private final AtomicReference<String> sqlFailure = new AtomicReference<>();
    private final ScheduledExecutorService healthScheduler;

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
        this.strictConsistency = !"default".equals(this.networkId) || !"standalone".equals(this.serverId);
        this.redisHealthy = new AtomicBoolean(redis != null || !strictConsistency);
        this.consistencyHealth = new ConsistencyHealth(redis == null && strictConsistency ? HealthState.BLOCKED : HealthState.HEALTHY);
        this.healthScheduler = redis == null ? null : Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "Bolt-Redis-Health");
            thread.setDaemon(true);
            return thread;
        });
        backingStore.loadBlockProtections().join().forEach(blockProtection -> {
            cacheBlock(blockProtection);
        });
        backingStore.loadEntityProtections().join().forEach(entityProtection -> cachedEntities.putIfAbsent(entityProtection.getId(), entityProtection));
        backingStore.loadGroups().join().forEach(group -> cachedGroups.putIfAbsent(group.getName(), group));
        backingStore.loadAccessLists().join().forEach(accessList -> cachedAccessLists.putIfAbsent(accessList.getOwner(), accessList));
        if (redis != null) {
            redis.subscribe(CacheKeys.invalidationChannel(), this::handleInvalidation);
            healthScheduler.scheduleWithFixedDelay(this::probeRedisHealth, 5L, 5L, TimeUnit.SECONDS);
        }
    }

    @Override
    public CompletableFuture<BlockProtection> loadBlockProtection(BlockLocation location) {
        final UUID id = cachedBlockLocationId.get(location);
        final BlockProtection blockProtection = id == null ? null : cachedBlocks.get(id);
        if (blockProtection != null && !strictConsistency) {
            Metrics.recordProtectionAccess(blockProtection != null);
            return CompletableFuture.completedFuture(blockProtection);
        }
        if (redis == null || strictConsistency) {
            return observeSql(backingStore.loadBlockProtection(location).thenApply(loaded -> {
                if (loaded != null) {
                    cacheBlock(loaded);
                }
                Metrics.recordProtectionAccess(loaded != null);
                return loaded;
            }));
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
    public CompletableFuture<Collection<BlockProtection>> loadBlockProtections(final Collection<BlockLocation> locations) {
        if (backingStore instanceof BulkBlockLookupStore bulkBlockLookupStore) {
            return observeSql(bulkBlockLookupStore.loadBlockProtections(locations).thenApply(loaded -> {
                loaded.forEach(this::cacheBlock);
                return loaded;
            }));
        }
        if (locations == null || locations.isEmpty()) {
            return CompletableFuture.completedFuture(java.util.List.of());
        }
        final Collection<CompletableFuture<BlockProtection>> futures = locations.stream()
                .map(backingStore::loadBlockProtection)
                .toList();
        return observeSql(CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .filter(java.util.Objects::nonNull)
                        .toList()));
    }

    @Override
    public CompletableFuture<Collection<BlockProtection>> loadBlockProtections() {
        return observeSql(reconcile(backingStore.loadBlockProtections(), loaded -> {
            loaded.forEach(this::cacheBlock);
            final java.util.Set<UUID> loadedIds = loaded.stream()
                    .map(BlockProtection::getId)
                    .collect(java.util.stream.Collectors.toSet());
            cachedBlocks.keySet().removeIf(id -> !loadedIds.contains(id));
            cachedBlockIdLocation.entrySet().removeIf(entry -> !loadedIds.contains(entry.getValue()));
            return java.util.List.copyOf(loaded);
        }));
    }

    @Override
    public void saveBlockProtection(BlockProtection protection) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final BlockProtection previous = cachedBlocks.get(protection.getId());
            cacheBlock(protection);
            versionedStore.saveBlockProtectionVersioned(protection).whenComplete((version, exception) -> {
                if (exception != null) {
                    markSqlFailure(exception);
                    restoreBlock(previous, protection);
                    return;
                }
                markSqlHealthy();
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
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final BlockProtection previous = cachedBlocks.get(protection.getId());
            removeCachedBlock(protection);
            versionedStore.removeBlockProtectionVersioned(protection).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    if (exception != null) {
                        markSqlFailure(exception);
                    }
                    restoreBlock(previous, protection);
                    return;
                }
                markSqlHealthy();
                invalidateRedisSafely(CacheKeys.block(BlockLocation.fromProtection(protection)));
                invalidateRedisSafely(CacheKeys.block(protection.getId()));
                publish(CacheInvalidationMessage.Operation.DELETE, "block", protection.getId().toString(), nextMutationVersion(protection.getVersion()));
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
        if (entityProtection != null && !strictConsistency) {
            Metrics.recordProtectionAccess(entityProtection != null);
            return CompletableFuture.completedFuture(entityProtection);
        }
        final CompletableFuture<EntityProtection> result = redis == null || strictConsistency
                ? backingStore.loadEntityProtection(id)
                : CompletableFuture.supplyAsync(() -> readRedisEntity(id))
                .thenCompose(cached -> cached != null
                        ? CompletableFuture.completedFuture(cached)
                        : backingStore.loadEntityProtection(id));
        return observeSql(result)
                .thenApply(loaded -> {
                    if (loaded != null) {
                        cachedEntities.put(loaded.getId(), loaded);
                        putRedisSafely(CacheKeys.entity(loaded.getId()), codec.encode(loaded));
                    }
                    Metrics.recordProtectionAccess(loaded != null);
                    return loaded;
                });
    }

    @Override
    public CompletableFuture<Collection<EntityProtection>> loadEntityProtections() {
        return reconcile(backingStore.loadEntityProtections(), loaded -> {
            loaded.forEach(protection -> cachedEntities.put(protection.getId(), protection));
            final java.util.Set<UUID> loadedIds = loaded.stream()
                    .map(EntityProtection::getId)
                    .collect(java.util.stream.Collectors.toSet());
            cachedEntities.keySet().removeIf(id -> !loadedIds.contains(id));
            return java.util.List.copyOf(loaded);
        });
    }

    @Override
    public void saveEntityProtection(EntityProtection protection) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final EntityProtection previous = cachedEntities.put(protection.getId(), protection);
            versionedStore.saveEntityProtectionVersioned(protection).whenComplete((version, exception) -> {
                if (exception != null) {
                    markSqlFailure(exception);
                    restoreEntity(previous, protection);
                    return;
                }
                markSqlHealthy();
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
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final EntityProtection previous = cachedEntities.remove(protection.getId());
            versionedStore.removeEntityProtectionVersioned(protection).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    if (exception != null) {
                        markSqlFailure(exception);
                    }
                    restoreEntity(previous, protection);
                    return;
                }
                markSqlHealthy();
                invalidateRedisSafely(CacheKeys.entity(protection.getId()));
                publish(CacheInvalidationMessage.Operation.DELETE, "entity", protection.getId().toString(), nextMutationVersion(protection.getVersion()));
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
        final Group cached = cachedGroups.get(group);
        if (cached != null && !strictConsistency) {
            return CompletableFuture.completedFuture(cached);
        }
        final CompletableFuture<Group> result = redis == null || strictConsistency
                ? backingStore.loadGroup(group)
                : CompletableFuture.supplyAsync(() -> readRedisGroup(group))
                .thenCompose(value -> value != null
                        ? CompletableFuture.completedFuture(value)
                        : backingStore.loadGroup(group));
        return observeSql(result)
                .thenApply(loaded -> {
                    if (loaded != null) {
                        cachedGroups.put(loaded.getName(), loaded);
                        putRedisSafely(CacheKeys.group(loaded.getName()), codec.encode(loaded));
                    }
                    return loaded;
                });
    }

    @Override
    public CompletableFuture<Collection<Group>> loadGroups() {
        return reconcile(backingStore.loadGroups(), loaded -> {
            loaded.forEach(group -> cachedGroups.put(group.getName(), group));
            final java.util.Set<String> loadedNames = loaded.stream()
                    .map(Group::getName)
                    .collect(java.util.stream.Collectors.toSet());
            cachedGroups.keySet().removeIf(name -> !loadedNames.contains(name));
            return java.util.List.copyOf(loaded);
        });
    }

    @Override
    public void saveGroup(Group group) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final Group previous = cachedGroups.put(group.getName(), group);
            versionedStore.saveGroupVersioned(group).whenComplete((version, exception) -> {
                if (exception != null) {
                    markSqlFailure(exception);
                    restoreGroup(previous, group);
                    return;
                }
                markSqlHealthy();
                putRedisSafely(CacheKeys.group(group.getName()), codec.encode(group));
                publish(CacheInvalidationMessage.Operation.UPSERT, "group", group.getName(), version);
            });
            return;
        }
        cachedGroups.put(group.getName(), group);
        backingStore.saveGroup(group);
        afterSqlCommit(() -> {
            putRedisSafely(CacheKeys.group(group.getName()), codec.encode(group));
            publish(CacheInvalidationMessage.Operation.UPSERT, "group", group.getName());
        });
    }

    @Override
    public void removeGroup(Group group) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final Group previous = cachedGroups.remove(group.getName());
            versionedStore.removeGroupVersioned(group).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    if (exception != null) {
                        markSqlFailure(exception);
                    }
                    restoreGroup(previous, group);
                    return;
                }
                markSqlHealthy();
                invalidateRedisSafely(CacheKeys.group(group.getName()));
                publish(CacheInvalidationMessage.Operation.DELETE, "group", group.getName(), nextMutationVersion(group.getVersion()));
            });
            return;
        }
        cachedGroups.remove(group.getName());
        backingStore.removeGroup(group);
        afterSqlCommit(() -> {
            invalidateRedisSafely(CacheKeys.group(group.getName()));
            publish(CacheInvalidationMessage.Operation.DELETE, "group", group.getName());
        });
    }

    @Override
    public CompletableFuture<AccessList> loadAccessList(UUID owner) {
        final AccessList cached = cachedAccessLists.get(owner);
        if (cached != null && !strictConsistency) {
            return CompletableFuture.completedFuture(cached);
        }
        final CompletableFuture<AccessList> result = redis == null || strictConsistency
                ? backingStore.loadAccessList(owner)
                : CompletableFuture.supplyAsync(() -> readRedisAccessList(owner))
                .thenCompose(value -> value != null
                        ? CompletableFuture.completedFuture(value)
                        : backingStore.loadAccessList(owner));
        return observeSql(result)
                .thenApply(loaded -> {
                    if (loaded != null) {
                        cachedAccessLists.put(loaded.getOwner(), loaded);
                        putRedisSafely(CacheKeys.accessList(loaded.getOwner()), codec.encode(loaded));
                    }
                    return loaded;
                });
    }

    @Override
    public CompletableFuture<Collection<AccessList>> loadAccessLists() {
        return reconcile(backingStore.loadAccessLists(), loaded -> {
            loaded.forEach(accessList -> cachedAccessLists.put(accessList.getOwner(), accessList));
            final java.util.Set<UUID> loadedOwners = loaded.stream()
                    .map(AccessList::getOwner)
                    .collect(java.util.stream.Collectors.toSet());
            cachedAccessLists.keySet().removeIf(owner -> !loadedOwners.contains(owner));
            return java.util.List.copyOf(loaded);
        });
    }

    @Override
    public void saveAccessList(AccessList accessList) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final AccessList previous = cachedAccessLists.put(accessList.getOwner(), accessList);
            versionedStore.saveAccessListVersioned(accessList).whenComplete((version, exception) -> {
                if (exception != null) {
                    markSqlFailure(exception);
                    restoreAccessList(previous, accessList);
                    return;
                }
                markSqlHealthy();
                putRedisSafely(CacheKeys.accessList(accessList.getOwner()), codec.encode(accessList));
                publish(CacheInvalidationMessage.Operation.UPSERT, "access-list", accessList.getOwner().toString(), version);
            });
            return;
        }
        cachedAccessLists.put(accessList.getOwner(), accessList);
        backingStore.saveAccessList(accessList);
        afterSqlCommit(() -> {
            putRedisSafely(CacheKeys.accessList(accessList.getOwner()), codec.encode(accessList));
            publish(CacheInvalidationMessage.Operation.UPSERT, "access-list", accessList.getOwner().toString());
        });
    }

    @Override
    public void removeAccessList(AccessList accessList) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            final AccessList previous = cachedAccessLists.remove(accessList.getOwner());
            versionedStore.removeAccessListVersioned(accessList).whenComplete((removed, exception) -> {
                if (exception != null || !Boolean.TRUE.equals(removed)) {
                    if (exception != null) {
                        markSqlFailure(exception);
                    }
                    restoreAccessList(previous, accessList);
                    return;
                }
                markSqlHealthy();
                invalidateRedisSafely(CacheKeys.accessList(accessList.getOwner()));
                publish(CacheInvalidationMessage.Operation.DELETE, "access-list", accessList.getOwner().toString(), nextMutationVersion(accessList.getVersion()));
            });
            return;
        }
        cachedAccessLists.remove(accessList.getOwner());
        backingStore.removeAccessList(accessList);
        afterSqlCommit(() -> {
            invalidateRedisSafely(CacheKeys.accessList(accessList.getOwner()));
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
    public CompletableFuture<Long> purgeAuditEventsBefore(final long cutoffMillis) {
        if (backingStore instanceof AuditStore auditStore) {
            return auditStore.purgeAuditEventsBefore(cutoffMillis);
        }
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<Void> enqueueOutboxEvent(final OutboxEvent event) {
        if (backingStore instanceof OutboxStore outboxStore) {
            return outboxStore.enqueueOutboxEvent(event);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support outbox events"));
    }

    @Override
    public CompletableFuture<Collection<OutboxEvent>> claimOutboxEvents(final String workerId, final int limit,
                                                                          final long now, final long leaseUntil) {
        if (backingStore instanceof OutboxStore outboxStore) {
            return outboxStore.claimOutboxEvents(workerId, limit, now, leaseUntil);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support outbox events"));
    }

    @Override
    public CompletableFuture<Boolean> acknowledgeOutboxEvent(final UUID eventId, final String workerId, final long publishedAt) {
        if (backingStore instanceof OutboxStore outboxStore) {
            return outboxStore.acknowledgeOutboxEvent(eventId, workerId, publishedAt);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support outbox events"));
    }

    @Override
    public CompletableFuture<Boolean> retryOutboxEvent(final UUID eventId, final String workerId, final long nextAttemptAt,
                                                       final String error, final boolean deadLetter) {
        if (backingStore instanceof OutboxStore outboxStore) {
            return outboxStore.retryOutboxEvent(eventId, workerId, nextAttemptAt, error, deadLetter);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support outbox events"));
    }

    @Override
    public CompletableFuture<WorldIdentity> registerWorld(final WorldIdentity identity) {
        if (backingStore instanceof WorldRegistryStore worldRegistryStore) {
            return observeSql(worldRegistryStore.registerWorld(identity));
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support world registry"));
    }

    @Override
    public CompletableFuture<WorldIdentity> loadWorld(final UUID worldUuid) {
        if (backingStore instanceof WorldRegistryStore worldRegistryStore) {
            return observeSql(worldRegistryStore.loadWorld(worldUuid));
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support world registry"));
    }

    @Override
    public CompletableFuture<Long> loadWatermark(final String aggregateType, final String aggregateId) {
        if (backingStore instanceof WatermarkStore watermarkStore) {
            return observeSql(watermarkStore.loadWatermark(aggregateType, aggregateId));
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support watermarks"));
    }

    @Override
    public CompletableFuture<Boolean> advanceWatermark(final String aggregateType, final String aggregateId,
                                                       final long aggregateVersion, final long updatedAt) {
        if (backingStore instanceof WatermarkStore watermarkStore) {
            return observeSql(watermarkStore.advanceWatermark(aggregateType, aggregateId, aggregateVersion, updatedAt));
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Backing store does not support watermarks"));
    }

    @Override
    public CompletableFuture<Void> flush() {
        return backingStore.flush();
    }

    @Override
    public ConsistencyHealth.Snapshot healthSnapshot() {
        return consistencyHealth.snapshot();
    }

    @Override
    public boolean allows(final SensitiveOperation operation) {
        return !strictConsistency || consistencyHealth.allows(operation);
    }

    @Override
    public void close() {
        if (healthScheduler != null) {
            healthScheduler.shutdownNow();
        }
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

    private <T, R> CompletableFuture<R> reconcile(final CompletableFuture<Collection<T>> loaded,
                                                  final java.util.function.Function<Collection<T>, R> updater) {
        return loaded.thenApply(updater);
    }

    private <T> CompletableFuture<T> observeSql(final CompletableFuture<T> future) {
        return future.whenComplete((ignored, exception) -> {
            if (exception == null) {
                markSqlHealthy();
            } else {
                markSqlFailure(exception);
            }
        });
    }

    private BlockProtection readRedisBlock(final BlockLocation location) {
        try {
            final String payload = redis.get(CacheKeys.block(location));
            return payload == null ? null : codec.decodeBlock(payload);
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            LOGGER.log(Level.FINE, "Redis protection read failed; falling back to SQL", exception);
            return null;
        }
    }

    private EntityProtection readRedisEntity(final UUID id) {
        try {
            final String payload = redis.get(CacheKeys.entity(id));
            return payload == null ? null : codec.decodeEntity(payload);
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            LOGGER.log(Level.FINE, "Redis entity read failed; falling back to SQL", exception);
            return null;
        }
    }

    private Group readRedisGroup(final String name) {
        try {
            final String payload = redis.get(CacheKeys.group(name));
            return payload == null ? null : codec.decodeGroup(payload);
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            LOGGER.log(Level.FINE, "Redis group read failed; falling back to SQL", exception);
            return null;
        }
    }

    private AccessList readRedisAccessList(final UUID owner) {
        try {
            final String payload = redis.get(CacheKeys.accessList(owner));
            return payload == null ? null : codec.decodeAccessList(payload);
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            LOGGER.log(Level.FINE, "Redis access-list read failed; falling back to SQL", exception);
            return null;
        }
    }

    private void afterSqlCommit(final Runnable action) {
        backingStore.flush().whenComplete((ignored, exception) -> {
            if (exception != null) {
                markSqlFailure(exception);
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
        try {
            redis.put(key, payload, REDIS_TTL);
            markRedisHealthy();
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            LOGGER.log(Level.WARNING, "Redis cache write failed; SQL remains authoritative", exception);
        }
    }

    private void invalidateRedisSafely(final String key) {
        if (redis != null) {
            try {
                redis.invalidate(key);
                markRedisHealthy();
            } catch (RuntimeException exception) {
                markRedisFailure(exception);
                LOGGER.log(Level.WARNING, "Redis cache invalidation failed; SQL remains authoritative", exception);
            }
        }
    }

    private void publish(final CacheInvalidationMessage.Operation operation, final String aggregateType, final String aggregateId) {
        publish(operation, aggregateType, aggregateId, 1);
    }

    private void publish(final CacheInvalidationMessage.Operation operation, final String aggregateType, final String aggregateId, final long version) {
        if (redis != null) {
            try {
                redis.publish(CacheKeys.invalidationChannel(), new CacheInvalidationMessage(networkId, serverId, aggregateType, aggregateId, version, operation));
                markRedisHealthy();
            } catch (RuntimeException exception) {
                markRedisFailure(exception);
                LOGGER.log(Level.WARNING, "Redis invalidation publish failed; reconciliation will repair cache state", exception);
            }
        }
    }

    /**
     * Publishes one durable mutation event. Unlike the normal best-effort
     * cache path, this method propagates Redis failures so the outbox
     * dispatcher can retain the lease and schedule a retry.
     */
    public void publishOutboxEvent(final OutboxEvent event) {
        if (redis == null) {
            throw new IllegalStateException("Redis is required to publish outbox events");
        }
        final CacheInvalidationMessage.Operation operation = event.eventType().endsWith(".removed")
                ? CacheInvalidationMessage.Operation.DELETE
                : CacheInvalidationMessage.Operation.UPSERT;
        try {
            redis.publish(CacheKeys.invalidationChannel(), new CacheInvalidationMessage(
                    networkId, serverId, event.aggregateType(), event.aggregateId(), event.aggregateVersion(), operation));
            markRedisHealthy();
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
            throw exception;
        }
    }

    private void handleInvalidation(final CacheInvalidationMessage message) {
        if (!networkId.equals(message.networkId()) || serverId.equals(message.serverId())) {
            return;
        }
        if (backingStore instanceof WatermarkStore watermarkStore) {
            watermarkStore.advanceWatermark(message.aggregateType(), message.aggregateId(), message.version(), System.currentTimeMillis())
                    .whenComplete((advanced, exception) -> {
                        if (exception != null) {
                            markSqlFailure(exception);
                            LOGGER.log(Level.WARNING, "Unable to advance invalidation watermark; ignoring event", exception);
                            return;
                        }
                        markSqlHealthy();
                        if (Boolean.TRUE.equals(advanced)) {
                            applyInvalidation(message);
                        }
                    });
            return;
        }
        applyInvalidation(message);
    }

    private void applyInvalidation(final CacheInvalidationMessage message) {
        final String versionKey = message.aggregateType() + ':' + message.aggregateId();
        final long previousVersion = latestInvalidationVersions.getOrDefault(versionKey, -1L);
        if (message.version() <= previousVersion) {
            return;
        }
        rememberInvalidationVersion(versionKey, message.version());
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

    private void rememberInvalidationVersion(final String versionKey, final long version) {
        if (latestInvalidationVersions.size() >= MAX_INVALIDATION_VERSION_ENTRIES) {
            final Iterator<String> iterator = latestInvalidationVersions.keySet().iterator();
            int removed = 0;
            while (iterator.hasNext() && removed < INVALIDATION_VERSION_PRUNE_BATCH) {
                iterator.next();
                iterator.remove();
                removed++;
            }
        }
        latestInvalidationVersions.put(versionKey, version);
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
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveBlockProtectionVersioned(protection);
        }
        saveBlockProtection(protection);
        return CompletableFuture.completedFuture(protection.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeBlockProtectionVersioned(final BlockProtection protection) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeBlockProtectionVersioned(protection);
        }
        removeBlockProtection(protection);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveEntityProtectionVersioned(final EntityProtection protection) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveEntityProtectionVersioned(protection);
        }
        saveEntityProtection(protection);
        return CompletableFuture.completedFuture(protection.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeEntityProtectionVersioned(final EntityProtection protection) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeEntityProtectionVersioned(protection);
        }
        removeEntityProtection(protection);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveGroupVersioned(final Group group) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveGroupVersioned(group);
        }
        saveGroup(group);
        return CompletableFuture.completedFuture(group.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeGroupVersioned(final Group group) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.removeGroupVersioned(group);
        }
        removeGroup(group);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Long> saveAccessListVersioned(final AccessList accessList) {
        requireMutationAllowed();
        if (backingStore instanceof VersionedStore versionedStore) {
            return versionedStore.saveAccessListVersioned(accessList);
        }
        saveAccessList(accessList);
        return CompletableFuture.completedFuture(accessList.getVersion());
    }

    @Override
    public CompletableFuture<Boolean> removeAccessListVersioned(final AccessList accessList) {
        requireMutationAllowed();
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

    private static long nextMutationVersion(final long currentVersion) {
        if (currentVersion < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        return currentVersion == Long.MAX_VALUE ? Long.MAX_VALUE : currentVersion + 1;
    }

    private void requireMutationAllowed() {
        if (!allows(SensitiveOperation.PROTECTION_MUTATION)) {
            throw new IllegalStateException("Shared protection state is not healthy; mutation refused");
        }
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
        if (value == null || value.isBlank() || value.length() > 128 || !value.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException(field + " must be a non-empty identifier");
        }
        return value;
    }

    private void probeRedisHealth() {
        if (!strictConsistency || redis == null) {
            return;
        }
        try {
            if (redis.ping()) {
                markRedisHealthy();
            } else {
                markRedisFailure("Redis ping did not return PONG");
            }
        } catch (RuntimeException exception) {
            markRedisFailure(exception);
        }
    }

    private void markRedisHealthy() {
        if (strictConsistency) {
            redisFailure.set(null);
            redisHealthy.set(true);
            refreshConsistencyHealth();
        }
    }

    private void markRedisFailure(final RuntimeException exception) {
        markRedisFailure(exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }

    private void markRedisFailure(final String reason) {
        if (strictConsistency) {
            redisHealthy.set(false);
            redisFailure.set(reason);
            refreshConsistencyHealth();
        }
    }

    private void markSqlHealthy() {
        if (strictConsistency) {
            sqlFailure.set(null);
            sqlHealthy.set(true);
            refreshConsistencyHealth();
        }
    }

    private void markSqlFailure(final Throwable exception) {
        if (strictConsistency) {
            sqlHealthy.set(false);
            sqlFailure.set(exception.getClass().getSimpleName() + ": " + exception.getMessage());
            refreshConsistencyHealth();
        }
    }

    private void refreshConsistencyHealth() {
        if (!strictConsistency) {
            return;
        }
        if (redisHealthy.get() && sqlHealthy.get()) {
            consistencyHealth.markHealthy(System.currentTimeMillis());
        } else {
            final String reason = redisFailure.get() != null ? redisFailure.get() : sqlFailure.get();
            consistencyHealth.markFailure(reason);
        }
    }
}
