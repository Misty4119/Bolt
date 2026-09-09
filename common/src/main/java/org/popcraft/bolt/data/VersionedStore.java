package org.popcraft.bolt.data;

import org.popcraft.bolt.access.AccessList;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.protection.EntityProtection;
import org.popcraft.bolt.util.Group;

import java.util.concurrent.CompletableFuture;

/**
 * Optional write contract for stores shared by more than one server.
 * A successful write advances the aggregate version; a stale write completes exceptionally.
 */
public interface VersionedStore {
    CompletableFuture<Long> saveBlockProtectionVersioned(BlockProtection protection);

    CompletableFuture<Boolean> removeBlockProtectionVersioned(BlockProtection protection);

    CompletableFuture<Long> saveEntityProtectionVersioned(EntityProtection protection);

    CompletableFuture<Boolean> removeEntityProtectionVersioned(EntityProtection protection);

    CompletableFuture<Long> saveGroupVersioned(Group group);

    CompletableFuture<Boolean> removeGroupVersioned(Group group);

    CompletableFuture<Long> saveAccessListVersioned(AccessList accessList);

    CompletableFuture<Boolean> removeAccessListVersioned(AccessList accessList);
}
