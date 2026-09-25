package org.popcraft.bolt.data;

import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.util.BlockLocation;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** Bounded exact-location lookup used by world-event guards such as portals. */
public interface BulkBlockLookupStore {
    CompletableFuture<Collection<BlockProtection>> loadBlockProtections(Collection<BlockLocation> locations);
}
