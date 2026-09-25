package org.popcraft.bolt.data;

import java.util.concurrent.CompletableFuture;

/**
 * Durable, monotonic progress for cross-server mutation consumption.
 *
 * <p>A watermark is an acknowledgement boundary, not a cache value. It must
 * never move backwards when a delayed or duplicated event is received.</p>
 */
public interface WatermarkStore {
    CompletableFuture<Long> loadWatermark(String aggregateType, String aggregateId);

    /**
     * Advances the watermark only when {@code aggregateVersion} is newer.
     *
     * @return true when the stored watermark was advanced, false when the
     *         incoming version was already applied or stale
     */
    CompletableFuture<Boolean> advanceWatermark(String aggregateType, String aggregateId,
                                                long aggregateVersion, long updatedAt);
}
