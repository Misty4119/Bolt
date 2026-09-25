package org.popcraft.bolt.data;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface OutboxStore {
    CompletableFuture<Void> enqueueOutboxEvent(OutboxEvent event);

    CompletableFuture<Collection<OutboxEvent>> claimOutboxEvents(String workerId, int limit,
                                                                  long now, long leaseUntil);

    CompletableFuture<Boolean> acknowledgeOutboxEvent(UUID eventId, String workerId, long publishedAt);

    CompletableFuture<Boolean> retryOutboxEvent(UUID eventId, String workerId, long nextAttemptAt,
                                                String error, boolean deadLetter);
}
