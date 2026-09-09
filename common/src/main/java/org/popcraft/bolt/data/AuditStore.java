package org.popcraft.bolt.data;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface AuditStore {
    void appendAuditEvent(AuditEvent event);

    CompletableFuture<Collection<AuditEvent>> loadRecentAuditEvents(UUID protectionId, int limit);
}
