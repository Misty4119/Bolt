package org.popcraft.bolt.data;

import java.util.UUID;

public record AuditEvent(UUID id, UUID actorId, String sourceType, String sourceId, String action,
                         UUID protectionId, String world, Integer x, Integer y, Integer z,
                         String itemType, Integer itemAmount, String metadata, long createdAt) {
    public AuditEvent {
        if (id == null || sourceType == null || sourceType.isBlank() || action == null || action.isBlank() || createdAt < 0) {
            throw new IllegalArgumentException("Audit event requires id, source type, action and timestamp");
        }
        if (itemAmount != null && itemAmount < 0) {
            throw new IllegalArgumentException("itemAmount must not be negative");
        }
    }
}
