package org.popcraft.bolt.data;

import java.util.Objects;
import java.util.UUID;

public record OutboxEvent(UUID id, String eventType, String aggregateType, String aggregateId,
                          long aggregateVersion, String payload, Status status,
                          long nextAttemptAt, String leaseOwner, Long leaseUntil,
                          String lastError, long attempts, long createdAt, Long publishedAt) {
    public OutboxEvent {
        Objects.requireNonNull(id, "id");
        requireText(eventType, "eventType");
        requireText(aggregateType, "aggregateType");
        requireText(aggregateId, "aggregateId");
        Objects.requireNonNull(payload, "payload");
        status = status == null ? Status.PENDING : status;
        if (aggregateVersion < 0 || nextAttemptAt < 0 || attempts < 0 || createdAt < 0) {
            throw new IllegalArgumentException("Outbox numeric fields must not be negative");
        }
    }

    public static OutboxEvent pending(final UUID id, final String eventType, final String aggregateType,
                                      final String aggregateId, final long aggregateVersion,
                                      final String payload, final long createdAt) {
        return new OutboxEvent(id, eventType, aggregateType, aggregateId, aggregateVersion, payload,
                Status.PENDING, createdAt, null, null, null, 0, createdAt, null);
    }

    private static void requireText(final String value, final String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    public enum Status {
        PENDING,
        LEASED,
        RETRY_WAIT,
        PUBLISHED,
        DEAD_LETTER
    }
}
