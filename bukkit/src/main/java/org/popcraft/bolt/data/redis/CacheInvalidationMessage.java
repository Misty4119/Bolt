package org.popcraft.bolt.data.redis;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

public record CacheInvalidationMessage(String networkId, String serverId, String aggregateType,
                                       String aggregateId, long version, Operation operation) {
    public CacheInvalidationMessage {
        requireText(networkId, "networkId");
        requireText(serverId, "serverId");
        requireText(aggregateType, "aggregateType");
        requireText(aggregateId, "aggregateId");
        Objects.requireNonNull(operation, "operation");
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
    }

    public String encode() {
        return String.join("|", encode(networkId), encode(serverId), encode(aggregateType), encode(aggregateId), Long.toString(version), operation.name());
    }

    public static CacheInvalidationMessage decode(final String encoded) {
        final String[] fields = encoded == null ? new String[0] : encoded.split("\\|", -1);
        if (fields.length != 6) {
            throw new IllegalArgumentException("Invalid Bolt cache invalidation message");
        }
        try {
            return new CacheInvalidationMessage(
                    decodeField(fields[0]), decodeField(fields[1]), decodeField(fields[2]), decodeField(fields[3]),
                    Long.parseLong(fields[4]), Operation.valueOf(fields[5])
            );
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid Bolt cache invalidation message", exception);
        }
    }

    private static String encode(final String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeField(final String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static void requireText(final String value, final String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    public enum Operation {
        UPSERT,
        INVALIDATE,
        DELETE
    }
}
