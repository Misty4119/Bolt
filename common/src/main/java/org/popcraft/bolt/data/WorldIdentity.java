package org.popcraft.bolt.data;

import java.util.Objects;
import java.util.UUID;

public record WorldIdentity(UUID worldUuid, String name, String serverGroup, long version) {
    public WorldIdentity {
        Objects.requireNonNull(worldUuid, "worldUuid");
        requireText(name, "name");
        requireText(serverGroup, "serverGroup");
        if (version < 0) {
            throw new IllegalArgumentException("World version must not be negative");
        }
    }

    private static void requireText(final String value, final String field) {
        if (value == null || value.isBlank() || value.length() > 255) {
            throw new IllegalArgumentException(field + " must be between 1 and 255 characters");
        }
    }
}
