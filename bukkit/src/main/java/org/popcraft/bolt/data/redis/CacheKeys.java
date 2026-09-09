package org.popcraft.bolt.data.redis;

import org.popcraft.bolt.util.BlockLocation;

import java.util.UUID;

public final class CacheKeys {
    private CacheKeys() {
    }

    public static String block(final BlockLocation location) {
        return "protection:block:%s:%d:%d:%d".formatted(location.world(), location.x(), location.y(), location.z());
    }

    public static String block(final UUID id) {
        return "protection:block:id:" + id;
    }

    public static String entity(final UUID id) {
        return "protection:entity:" + id;
    }

    public static String invalidationChannel() {
        return "invalidation";
    }
}
