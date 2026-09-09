package org.popcraft.bolt.data.redis;

import java.time.Duration;
import java.util.function.Consumer;

public interface CacheTransport extends AutoCloseable {
    String get(String key);

    void put(String key, String value, Duration ttl);

    void invalidate(String key);

    void publish(String channel, CacheInvalidationMessage message);

    void subscribe(String channel, Consumer<CacheInvalidationMessage> consumer);

    boolean ping();

    @Override
    void close();
}
