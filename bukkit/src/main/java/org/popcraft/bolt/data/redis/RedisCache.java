package org.popcraft.bolt.data.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class RedisCache implements CacheTransport {
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> cacheConnection;
    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final String namespace;

    public RedisCache(final Configuration configuration) {
        this.namespace = validateNamespace(configuration.namespace());
        final RedisURI uri = RedisURI.create(configuration.uri());
        this.client = RedisClient.create(uri);
        this.cacheConnection = client.connect();
        this.pubSubConnection = client.connectPubSub();
    }

    @Override
    public String get(final String key) {
        return cacheConnection.sync().get(key(key));
    }

    @Override
    public void put(final String key, final String value, final Duration ttl) {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(ttl, "ttl");
        final long seconds = Math.max(1, ttl.toSeconds());
        cacheConnection.sync().setex(key(key), seconds, value);
    }

    @Override
    public void invalidate(final String key) {
        cacheConnection.sync().del(key(key));
    }

    @Override
    public void publish(final String channel, final CacheInvalidationMessage message) {
        pubSubConnection.sync().publish(key(channel), message.encode());
    }

    @Override
    public void subscribe(final String channel, final Consumer<CacheInvalidationMessage> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(final String receivedChannel, final String body) {
                if (key(channel).equals(receivedChannel)) {
                    consumer.accept(CacheInvalidationMessage.decode(body));
                }
            }
        });
        pubSubConnection.sync().subscribe(key(channel));
    }

    @Override
    public boolean ping() {
        return "PONG".equalsIgnoreCase(cacheConnection.sync().ping());
    }

    private String key(final String value) {
        if (value == null || value.isBlank() || value.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Redis key/channel must not be blank or contain spaces");
        }
        return namespace + value;
    }

    private static String validateNamespace(final String value) {
        if (value == null || value.isBlank() || !value.endsWith(":")) {
            throw new IllegalArgumentException("Redis namespace must be non-empty and end with ':'");
        }
        return value;
    }

    @Override
    public void close() {
        pubSubConnection.close();
        cacheConnection.close();
        client.shutdown();
    }

    public record Configuration(String uri, String namespace) {
        public Configuration {
            Objects.requireNonNull(uri, "uri");
            Objects.requireNonNull(namespace, "namespace");
        }
    }
}
