package org.popcraft.bolt.data.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class RedisCache implements CacheTransport {
    private static final Logger LOGGER = Logger.getLogger(RedisCache.class.getName());
    private final RedisClient client;
    private final RedisURI redisUri;
    private final boolean sentinel;
    private final StatefulRedisConnection<String, String> cacheConnection;
    private volatile StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final Object pubSubLock = new Object();
    private final Map<String, Consumer<CacheInvalidationMessage>> subscribers = new ConcurrentHashMap<>();
    private final String namespace;
    private volatile boolean closed;

    public RedisCache(final Configuration configuration) {
        this.namespace = validateNamespace(configuration.namespace());
        this.redisUri = RedisURI.create(configuration.uri());
        this.sentinel = !redisUri.getSentinels().isEmpty();
        if (sentinel && (redisUri.getSentinelMasterId() == null || redisUri.getSentinelMasterId().isBlank())) {
            throw new IllegalArgumentException("Redis Sentinel URI must declare sentinelMasterId");
        }
        this.client = RedisClient.create(redisUri);
        this.cacheConnection = sentinel
                ? MasterReplica.connect(client, StringCodec.UTF8, redisUri)
                : client.connect();
        this.pubSubConnection = connectPubSub();
    }

    public static boolean isSentinelUri(final String uri) {
        try {
            return uri != null && !RedisURI.create(uri).getSentinels().isEmpty();
        } catch (RuntimeException exception) {
            return false;
        }
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
        runPubSubWithRetry(connection -> connection.sync().publish(key(channel), message.encode()));
    }

    @Override
    public void subscribe(final String channel, final Consumer<CacheInvalidationMessage> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        subscribers.put(channel, consumer);
        runPubSubWithRetry(connection -> subscribeOnConnection(connection, channel, consumer));
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
        if (value == null || value.isBlank() || value.length() > 128 || !value.endsWith(":")
                || !value.matches("[A-Za-z0-9_.:-]+")) {
            throw new IllegalArgumentException("Redis namespace must be non-empty and end with ':'");
        }
        return value;
    }

    @Override
    public void close() {
        closed = true;
        synchronized (pubSubLock) {
            pubSubConnection.close();
        }
        cacheConnection.close();
        client.shutdown();
    }

    private StatefulRedisPubSubConnection<String, String> connectPubSub() {
        return client.connectPubSub(sentinel ? resolveMasterUri() : redisUri);
    }

    private RedisURI resolveMasterUri() {
        try (StatefulRedisSentinelConnection<String, String> sentinelConnection = client.connectSentinel(redisUri)) {
            final SocketAddress address = sentinelConnection.sync().getMasterAddrByName(redisUri.getSentinelMasterId());
            if (!(address instanceof InetSocketAddress inetAddress) || inetAddress.isUnresolved()) {
                throw new IllegalStateException("Redis Sentinel returned an unusable master address");
            }
            return RedisURI.builder()
                    .withHost(inetAddress.getHostString())
                    .withPort(inetAddress.getPort())
                    .withAuthentication(redisUri)
                    .withSsl(redisUri)
                    .withDatabase(redisUri.getDatabase())
                    .withTimeout(redisUri.getTimeout())
                    .withClientName(redisUri.getClientName())
                    .build();
        }
    }

    private void subscribeOnConnection(final StatefulRedisPubSubConnection<String, String> connection,
                                       final String channel,
                                       final Consumer<CacheInvalidationMessage> consumer) {
        connection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(final String receivedChannel, final String body) {
                if (key(channel).equals(receivedChannel)) {
                    try {
                        consumer.accept(CacheInvalidationMessage.decode(body));
                    } catch (RuntimeException exception) {
                        LOGGER.log(Level.WARNING, "Ignoring malformed Bolt Redis invalidation message", exception);
                    }
                }
            }
        });
        connection.sync().subscribe(key(channel));
    }

    private void runPubSubWithRetry(final Consumer<StatefulRedisPubSubConnection<String, String>> operation) {
        try {
            operation.accept(pubSubConnection);
        } catch (RuntimeException firstFailure) {
            if (closed) {
                throw firstFailure;
            }
            LOGGER.log(Level.WARNING, "Redis Pub/Sub connection failed; reconnecting", firstFailure);
            reconnectPubSub();
            operation.accept(pubSubConnection);
        }
    }

    private void reconnectPubSub() {
        synchronized (pubSubLock) {
            if (closed) {
                throw new IllegalStateException("Redis cache is closed");
            }
            final StatefulRedisPubSubConnection<String, String> previous = pubSubConnection;
            previous.close();
            final StatefulRedisPubSubConnection<String, String> replacement = connectPubSub();
            try {
                subscribers.forEach((channel, consumer) -> subscribeOnConnection(replacement, channel, consumer));
                pubSubConnection = replacement;
            } catch (RuntimeException exception) {
                replacement.close();
                throw exception;
            }
        }
    }

    public record Configuration(String uri, String namespace) {
        public Configuration {
            Objects.requireNonNull(uri, "uri");
            Objects.requireNonNull(namespace, "namespace");
        }
    }
}
