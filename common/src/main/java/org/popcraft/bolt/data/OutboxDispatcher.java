package org.popcraft.bolt.data;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Claims durable outbox rows and publishes them with bounded retries.
 *
 * <p>The SQL store remains the source of truth. A successful publish is only
 * acknowledged after the publisher returns normally; failures are returned to
 * the outbox with exponential backoff and eventually dead-lettered.</p>
 */
public final class OutboxDispatcher implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(OutboxDispatcher.class.getName());
    private static final long MAX_BACKOFF_MILLIS = Duration.ofMinutes(5).toMillis();

    private final OutboxStore store;
    private final Publisher publisher;
    private final String workerId;
    private final int batchSize;
    private final int maxAttempts;
    private final long leaseMillis;
    private final long retryBaseMillis;
    private final ExecutorService publisherExecutor;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean();

    public OutboxDispatcher(final OutboxStore store, final Publisher publisher, final String workerId,
                            final int batchSize, final int maxAttempts,
                            final long leaseMillis, final long retryBaseMillis) {
        this.store = Objects.requireNonNull(store, "store");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.workerId = requireText(workerId, "workerId", 128);
        if (batchSize < 1 || batchSize > 2_000 || maxAttempts < 1
                || leaseMillis < 1 || retryBaseMillis < 1) {
            throw new IllegalArgumentException("Invalid outbox dispatcher configuration");
        }
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.leaseMillis = leaseMillis;
        this.retryBaseMillis = retryBaseMillis;
        this.publisherExecutor = Executors.newFixedThreadPool(2, runnable -> {
            final Thread thread = new Thread(runnable, "Bolt-Outbox-Publisher");
            thread.setDaemon(true);
            return thread;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "Bolt-Outbox-Scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start(final long intervalMillis) {
        if (intervalMillis < 100) {
            throw new IllegalArgumentException("Outbox interval must be at least 100ms");
        }
        scheduler.scheduleWithFixedDelay(() -> dispatchOnce(System.currentTimeMillis())
                        .exceptionally(exception -> {
                            LOGGER.log(Level.WARNING, "Bolt outbox dispatch failed", exception);
                            return DispatchResult.empty();
                        }), intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<DispatchResult> dispatchOnce(final long now) {
        if (now < 0) {
            throw new IllegalArgumentException("now must not be negative");
        }
        if (!running.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(DispatchResult.empty());
        }
        final CompletableFuture<DispatchResult> result = store.claimOutboxEvents(
                        workerId, batchSize, now, safeAdd(now, leaseMillis))
                .thenCompose(events -> processEvents(events, now))
                .whenComplete((ignored, exception) -> running.set(false));
        return result;
    }

    private CompletableFuture<DispatchResult> processEvents(final Collection<OutboxEvent> events, final long now) {
        CompletableFuture<DispatchResult> result = CompletableFuture.completedFuture(DispatchResult.empty());
        for (final OutboxEvent event : events) {
            result = result.thenCompose(current -> processEvent(event, now).thenApply(outcome ->
                    current.plus(new DispatchResult(1, outcome.published(), outcome.retried(), outcome.deadLettered()))));
        }
        return result;
    }

    private CompletableFuture<DispatchResult> processEvent(final OutboxEvent event, final long now) {
        final CompletableFuture<Void> publish = CompletableFuture.runAsync(() -> publisher.publish(event), publisherExecutor);
        return publish.handle((ignored, failure) -> {
            if (failure == null) {
                return store.acknowledgeOutboxEvent(event.id(), workerId, now)
                .thenApply(acknowledged -> acknowledged ? DispatchResult.publishedResult() : DispatchResult.empty());
            }
            final String error = errorMessage(failure);
            final boolean deadLetter = event.attempts() >= maxAttempts;
            return store.retryOutboxEvent(event.id(), workerId, safeAdd(now, retryDelay(event.attempts())), error, deadLetter)
                    .thenApply(retried -> retried
                            ? (deadLetter ? DispatchResult.deadLetteredResult() : DispatchResult.retriedResult())
                            : DispatchResult.empty());
        }).thenCompose(future -> future);
    }

    private long retryDelay(final long attempts) {
        final int exponent = (int) Math.min(10, Math.max(0, attempts - 1));
        final long multiplier = 1L << exponent;
        return Math.min(MAX_BACKOFF_MILLIS, safeMultiply(retryBaseMillis, multiplier));
    }

    private static long safeAdd(final long left, final long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long safeMultiply(final long left, final long right) {
        if (left > 0 && right > Long.MAX_VALUE / left) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static String errorMessage(final Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        final String message = current.getMessage();
        final String value = current.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
        return value.substring(0, Math.min(2000, value.length()));
    }

    private static String requireText(final String value, final String field, final int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(field + " must be non-empty and <= " + maxLength + " characters");
        }
        return value;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        publisherExecutor.shutdownNow();
    }

    @FunctionalInterface
    public interface Publisher {
        void publish(OutboxEvent event);
    }

    public record DispatchResult(int claimed, int published, int retried, int deadLettered) {
        public static DispatchResult empty() {
            return new DispatchResult(0, 0, 0, 0);
        }

        private static DispatchResult publishedResult() {
            return new DispatchResult(1, 1, 0, 0);
        }

        private static DispatchResult retriedResult() {
            return new DispatchResult(1, 0, 1, 0);
        }

        private static DispatchResult deadLetteredResult() {
            return new DispatchResult(1, 0, 0, 1);
        }

        private DispatchResult plus(final DispatchResult other) {
            return new DispatchResult(claimed + other.claimed, published + other.published,
                    retried + other.retried, deadLettered + other.deadLettered);
        }
    }
}
