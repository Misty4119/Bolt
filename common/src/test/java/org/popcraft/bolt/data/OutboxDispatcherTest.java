package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class OutboxDispatcherTest {
    @Test
    void acknowledgesSuccessfulPublish() {
        final FakeOutboxStore store = new FakeOutboxStore();
        store.add(OutboxEvent.pending(UUID.randomUUID(), "protection.updated", "block", "one", 2,
                "{}", 1L));
        final int[] published = {0};
        final OutboxDispatcher dispatcher = new OutboxDispatcher(store, event -> published[0]++, "worker", 10, 3, 1_000, 100);
        try {
            assertEquals(new OutboxDispatcher.DispatchResult(1, 1, 0, 0), dispatcher.dispatchOnce(100L).join());
            assertEquals(1, published[0]);
            assertEquals(OutboxEvent.Status.PUBLISHED, store.onlyEvent().status());
        } finally {
            dispatcher.close();
        }
    }

    @Test
    void retriesAndThenDeadLettersAfterAttemptLimit() {
        final FakeOutboxStore store = new FakeOutboxStore();
        store.add(OutboxEvent.pending(UUID.randomUUID(), "protection.updated", "block", "one", 2,
                "{}", 1L));
        final OutboxDispatcher dispatcher = new OutboxDispatcher(store, event -> {
            throw new IllegalStateException("redis unavailable");
        }, "worker", 10, 2, 1_000, 100);
        try {
            assertEquals(new OutboxDispatcher.DispatchResult(1, 0, 1, 0), dispatcher.dispatchOnce(100L).join());
            assertEquals(new OutboxDispatcher.DispatchResult(1, 0, 0, 1), dispatcher.dispatchOnce(200L).join());
            assertEquals(OutboxEvent.Status.DEAD_LETTER, store.onlyEvent().status());
        } finally {
            dispatcher.close();
        }
    }

    private static final class FakeOutboxStore implements OutboxStore {
        private final Map<UUID, OutboxEvent> events = new LinkedHashMap<>();

        private void add(final OutboxEvent event) {
            events.put(event.id(), event);
        }

        private OutboxEvent onlyEvent() {
            return events.values().iterator().next();
        }

        @Override
        public CompletableFuture<Void> enqueueOutboxEvent(final OutboxEvent event) {
            add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Collection<OutboxEvent>> claimOutboxEvents(final String workerId, final int limit,
                                                                              final long now, final long leaseUntil) {
            final Collection<OutboxEvent> claimed = new ArrayList<>();
            for (final OutboxEvent event : events.values()) {
                if (claimed.size() >= limit || event.status() != OutboxEvent.Status.PENDING
                        && event.status() != OutboxEvent.Status.RETRY_WAIT
                        || event.nextAttemptAt() > now) {
                    continue;
                }
                final OutboxEvent leased = new OutboxEvent(event.id(), event.eventType(), event.aggregateType(),
                        event.aggregateId(), event.aggregateVersion(), event.payload(), OutboxEvent.Status.LEASED,
                        event.nextAttemptAt(), workerId, leaseUntil, event.lastError(), event.attempts() + 1,
                        event.createdAt(), event.publishedAt());
                events.put(event.id(), leased);
                claimed.add(leased);
            }
            return CompletableFuture.completedFuture(claimed);
        }

        @Override
        public CompletableFuture<Boolean> acknowledgeOutboxEvent(final UUID eventId, final String workerId, final long publishedAt) {
            final OutboxEvent event = events.get(eventId);
            if (event == null || event.status() != OutboxEvent.Status.LEASED || !workerId.equals(event.leaseOwner())) {
                return CompletableFuture.completedFuture(false);
            }
            events.put(eventId, new OutboxEvent(event.id(), event.eventType(), event.aggregateType(), event.aggregateId(),
                    event.aggregateVersion(), event.payload(), OutboxEvent.Status.PUBLISHED, event.nextAttemptAt(),
                    null, null, null, event.attempts(), event.createdAt(), publishedAt));
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> retryOutboxEvent(final UUID eventId, final String workerId,
                                                           final long nextAttemptAt, final String error,
                                                           final boolean deadLetter) {
            final OutboxEvent event = events.get(eventId);
            if (event == null || event.status() != OutboxEvent.Status.LEASED || !workerId.equals(event.leaseOwner())) {
                return CompletableFuture.completedFuture(false);
            }
            events.put(eventId, new OutboxEvent(event.id(), event.eventType(), event.aggregateType(), event.aggregateId(),
                    event.aggregateVersion(), event.payload(), deadLetter ? OutboxEvent.Status.DEAD_LETTER : OutboxEvent.Status.RETRY_WAIT,
                    nextAttemptAt, null, null, error, event.attempts(), event.createdAt(), event.publishedAt()));
            return CompletableFuture.completedFuture(true);
        }
    }
}
