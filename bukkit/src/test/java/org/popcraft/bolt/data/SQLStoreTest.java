package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;
import java.util.Collection;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.util.BlockLocation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SQLStoreTest {
    @Test
    void persistsNormalizedProtectionAndAccessWithSqlite() throws Exception {
        final Path database = Files.createTempFile("bolt-sql-store-", ".db");
        final SQLStore store = new SQLStore(new SQLStore.Configuration(
                "sqlite", database.toString(), "", "", "", "", "test_", java.util.Map.of()
        ));
        final UUID id = UUID.randomUUID();
        final UUID owner = UUID.randomUUID();
        final BlockProtection protection = new BlockProtection(
                id, owner, "private", 100L, 200L,
                new HashMap<>(java.util.Map.of("player:" + owner, "normal")),
                "world", 1, 64, -2, "CHEST"
        );

        try {
            store.saveBlockProtection(protection);
            store.flush().join();

            final long claimNow = System.currentTimeMillis();
            final Collection<OutboxEvent> mutationEvents = store.claimOutboxEvents("schema-test", 10, claimNow, claimNow + 1_000L).join();
            assertEquals(1, mutationEvents.size());
            assertEquals("protection.updated", mutationEvents.iterator().next().eventType());
            assertEquals(true, store.acknowledgeOutboxEvent(mutationEvents.iterator().next().id(), "schema-test", 1_001L).join());

            final BlockProtection loaded = store.loadBlockProtection(new BlockLocation("world", 1, 64, -2)).join();
            assertNotNull(loaded);
            assertEquals(id, loaded.getId());
            assertEquals(1L, loaded.getVersion());
            assertEquals("normal", loaded.getAccess().get("player:" + owner));
            assertEquals(1, store.loadBlockProtections(java.util.List.of(
                    new BlockLocation("world", 1, 64, -2),
                    new BlockLocation("world", 99, 64, 99)
            )).join().size());

            final BlockProtection stale = new BlockProtection(
                    id, owner, "private", 100L, 200L, loaded.getVersion(), new HashMap<>(),
                    "world", 1, 64, -2, "CHEST"
            );
            loaded.setType("public");
            assertEquals(2L, store.saveBlockProtectionVersioned(loaded).join());
            assertEquals(2L, loaded.getVersion());
            assertThrows(java.util.concurrent.CompletionException.class, () -> store.saveBlockProtectionVersioned(stale).join());

            try (var connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + database);
                 var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT version FROM test_bolt_schema_version")) {
                assertNotNull(result);
                result.next();
                assertEquals(2, result.getInt(1));
            }

            final AuditEvent auditEvent = new AuditEvent(
                    UUID.randomUUID(), null, "inventory_move", "world:1:64:-2", "hopper_insert", id,
                    "world", 1, 64, -2, "DIAMOND", 3, null, 300L
            );
            store.appendAuditEvent(auditEvent);
            store.flush().join();
            final Collection<AuditEvent> events = store.loadRecentAuditEvents(id, 100).join();
            assertEquals(1, events.size());
            assertEquals("hopper_insert", events.iterator().next().action());
        } finally {
            store.close();
            Files.deleteIfExists(database);
        }
    }

    @Test
    void leasesAcknowledgesAndRetriesOutboxEvents() throws Exception {
        final Path database = Files.createTempFile("bolt-outbox-", ".db");
        final SQLStore store = new SQLStore(new SQLStore.Configuration(
                "sqlite", database.toString(), "", "", "", "", "test_", java.util.Map.of()
        ));
        final UUID eventId = UUID.randomUUID();
        final OutboxEvent event = OutboxEvent.pending(eventId, "protection.updated", "block", UUID.randomUUID().toString(), 2,
                "{\"version\":2}", 100L);
        try {
            store.enqueueOutboxEvent(event).join();
            final Collection<OutboxEvent> claimed = store.claimOutboxEvents("canvas-a", 10, 100L, 200L).join();
            assertEquals(1, claimed.size());
            assertEquals(OutboxEvent.Status.LEASED, claimed.iterator().next().status());
            assertEquals(0, store.claimOutboxEvents("canvas-b", 10, 100L, 200L).join().size());
            assertEquals(false, store.acknowledgeOutboxEvent(eventId, "canvas-b", 201L).join());
            assertEquals(true, store.retryOutboxEvent(eventId, "canvas-a", 300L, "redis unavailable", false).join());
            assertEquals(1, store.claimOutboxEvents("canvas-b", 10, 300L, 400L).join().size());
            assertEquals(true, store.retryOutboxEvent(eventId, "canvas-b", 400L, "permanent failure", true).join());
            assertEquals(0, store.claimOutboxEvents("canvas-a", 10, 401L, 500L).join().size());
        } finally {
            store.close();
            Files.deleteIfExists(database);
        }
    }

    @Test
    void registersWorldByUuidAndRejectsGroupConflicts() throws Exception {
        final Path database = Files.createTempFile("bolt-world-registry-", ".db");
        final SQLStore store = new SQLStore(new SQLStore.Configuration(
                "sqlite", database.toString(), "", "", "", "", "test_", java.util.Map.of()
        ));
        final UUID worldUuid = UUID.randomUUID();
        try {
            final WorldIdentity registered = store.registerWorld(new WorldIdentity(worldUuid, "world", "survival", 0L)).join();
            assertEquals(worldUuid, registered.worldUuid());
            assertEquals("world", registered.name());
            assertEquals("survival", registered.serverGroup());

            final WorldIdentity renamed = store.registerWorld(new WorldIdentity(worldUuid, "survival-world", "survival", registered.version())).join();
            assertEquals("survival-world", renamed.name());
            assertEquals(2L, renamed.version());
            assertEquals(renamed, store.loadWorld(worldUuid).join());

            final WorldIdentity otherWorld = new WorldIdentity(UUID.randomUUID(), "survival-world", "survival", 0L);
            org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class,
                    () -> store.registerWorld(otherWorld).join());
        } finally {
            store.close();
            Files.deleteIfExists(database);
        }
    }

    @Test
    void advancesWatermarksMonotonically() throws Exception {
        final Path database = Files.createTempFile("bolt-watermark-", ".db");
        final SQLStore store = new SQLStore(new SQLStore.Configuration(
                "sqlite", database.toString(), "", "", "", "", "test_", java.util.Map.of()
        ));
        try {
            assertEquals(0L, store.loadWatermark("block", "one").join());
            assertEquals(true, store.advanceWatermark("block", "one", 2L, 200L).join());
            assertEquals(false, store.advanceWatermark("block", "one", 1L, 100L).join());
            assertEquals(false, store.advanceWatermark("block", "one", 2L, 300L).join());
            assertEquals(true, store.advanceWatermark("block", "one", 3L, 400L).join());
            assertEquals(3L, store.loadWatermark("block", "one").join());
        } finally {
            store.close();
            Files.deleteIfExists(database);
        }
    }
}
