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

            final BlockProtection loaded = store.loadBlockProtection(new BlockLocation("world", 1, 64, -2)).join();
            assertNotNull(loaded);
            assertEquals(id, loaded.getId());
            assertEquals(1L, loaded.getVersion());
            assertEquals("normal", loaded.getAccess().get("player:" + owner));

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
                assertEquals(1, result.getInt(1));
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
}
