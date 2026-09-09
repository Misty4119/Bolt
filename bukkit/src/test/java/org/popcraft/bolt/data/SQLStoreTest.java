package org.popcraft.bolt.data;

import org.junit.jupiter.api.Test;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.util.BlockLocation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
            assertEquals("normal", loaded.getAccess().get("player:" + owner));
        } finally {
            store.close();
            Files.deleteIfExists(database);
        }
    }
}
