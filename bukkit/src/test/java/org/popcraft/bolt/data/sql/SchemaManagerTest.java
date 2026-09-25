package org.popcraft.bolt.data.sql;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SchemaManagerTest {
    @Test
    void rendersNormalizedSchemaWithSafePrefix() {
        final String schema = SchemaManager.render(DatabaseType.POSTGRES, "server_");

        assertTrue(schema.contains("server_bolt_blocks"));
        assertTrue(schema.contains("server_bolt_access_entries"));
        assertTrue(schema.contains("server_bolt_outbox"));
        assertTrue(schema.contains("server_bolt_watermarks"));
        assertTrue(schema.contains("next_attempt_at"));
    }

    @Test
    void migratesSchemaVersionOneToTwo() throws Exception {
        final Path database = Files.createTempFile("bolt-schema-migration-", ".db");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test_bolt_schema_version (version INTEGER PRIMARY KEY, installed_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_schema_version (version, installed_at) VALUES (1, 1)");
            statement.execute("CREATE TABLE test_bolt_worlds (world_id TEXT PRIMARY KEY, world_uuid TEXT NOT NULL UNIQUE, name TEXT NOT NULL, server_group TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, version INTEGER NOT NULL)");
            statement.execute("CREATE TABLE test_bolt_outbox (id TEXT PRIMARY KEY, event_type TEXT NOT NULL, aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, aggregate_version INTEGER NOT NULL, payload TEXT NOT NULL, published_at INTEGER, attempts INTEGER NOT NULL, created_at INTEGER NOT NULL)");

            SchemaManager.initialize(connection, DatabaseType.SQLITE, "test_");

            try (var version = statement.executeQuery("SELECT MAX(version) FROM test_bolt_schema_version")) {
                version.next();
                assertEquals(2, version.getInt(1));
            }
            try (var columns = statement.executeQuery("PRAGMA table_info(test_bolt_outbox)")) {
                boolean hasStatus = false;
                boolean hasLease = false;
                while (columns.next()) {
                    hasStatus |= "status".equals(columns.getString("name"));
                    hasLease |= "lease_until".equals(columns.getString("name"));
                }
                assertTrue(hasStatus);
                assertTrue(hasLease);
            }
            try (var watermarks = statement.executeQuery("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'test_bolt_watermarks'")) {
                assertTrue(watermarks.next());
            }
        } finally {
            Files.deleteIfExists(database);
        }
    }
}
