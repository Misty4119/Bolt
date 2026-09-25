package org.popcraft.bolt.data.sql;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void migratesSchemaVersionOneToThreeWithoutLosingExistingData() throws Exception {
        final Path database = Files.createTempFile("bolt-schema-migration-", ".db");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test_bolt_schema_version (version INTEGER PRIMARY KEY, installed_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_schema_version (version, installed_at) VALUES (1, 1)");
            statement.execute("CREATE TABLE test_bolt_worlds (world_id TEXT PRIMARY KEY, world_uuid TEXT NOT NULL UNIQUE, name TEXT NOT NULL, server_group TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, version INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_worlds (world_id, world_uuid, name, server_group, created_at, updated_at, version) VALUES ('world-1', 'uuid-1', 'world', 'survival', 10, 20, 3)");
            statement.execute("CREATE TABLE test_bolt_blocks (id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, type TEXT NOT NULL, created_at INTEGER NOT NULL, accessed_at INTEGER NOT NULL, world_id TEXT NOT NULL, x INTEGER NOT NULL, y INTEGER NOT NULL, z INTEGER NOT NULL, block TEXT NOT NULL, version INTEGER NOT NULL, updated_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_blocks (id, owner_id, type, created_at, accessed_at, world_id, x, y, z, block, version, updated_at) VALUES ('block-1', 'owner-1', 'private', 10, 20, 'world-1', 1, 64, -2, 'CHEST', 4, 30)");
            statement.execute("CREATE TABLE test_bolt_access_entries (protection_id TEXT NOT NULL, subject_type TEXT NOT NULL, subject_id TEXT NOT NULL, action TEXT NOT NULL, effect TEXT NOT NULL, version INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (protection_id, subject_type, subject_id, action))");
            statement.execute("INSERT INTO test_bolt_access_entries (protection_id, subject_type, subject_id, action, effect, version, updated_at) VALUES ('block-1', 'player', 'guest-1', 'open', 'allow', 2, 30)");
            statement.execute("CREATE TABLE test_bolt_outbox (id TEXT PRIMARY KEY, event_type TEXT NOT NULL, aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, aggregate_version INTEGER NOT NULL, payload TEXT NOT NULL, published_at INTEGER, attempts INTEGER NOT NULL, created_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_outbox (id, event_type, aggregate_type, aggregate_id, aggregate_version, payload, published_at, attempts, created_at) VALUES ('event-published', 'protection.updated', 'block', 'block-1', 4, '{}', 25, 1, 15)");
            statement.execute("INSERT INTO test_bolt_outbox (id, event_type, aggregate_type, aggregate_id, aggregate_version, payload, published_at, attempts, created_at) VALUES ('event-pending', 'protection.updated', 'block', 'block-1', 5, '{}', NULL, 2, 35)");

            SchemaManager.initialize(connection, DatabaseType.SQLITE, "test_");

            try (var version = statement.executeQuery("SELECT MAX(version) FROM test_bolt_schema_version")) {
                version.next();
                assertEquals(3, version.getInt(1));
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
            try (var block = statement.executeQuery("SELECT owner_id, block, version FROM test_bolt_blocks WHERE id = 'block-1'")) {
                assertTrue(block.next());
                assertEquals("owner-1", block.getString("owner_id"));
                assertEquals("CHEST", block.getString("block"));
                assertEquals(4, block.getInt("version"));
            }
            try (var access = statement.executeQuery("SELECT effect FROM test_bolt_access_entries WHERE protection_id = 'block-1'")) {
                assertTrue(access.next());
                assertEquals("allow", access.getString("effect"));
            }
            try (var world = statement.executeQuery("SELECT world_uuid, status FROM test_bolt_worlds WHERE world_id = 'world-1'")) {
                assertTrue(world.next());
                assertEquals("uuid-1", world.getString("world_uuid"));
                assertEquals("ACTIVE", world.getString("status"));
            }
            try (var published = statement.executeQuery("SELECT status, published_at FROM test_bolt_outbox WHERE id = 'event-published'")) {
                assertTrue(published.next());
                assertEquals("PUBLISHED", published.getString("status"));
                assertEquals(25, published.getLong("published_at"));
            }
            try (var pending = statement.executeQuery("SELECT status, attempts FROM test_bolt_outbox WHERE id = 'event-pending'")) {
                assertTrue(pending.next());
                assertEquals("PENDING", pending.getString("status"));
                assertEquals(2, pending.getInt("attempts"));
            }

            SchemaManager.initialize(connection, DatabaseType.SQLITE, "test_");
        } finally {
            Files.deleteIfExists(database);
        }
    }

    @Test
    void marksPreviouslyPublishedVersionTwoOutboxRowsAsPublished() throws Exception {
        final Path database = Files.createTempFile("bolt-schema-v2-migration-", ".db");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test_bolt_schema_version (version INTEGER PRIMARY KEY, installed_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_schema_version (version, installed_at) VALUES (2, 1)");
            statement.execute("CREATE TABLE test_bolt_outbox (id TEXT PRIMARY KEY, status TEXT NOT NULL, lease_owner TEXT, lease_until INTEGER, last_error TEXT, published_at INTEGER, attempts INTEGER NOT NULL, next_attempt_at INTEGER NOT NULL, event_type TEXT NOT NULL, aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, aggregate_version INTEGER NOT NULL, payload TEXT NOT NULL, created_at INTEGER NOT NULL)");
            statement.execute("INSERT INTO test_bolt_outbox (id, status, published_at, attempts, next_attempt_at, event_type, aggregate_type, aggregate_id, aggregate_version, payload, created_at) VALUES ('event-old-published', 'PENDING', 25, 1, 0, 'protection.updated', 'block', 'block-1', 4, '{}', 15)");

            SchemaManager.initialize(connection, DatabaseType.SQLITE, "test_");

            try (var version = statement.executeQuery("SELECT MAX(version) FROM test_bolt_schema_version")) {
                version.next();
                assertEquals(3, version.getInt(1));
            }
            try (var event = statement.executeQuery("SELECT status, lease_owner, lease_until, last_error FROM test_bolt_outbox WHERE id = 'event-old-published'")) {
                assertTrue(event.next());
                assertEquals("PUBLISHED", event.getString("status"));
                assertNull(event.getString("lease_owner"));
                assertNull(event.getString("lease_until"));
                assertNull(event.getString("last_error"));
            }
        } finally {
            Files.deleteIfExists(database);
        }
    }
}
