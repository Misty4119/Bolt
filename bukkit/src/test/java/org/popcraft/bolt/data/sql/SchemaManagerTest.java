package org.popcraft.bolt.data.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class SchemaManagerTest {
    @Test
    void rendersNormalizedSchemaWithSafePrefix() {
        final String schema = SchemaManager.render(DatabaseType.POSTGRES, "server_");

        assertTrue(schema.contains("server_bolt_blocks"));
        assertTrue(schema.contains("server_bolt_access_entries"));
        assertTrue(schema.contains("server_bolt_outbox"));
    }
}
