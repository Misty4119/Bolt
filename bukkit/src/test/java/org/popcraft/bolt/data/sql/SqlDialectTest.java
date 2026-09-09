package org.popcraft.bolt.data.sql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class SqlDialectTest {
    @Test
    void usesNativeConflictSyntaxForPostgres() {
        final String sql = SqlDialect.forType(DatabaseType.POSTGRES).upsert(
                "blocks",
                List.of("id", "owner", "version"),
                List.of("id")
        );

        assertTrue(sql.contains("ON CONFLICT"));
        assertTrue(sql.contains("EXCLUDED.\"owner\""));
    }

    @Test
    void usesDuplicateKeySyntaxForMysql() {
        final String sql = SqlDialect.forType(DatabaseType.MYSQL).upsert(
                "blocks",
                List.of("id", "owner", "version"),
                List.of("id")
        );

        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        assertTrue(sql.contains("VALUES(`owner`)"));
    }
}
