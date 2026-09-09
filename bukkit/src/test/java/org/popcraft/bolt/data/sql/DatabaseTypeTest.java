package org.popcraft.bolt.data.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class DatabaseTypeTest {
    @Test
    void acceptsConfiguredDatabaseAliases() {
        assertEquals(DatabaseType.SQLITE, DatabaseType.parse("sqlite"));
        assertEquals(DatabaseType.MYSQL, DatabaseType.parse("mysql"));
        assertEquals(DatabaseType.POSTGRES, DatabaseType.parse("postgresql"));
        assertEquals(DatabaseType.POSTGRES, DatabaseType.parse("pg"));
    }

    @Test
    void rejectsUnknownDatabaseTypes() {
        assertThrows(IllegalArgumentException.class, () -> DatabaseType.parse("oracle"));
    }
}
