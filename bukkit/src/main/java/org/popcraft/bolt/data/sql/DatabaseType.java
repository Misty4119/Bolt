package org.popcraft.bolt.data.sql;

import java.util.Locale;

public enum DatabaseType {
    SQLITE("sqlite", true),
    MYSQL("mysql", false),
    POSTGRES("postgres", false);

    private final String configValue;
    private final boolean fileBacked;

    DatabaseType(final String configValue, final boolean fileBacked) {
        this.configValue = configValue;
        this.fileBacked = fileBacked;
    }

    public static DatabaseType parse(final String value) {
        final String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "sqlite" -> SQLITE;
            case "mysql", "mariadb" -> MYSQL;
            case "postgres", "postgresql", "pgsql", "pg" -> POSTGRES;
            default -> throw new IllegalArgumentException("Unsupported database type: " + value);
        };
    }

    public String configValue() {
        return configValue;
    }

    public boolean fileBacked() {
        return fileBacked;
    }
}
