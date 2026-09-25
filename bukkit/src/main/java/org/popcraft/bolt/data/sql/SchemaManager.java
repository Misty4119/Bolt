package org.popcraft.bolt.data.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public final class SchemaManager {
    public static final int CURRENT_SCHEMA_VERSION = 3;

    private SchemaManager() {
    }

    public static String render(final DatabaseType type, final String prefix) {
        if (prefix == null || !prefix.matches("[A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Unsafe database table prefix: " + prefix);
        }
        final String resource = "/database/schema/" + type.configValue() + ".sql";
        try (InputStream stream = SchemaManager.class.getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IllegalStateException("Missing database schema resource: " + resource);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8).replace("${prefix}", prefix);
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to read database schema: " + resource, exception);
        }
    }

    public static List<String> statements(final DatabaseType type, final String prefix) {
        return Arrays.stream(render(type, prefix).split(";"))
                .map(String::trim)
                .filter(statement -> !statement.isEmpty())
                .toList();
    }

    public static List<String> migrationStatements(final DatabaseType type, final int version, final String prefix) {
        if (version < 2 || version > CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported Bolt schema migration: " + version);
        }
        final String resource = "/database/migration/" + type.configValue() + "/" + version + ".sql";
        try (InputStream stream = SchemaManager.class.getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IllegalStateException("Missing database migration resource: " + resource);
            }
            return Arrays.stream(new String(stream.readAllBytes(), StandardCharsets.UTF_8).replace("${prefix}", prefix).split(";"))
                    .map(String::trim)
                    .filter(statement -> !statement.isEmpty())
                    .toList();
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to read database migration: " + resource, exception);
        }
    }

    public static void initialize(final Connection connection, final DatabaseType type, final String prefix) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            final List<String> schemaStatements = statements(type, prefix);
            final String versionTable = SqlDialect.forType(type).quoteIdentifier(prefix + "bolt_schema_version");
            statement.execute(schemaStatements.getFirst());
            try (var result = statement.executeQuery("SELECT version FROM " + versionTable + " ORDER BY version DESC")) {
                if (!result.next()) {
                    for (final String sql : schemaStatements.subList(1, schemaStatements.size())) {
                        statement.execute(sql);
                    }
                    statement.executeUpdate("INSERT INTO " + versionTable + " (version, installed_at) VALUES (" + CURRENT_SCHEMA_VERSION + ", " + System.currentTimeMillis() + ")");
                    return;
                }
                final int installedVersion = result.getInt("version");
                if (installedVersion > CURRENT_SCHEMA_VERSION) {
                    throw new SQLException("Unsupported Bolt schema version " + installedVersion
                            + "; maximum supported is " + CURRENT_SCHEMA_VERSION);
                }
                for (int version = installedVersion + 1; version <= CURRENT_SCHEMA_VERSION; version++) {
                    for (final String migration : migrationStatements(type, version, prefix)) {
                        statement.execute(migration);
                    }
                    statement.executeUpdate("INSERT INTO " + versionTable + " (version, installed_at) VALUES (" + version + ", " + System.currentTimeMillis() + ")");
                }
            }
        }
    }
}
