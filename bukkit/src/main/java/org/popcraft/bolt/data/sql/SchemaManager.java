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

    public static void initialize(final Connection connection, final DatabaseType type, final String prefix) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (final String sql : statements(type, prefix)) {
                statement.execute(sql);
            }
        }
    }
}
