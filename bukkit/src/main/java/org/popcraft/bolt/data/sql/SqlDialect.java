package org.popcraft.bolt.data.sql;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class SqlDialect {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private final DatabaseType type;

    private SqlDialect(final DatabaseType type) {
        this.type = Objects.requireNonNull(type, "type");
    }

    public static SqlDialect forType(final DatabaseType type) {
        return new SqlDialect(type);
    }

    public static SqlDialect forType(final String type) {
        return forType(DatabaseType.parse(type));
    }

    public DatabaseType type() {
        return type;
    }

    public String quoteIdentifier(final String identifier) {
        validateIdentifier(identifier);
        final String quote = type == DatabaseType.POSTGRES ? "\"" : "`";
        return quote + identifier + quote;
    }

    public String upsert(final String table, final List<String> columns, final List<String> keyColumns) {
        validateIdentifier(table);
        if (columns.isEmpty() || keyColumns.isEmpty() || !columns.containsAll(keyColumns)) {
            throw new IllegalArgumentException("Upsert columns must contain at least one key column");
        }
        final String quotedTable = quoteIdentifier(table);
        final String columnSql = columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        final String values = columns.stream().map(ignored -> "?").collect(Collectors.joining(", "));
        final List<String> updates = columns.stream()
                .filter(column -> !keyColumns.contains(column))
                .map(column -> quoteIdentifier(column) + " = " + valueReference(column))
                .toList();
        if (updates.isEmpty()) {
            return "INSERT INTO %s (%s) VALUES (%s) %s".formatted(
                    quotedTable,
                    columnSql,
                    values,
                    type == DatabaseType.MYSQL ? "ON DUPLICATE KEY UPDATE " + quoteIdentifier(keyColumns.getFirst()) + " = " + quoteIdentifier(keyColumns.getFirst()) : "ON CONFLICT DO NOTHING"
            );
        }
        return "INSERT INTO %s (%s) VALUES (%s) %s".formatted(
                quotedTable,
                columnSql,
                values,
                type == DatabaseType.MYSQL
                        ? "ON DUPLICATE KEY UPDATE " + String.join(", ", updates)
                        : "ON CONFLICT (" + keyColumns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")) + ") DO UPDATE SET " + String.join(", ", updates)
        );
    }

    private String valueReference(final String column) {
        return type == DatabaseType.MYSQL ? "VALUES(" + quoteIdentifier(column) + ")" : "EXCLUDED." + quoteIdentifier(column);
    }

    private static void validateIdentifier(final String identifier) {
        if (identifier == null || !IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        }
    }
}
