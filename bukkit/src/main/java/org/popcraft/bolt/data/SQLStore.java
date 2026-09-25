package org.popcraft.bolt.data;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.popcraft.bolt.access.AccessList;
import org.popcraft.bolt.data.sql.DatabaseType;
import org.popcraft.bolt.data.sql.SchemaManager;
import org.popcraft.bolt.data.sql.SqlDialect;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.protection.EntityProtection;
import org.popcraft.bolt.util.BlockLocation;
import org.popcraft.bolt.util.Group;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLStore implements Store, VersionedStore, AuditStore, OutboxStore, WorldRegistryStore, WatermarkStore, BulkBlockLookupStore {
    private static final Logger LOGGER = Logger.getLogger(SQLStore.class.getName());
    private static final List<String> BLOCK_COLUMNS = List.of(
            "id", "owner_id", "type", "created_at", "accessed_at", "world_id", "x", "y", "z", "block", "version", "updated_at"
    );
    private static final List<String> ENTITY_COLUMNS = List.of(
            "id", "owner_id", "type", "created_at", "accessed_at", "entity", "version", "updated_at"
    );
    private static final List<String> GROUP_COLUMNS = List.of(
            "name", "owner_id", "created_at", "updated_at", "version"
    );
    private static final List<String> ACCESS_LIST_COLUMNS = List.of(
            "owner_id", "version", "updated_at"
    );

    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        final Thread thread = new Thread(runnable, "Bolt-SQL");
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        final Thread thread = new Thread(runnable, "Bolt-SQL-Scheduler");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicLong pending = new AtomicLong();
    private final Configuration configuration;
    private final DatabaseType databaseType;
    private final SqlDialect dialect;
    private final String prefix;
    private final HikariDataSource dataSource;

    public SQLStore(final Configuration configuration) {
        this.configuration = configuration;
        this.databaseType = DatabaseType.parse(configuration.type());
        this.dialect = SqlDialect.forType(databaseType);
        this.prefix = configuration.prefix();
        validatePrefix(prefix);
        prepareSqlitePath();
        this.dataSource = createDataSource();
        initializeSchema();
        scheduler.scheduleWithFixedDelay(this::flushSafely, 30, 30, TimeUnit.SECONDS);
    }

    public record Configuration(String type, String path, String hostname, String database, String username,
                                String password, String prefix, Map<String, String> properties) {
        public Configuration {
            type = type == null || type.isBlank() ? "sqlite" : type;
            path = path == null || path.isBlank() ? "plugins/Bolt/bolt.db" : path;
            hostname = hostname == null ? "" : hostname;
            database = database == null ? "" : database;
            username = username == null ? "" : username;
            password = password == null ? "" : password;
            prefix = prefix == null ? "" : prefix;
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }
    }

    public DatabaseType databaseType() {
        return databaseType;
    }

    @Override
    public CompletableFuture<WorldIdentity> registerWorld(final WorldIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        return submitWriteValue(connection -> {
            final WorldIdentity[] registered = new WorldIdentity[1];
            inTransaction(connection, () -> {
                final String findByUuid = "SELECT world_uuid, name, server_group, version FROM %s WHERE world_uuid = ?".formatted(table("worlds"));
                try (PreparedStatement statement = connection.prepareStatement(findByUuid)) {
                    statement.setString(1, identity.worldUuid().toString());
                    try (ResultSet result = statement.executeQuery()) {
                        if (result.next()) {
                            final String currentGroup = result.getString("server_group");
                            if (!identity.serverGroup().equals(currentGroup)) {
                                throw new WorldIdentityConflictException("World UUID " + identity.worldUuid() + " belongs to server group " + currentGroup);
                            }
                            final long currentVersion = result.getLong("version");
                            if (identity.version() != 0 && identity.version() != currentVersion) {
                                throw new StaleWriteException("world", identity.worldUuid().toString(), identity.version());
                            }
                            final String currentName = result.getString("name");
                            if (currentName.equals(identity.name())) {
                                registered[0] = new WorldIdentity(identity.worldUuid(), currentName, currentGroup, currentVersion);
                                return;
                            }
                            ensureWorldNameAvailable(connection, identity);
                            final long nextVersion = nextVersion(currentVersion);
                            final String update = "UPDATE %s SET name = ?, updated_at = ?, version = ? WHERE world_uuid = ? AND version = ?".formatted(table("worlds"));
                            try (PreparedStatement updateStatement = connection.prepareStatement(update)) {
                                updateStatement.setString(1, identity.name());
                                updateStatement.setLong(2, System.currentTimeMillis());
                                updateStatement.setLong(3, nextVersion);
                                updateStatement.setString(4, identity.worldUuid().toString());
                                updateStatement.setLong(5, currentVersion);
                                if (updateStatement.executeUpdate() != 1) {
                                    throw new StaleWriteException("world", identity.worldUuid().toString(), currentVersion);
                                }
                            }
                            registered[0] = new WorldIdentity(identity.worldUuid(), identity.name(), currentGroup, nextVersion);
                            return;
                        }
                    }
                }
                ensureWorldNameAvailable(connection, identity);
                final String insert = "INSERT INTO %s (world_id, world_uuid, name, server_group, status, created_at, updated_at, version) VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?)".formatted(table("worlds"));
                final long now = System.currentTimeMillis();
                try (PreparedStatement statement = connection.prepareStatement(insert)) {
                    statement.setString(1, identity.worldUuid().toString());
                    statement.setString(2, identity.worldUuid().toString());
                    statement.setString(3, identity.name());
                    statement.setString(4, identity.serverGroup());
                    statement.setLong(5, now);
                    statement.setLong(6, now);
                    statement.setLong(7, 1L);
                    statement.executeUpdate();
                }
                registered[0] = new WorldIdentity(identity.worldUuid(), identity.name(), identity.serverGroup(), 1L);
            });
            return registered[0];
        });
    }

    @Override
    public CompletableFuture<WorldIdentity> loadWorld(final UUID worldUuid) {
        Objects.requireNonNull(worldUuid, "worldUuid");
        return submit(connection -> {
            final String sql = "SELECT world_uuid, name, server_group, version FROM %s WHERE world_uuid = ?".formatted(table("worlds"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, worldUuid.toString());
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return null;
                    }
                    return new WorldIdentity(
                            UUID.fromString(result.getString("world_uuid")),
                            result.getString("name"),
                            result.getString("server_group"),
                            result.getLong("version")
                    );
                }
            }
        });
    }

    @Override
    public CompletableFuture<Long> loadWatermark(final String aggregateType, final String aggregateId) {
        requireAggregateIdentity(aggregateType, aggregateId);
        return submit(connection -> {
            final String sql = "SELECT aggregate_version FROM %s WHERE aggregate_type = ? AND aggregate_id = ?".formatted(table("watermarks"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, aggregateType);
                statement.setString(2, aggregateId);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? result.getLong(1) : 0L;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> advanceWatermark(final String aggregateType, final String aggregateId,
                                                       final long aggregateVersion, final long updatedAt) {
        requireAggregateIdentity(aggregateType, aggregateId);
        if (aggregateVersion < 0 || updatedAt < 0) {
            throw new IllegalArgumentException("Watermark version and timestamp must not be negative");
        }
        return submitWriteValue(connection -> {
            final boolean[] advanced = {false};
            inTransaction(connection, () -> {
                final String select = "SELECT aggregate_version FROM %s WHERE aggregate_type = ? AND aggregate_id = ?%s".formatted(
                        table("watermarks"), databaseType.fileBacked() ? "" : " FOR UPDATE");
                Long current = null;
                try (PreparedStatement statement = connection.prepareStatement(select)) {
                    statement.setString(1, aggregateType);
                    statement.setString(2, aggregateId);
                    try (ResultSet result = statement.executeQuery()) {
                        if (result.next()) {
                            current = result.getLong(1);
                        }
                    }
                }
                if (current != null && aggregateVersion <= current) {
                    return;
                }
                if (current == null) {
                    final String insert = "INSERT INTO %s (aggregate_type, aggregate_id, aggregate_version, updated_at) VALUES (?, ?, ?, ?)".formatted(table("watermarks"));
                    try (PreparedStatement statement = connection.prepareStatement(insert)) {
                        statement.setString(1, aggregateType);
                        statement.setString(2, aggregateId);
                        statement.setLong(3, aggregateVersion);
                        statement.setLong(4, updatedAt);
                        statement.executeUpdate();
                    }
                } else {
                    final String update = "UPDATE %s SET aggregate_version = ?, updated_at = ? WHERE aggregate_type = ? AND aggregate_id = ? AND aggregate_version < ?".formatted(table("watermarks"));
                    try (PreparedStatement statement = connection.prepareStatement(update)) {
                        statement.setLong(1, aggregateVersion);
                        statement.setLong(2, updatedAt);
                        statement.setString(3, aggregateType);
                        statement.setString(4, aggregateId);
                        statement.setLong(5, aggregateVersion);
                        if (statement.executeUpdate() != 1) {
                            return;
                        }
                    }
                }
                advanced[0] = true;
            });
            return advanced[0];
        });
    }

    private void ensureWorldNameAvailable(final Connection connection, final WorldIdentity identity) throws SQLException {
        final String sql = "SELECT world_uuid FROM %s WHERE name = ? AND server_group = ?".formatted(table("worlds"));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identity.name());
            statement.setString(2, identity.serverGroup());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next() && !identity.worldUuid().toString().equals(result.getString("world_uuid"))) {
                    throw new WorldIdentityConflictException("World name " + identity.name() + " is already registered in server group " + identity.serverGroup());
                }
            }
        }
    }

    private void prepareSqlitePath() {
        if (!databaseType.fileBacked()) {
            return;
        }
        try {
            final Path databasePath = Path.of(configuration.path()).toAbsolutePath();
            final Path parent = databasePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to create SQLite database directory", exception);
        }
    }

    private HikariDataSource createDataSource() {
        final HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("Bolt-" + databaseType.configValue());
        hikari.setJdbcUrl(connectionUrl());
        hikari.setMaximumPoolSize(databaseType.fileBacked() ? 1 : 10);
        hikari.setMinimumIdle(databaseType.fileBacked() ? 1 : 2);
        hikari.setAutoCommit(true);
        if (!configuration.username().isBlank()) {
            hikari.setUsername(configuration.username());
        }
        if (!configuration.password().isBlank()) {
            hikari.setPassword(configuration.password());
        }
        configuration.properties().forEach(hikari::addDataSourceProperty);
        if (databaseType == DatabaseType.SQLITE) {
            hikari.setConnectionInitSql("PRAGMA foreign_keys = ON");
        }
        return new HikariDataSource(hikari);
    }

    private String connectionUrl() {
        return switch (databaseType) {
            case SQLITE -> "jdbc:sqlite:" + configuration.path();
            case MYSQL -> "jdbc:mysql://%s/%s".formatted(configuration.hostname(), configuration.database());
            case POSTGRES -> "jdbc:postgresql://%s/%s".formatted(configuration.hostname(), configuration.database());
        };
    }

    private void initializeSchema() {
        try (Connection connection = dataSource.getConnection()) {
            SchemaManager.initialize(connection, databaseType, prefix);
        } catch (SQLException exception) {
            dataSource.close();
            throw new IllegalStateException("Unable to initialize Bolt " + databaseType.configValue() + " schema", exception);
        }
    }

    private String table(final String name) {
        return dialect.quoteIdentifier(tableName(name));
    }

    private String tableName(final String name) {
        return prefix + "bolt_" + name;
    }

    private static void validatePrefix(final String prefix) {
        if (!prefix.matches("[A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Unsafe database table prefix: " + prefix);
        }
    }

    @Override
    public CompletableFuture<BlockProtection> loadBlockProtection(final BlockLocation location) {
        return submit(connection -> {
            final String sql = "SELECT id, owner_id, type, created_at, accessed_at, world_id, x, y, z, block, version, updated_at FROM %s WHERE world_id = ? AND x = ? AND y = ? AND z = ?".formatted(table("blocks"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, location.world());
                statement.setInt(2, location.x());
                statement.setInt(3, location.y());
                statement.setInt(4, location.z());
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? readBlock(connection, result) : null;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Collection<BlockProtection>> loadBlockProtections(final Collection<BlockLocation> locations) {
        final List<BlockLocation> requested = locations == null ? List.of() : locations.stream().distinct().toList();
        if (requested.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        return submit(connection -> {
            final List<BlockProtection> protections = new ArrayList<>();
            // Keep the parameter count bounded for SQLite and avoid oversized
            // packets on network databases. Four columns are bound per row.
            for (int offset = 0; offset < requested.size(); offset += 150) {
                final List<BlockLocation> batch = requested.subList(offset, Math.min(requested.size(), offset + 150));
                final String predicates = batch.stream()
                        .map(ignored -> "(world_id = ? AND x = ? AND y = ? AND z = ?)")
                        .collect(java.util.stream.Collectors.joining(" OR "));
                final String sql = "SELECT id, owner_id, type, created_at, accessed_at, world_id, x, y, z, block, version, updated_at FROM %s WHERE %s".formatted(table("blocks"), predicates);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    int index = 1;
                    for (final BlockLocation location : batch) {
                        statement.setString(index++, location.world());
                        statement.setInt(index++, location.x());
                        statement.setInt(index++, location.y());
                        statement.setInt(index++, location.z());
                    }
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            protections.add(readBlock(connection, result));
                        }
                    }
                }
            }
            return protections;
        });
    }

    @Override
    public CompletableFuture<Collection<BlockProtection>> loadBlockProtections() {
        return submit(connection -> {
            final String sql = "SELECT id, owner_id, type, created_at, accessed_at, world_id, x, y, z, block, version, updated_at FROM %s".formatted(table("blocks"));
            final List<BlockProtection> protections = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    protections.add(readBlock(connection, result));
                }
            }
            return protections;
        });
    }

    private BlockProtection readBlock(final Connection connection, final ResultSet result) throws SQLException {
        return new BlockProtection(
                UUID.fromString(result.getString("id")),
                UUID.fromString(result.getString("owner_id")),
                result.getString("type"),
                result.getLong("created_at"),
                result.getLong("accessed_at"),
                result.getLong("version"),
                loadProtectionAccess(connection, result.getString("id")),
                result.getString("world_id"),
                result.getInt("x"),
                result.getInt("y"),
                result.getInt("z"),
                result.getString("block")
        );
    }

    @Override
    public void saveBlockProtection(final BlockProtection protection) {
        saveBlockProtectionVersioned(protection).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Long> saveBlockProtectionVersioned(final BlockProtection protection) {
        return submitWriteValue(connection -> saveBlockNow(connection, protection));
    }

    private long saveBlockNow(final Connection connection, final BlockProtection protection) throws SQLException {
        final long nextVersion = nextVersion(protection.getVersion());
        inTransaction(connection, () -> {
            final String sql = protection.getVersion() == 0
                    ? "INSERT INTO %s (id, owner_id, type, created_at, accessed_at, world_id, x, y, z, block, version, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".formatted(table("blocks"))
                    : "UPDATE %s SET owner_id = ?, type = ?, created_at = ?, accessed_at = ?, world_id = ?, x = ?, y = ?, z = ?, block = ?, version = ?, updated_at = ? WHERE id = ? AND version = ?".formatted(table("blocks"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = 1;
                if (protection.getVersion() == 0) {
                    statement.setString(index++, protection.getId().toString());
                }
                statement.setString(index++, protection.getOwner().toString());
                statement.setString(index++, protection.getType());
                statement.setLong(index++, protection.getCreated());
                statement.setLong(index++, protection.getAccessed());
                statement.setString(index++, protection.getWorld());
                statement.setInt(index++, protection.getX());
                statement.setInt(index++, protection.getY());
                statement.setInt(index++, protection.getZ());
                statement.setString(index++, protection.getBlock());
                statement.setLong(index++, nextVersion);
                statement.setLong(index++, System.currentTimeMillis());
                if (protection.getVersion() != 0) {
                    statement.setString(index++, protection.getId().toString());
                    statement.setLong(index, protection.getVersion());
                }
                if (statement.executeUpdate() != 1) {
                    throw new StaleWriteException("block", protection.getId().toString(), protection.getVersion());
                }
            }
            replaceProtectionAccess(connection, protection.getId().toString(), protection.getAccess(), nextVersion);
            enqueueMutationEvent(connection, "protection.updated", "block", protection.getId().toString(), nextVersion, "UPSERT");
        });
        protection.setVersion(nextVersion);
        return nextVersion;
    }

    @Override
    public void removeBlockProtection(final BlockProtection protection) {
        removeBlockProtectionVersioned(protection).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Boolean> removeBlockProtectionVersioned(final BlockProtection protection) {
        return submitWriteValue(connection -> {
            final boolean[] removed = {false};
            inTransaction(connection, () -> {
                final String sql = protection.getVersion() == 0
                        ? "DELETE FROM %s WHERE id = ?".formatted(table("blocks"))
                        : "DELETE FROM %s WHERE id = ? AND version = ?".formatted(table("blocks"));
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, protection.getId().toString());
                    if (protection.getVersion() != 0) {
                        statement.setLong(2, protection.getVersion());
                    }
                    removed[0] = statement.executeUpdate() == 1;
                }
                if (removed[0]) {
                    deleteProtectionAccess(connection, protection.getId().toString());
                    enqueueMutationEvent(connection, "protection.removed", "block", protection.getId().toString(),
                            nextVersion(protection.getVersion()), "DELETE");
                }
            });
            return removed[0];
        });
    }

    @Override
    public CompletableFuture<EntityProtection> loadEntityProtection(final UUID id) {
        return submit(connection -> {
            final String sql = "SELECT id, owner_id, type, created_at, accessed_at, entity, version, updated_at FROM %s WHERE id = ?".formatted(table("entities"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, id.toString());
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? readEntity(connection, result) : null;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Collection<EntityProtection>> loadEntityProtections() {
        return submit(connection -> {
            final String sql = "SELECT id, owner_id, type, created_at, accessed_at, entity, version, updated_at FROM %s".formatted(table("entities"));
            final List<EntityProtection> protections = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    protections.add(readEntity(connection, result));
                }
            }
            return protections;
        });
    }

    private EntityProtection readEntity(final Connection connection, final ResultSet result) throws SQLException {
        return new EntityProtection(
                UUID.fromString(result.getString("id")),
                UUID.fromString(result.getString("owner_id")),
                result.getString("type"),
                result.getLong("created_at"),
                result.getLong("accessed_at"),
                result.getLong("version"),
                loadProtectionAccess(connection, result.getString("id")),
                result.getString("entity")
        );
    }

    @Override
    public void saveEntityProtection(final EntityProtection protection) {
        saveEntityProtectionVersioned(protection).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Long> saveEntityProtectionVersioned(final EntityProtection protection) {
        return submitWriteValue(connection -> saveEntityNow(connection, protection));
    }

    private long saveEntityNow(final Connection connection, final EntityProtection protection) throws SQLException {
        final long nextVersion = nextVersion(protection.getVersion());
        inTransaction(connection, () -> {
            final String sql = protection.getVersion() == 0
                    ? "INSERT INTO %s (id, owner_id, type, created_at, accessed_at, entity, version, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)".formatted(table("entities"))
                    : "UPDATE %s SET owner_id = ?, type = ?, created_at = ?, accessed_at = ?, entity = ?, version = ?, updated_at = ? WHERE id = ? AND version = ?".formatted(table("entities"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = 1;
                if (protection.getVersion() == 0) {
                    statement.setString(index++, protection.getId().toString());
                }
                statement.setString(index++, protection.getOwner().toString());
                statement.setString(index++, protection.getType());
                statement.setLong(index++, protection.getCreated());
                statement.setLong(index++, protection.getAccessed());
                statement.setString(index++, protection.getEntity());
                statement.setLong(index++, nextVersion);
                statement.setLong(index++, System.currentTimeMillis());
                if (protection.getVersion() != 0) {
                    statement.setString(index++, protection.getId().toString());
                    statement.setLong(index, protection.getVersion());
                }
                if (statement.executeUpdate() != 1) {
                    throw new StaleWriteException("entity", protection.getId().toString(), protection.getVersion());
                }
            }
            replaceProtectionAccess(connection, protection.getId().toString(), protection.getAccess(), nextVersion);
            enqueueMutationEvent(connection, "protection.updated", "entity", protection.getId().toString(), nextVersion, "UPSERT");
        });
        protection.setVersion(nextVersion);
        return nextVersion;
    }

    @Override
    public void removeEntityProtection(final EntityProtection protection) {
        removeEntityProtectionVersioned(protection).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Boolean> removeEntityProtectionVersioned(final EntityProtection protection) {
        return submitWriteValue(connection -> {
            final boolean[] removed = {false};
            inTransaction(connection, () -> {
                final String sql = protection.getVersion() == 0
                        ? "DELETE FROM %s WHERE id = ?".formatted(table("entities"))
                        : "DELETE FROM %s WHERE id = ? AND version = ?".formatted(table("entities"));
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, protection.getId().toString());
                    if (protection.getVersion() != 0) {
                        statement.setLong(2, protection.getVersion());
                    }
                    removed[0] = statement.executeUpdate() == 1;
                }
                if (removed[0]) {
                    deleteProtectionAccess(connection, protection.getId().toString());
                    enqueueMutationEvent(connection, "protection.removed", "entity", protection.getId().toString(),
                            nextVersion(protection.getVersion()), "DELETE");
                }
            });
            return removed[0];
        });
    }

    private Map<String, String> loadProtectionAccess(final Connection connection, final String protectionId) throws SQLException {
        final Map<String, String> access = new LinkedHashMap<>();
        final String sql = "SELECT subject_id, effect AS access_type FROM %s WHERE protection_id = ?".formatted(table("access_entries"));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, protectionId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    access.put(result.getString("subject_id"), result.getString("access_type"));
                }
            }
        }
        return access;
    }

    private void replaceProtectionAccess(final Connection connection, final String protectionId, final Map<String, String> access, final long version) throws SQLException {
        deleteProtectionAccess(connection, protectionId);
        final String sql = "INSERT INTO %s (protection_id, subject_type, subject_id, action, effect, version, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)".formatted(table("access_entries"));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (final Map.Entry<String, String> entry : access.entrySet()) {
                statement.setString(1, protectionId);
                statement.setString(2, "source");
                statement.setString(3, entry.getKey());
                statement.setString(4, "access_type");
                statement.setString(5, entry.getValue());
                statement.setLong(6, version);
                statement.setLong(7, System.currentTimeMillis());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private void deleteProtectionAccess(final Connection connection, final String protectionId) throws SQLException {
        delete(connection, table("access_entries"), "protection_id", protectionId);
    }

    @Override
    public CompletableFuture<Group> loadGroup(final String group) {
        return submit(connection -> {
            final String sql = "SELECT name, owner_id, version FROM %s WHERE name = ?".formatted(table("groups"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, group);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? readGroup(connection, result) : null;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Group>> loadGroups() {
        return submit(connection -> {
            final String sql = "SELECT name, owner_id, version FROM %s".formatted(table("groups"));
            final List<Group> groups = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    groups.add(readGroup(connection, result));
                }
            }
            return groups;
        });
    }

    private Group readGroup(final Connection connection, final ResultSet result) throws SQLException {
        final List<UUID> members = new ArrayList<>();
        final String sql = "SELECT player_id FROM %s WHERE group_name = ?".formatted(table("group_members"));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, result.getString("name"));
            try (ResultSet membersResult = statement.executeQuery()) {
                while (membersResult.next()) {
                    members.add(UUID.fromString(membersResult.getString("player_id")));
                }
            }
        }
        return new Group(result.getString("name"), UUID.fromString(result.getString("owner_id")), result.getLong("version"), members);
    }

    @Override
    public void saveGroup(final Group group) {
        saveGroupVersioned(group).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Long> saveGroupVersioned(final Group group) {
        return submitWriteValue(connection -> {
            final long nextVersion = nextVersion(group.getVersion());
            inTransaction(connection, () -> {
            final String sql = group.getVersion() == 0
                    ? "INSERT INTO %s (name, owner_id, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?)".formatted(table("groups"))
                    : "UPDATE %s SET owner_id = ?, updated_at = ?, version = ? WHERE name = ? AND version = ?".formatted(table("groups"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                if (group.getVersion() == 0) {
                    statement.setString(1, group.getName());
                    statement.setString(2, group.getOwner().toString());
                    statement.setLong(3, System.currentTimeMillis());
                    statement.setLong(4, System.currentTimeMillis());
                    statement.setLong(5, nextVersion);
                } else {
                    statement.setString(1, group.getOwner().toString());
                    statement.setLong(2, System.currentTimeMillis());
                    statement.setLong(3, nextVersion);
                    statement.setString(4, group.getName());
                    statement.setLong(5, group.getVersion());
                }
                if (statement.executeUpdate() != 1) {
                    throw new StaleWriteException("group", group.getName(), group.getVersion());
                }
            }
            delete(connection, table("group_members"), "group_name", group.getName());
            final String memberSql = "INSERT INTO %s (group_name, player_id, role, version, updated_at) VALUES (?, ?, ?, ?, ?)".formatted(table("group_members"));
            try (PreparedStatement statement = connection.prepareStatement(memberSql)) {
                for (final UUID member : group.getMembers()) {
                    statement.setString(1, group.getName());
                    statement.setString(2, member.toString());
                    statement.setString(3, "member");
                    statement.setLong(4, nextVersion);
                    statement.setLong(5, System.currentTimeMillis());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            enqueueMutationEvent(connection, "group.updated", "group", group.getName(), nextVersion, "UPSERT");
            });
            group.setVersion(nextVersion);
            return nextVersion;
        });
    }

    @Override
    public void removeGroup(final Group group) {
        removeGroupVersioned(group).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Boolean> removeGroupVersioned(final Group group) {
        return submitWriteValue(connection -> {
            final boolean[] removed = {false};
            inTransaction(connection, () -> {
                final String sql = group.getVersion() == 0
                        ? "DELETE FROM %s WHERE name = ?".formatted(table("groups"))
                        : "DELETE FROM %s WHERE name = ? AND version = ?".formatted(table("groups"));
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, group.getName());
                    if (group.getVersion() != 0) {
                        statement.setLong(2, group.getVersion());
                    }
                    removed[0] = statement.executeUpdate() == 1;
                }
                if (removed[0]) {
                    delete(connection, table("group_members"), "group_name", group.getName());
                    enqueueMutationEvent(connection, "group.removed", "group", group.getName(),
                            nextVersion(group.getVersion()), "DELETE");
                }
            });
            return removed[0];
        });
    }

    @Override
    public CompletableFuture<AccessList> loadAccessList(final UUID owner) {
        return submit(connection -> readAccessList(connection, owner));
    }

    @Override
    public CompletableFuture<Collection<AccessList>> loadAccessLists() {
        return submit(connection -> {
            final String sql = "SELECT owner_id, version FROM %s".formatted(table("access_lists"));
            final List<AccessList> accessLists = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    accessLists.add(readAccessList(connection, UUID.fromString(result.getString("owner_id"))));
                }
            }
            return accessLists;
        });
    }

    private AccessList readAccessList(final Connection connection, final UUID owner) throws SQLException {
        final String headerSql = "SELECT owner_id, version FROM %s WHERE owner_id = ?".formatted(table("access_lists"));
        long version;
        try (PreparedStatement header = connection.prepareStatement(headerSql)) {
            header.setString(1, owner.toString());
            try (ResultSet result = header.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                version = result.getLong("version");
            }
        }
        final Map<String, String> access = new LinkedHashMap<>();
        final String entriesSql = "SELECT subject_id, access_type FROM %s WHERE owner_id = ?".formatted(table("access_list_entries"));
        try (PreparedStatement statement = connection.prepareStatement(entriesSql)) {
            statement.setString(1, owner.toString());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    access.put(result.getString("subject_id"), result.getString("access_type"));
                }
            }
        }
        return new AccessList(owner, version, access);
    }

    @Override
    public void saveAccessList(final AccessList accessList) {
        saveAccessListVersioned(accessList).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Long> saveAccessListVersioned(final AccessList accessList) {
        return submitWriteValue(connection -> {
            final long nextVersion = nextVersion(accessList.getVersion());
            inTransaction(connection, () -> {
            final String sql = accessList.getVersion() == 0
                    ? "INSERT INTO %s (owner_id, version, updated_at) VALUES (?, ?, ?)".formatted(table("access_lists"))
                    : "UPDATE %s SET version = ?, updated_at = ? WHERE owner_id = ? AND version = ?".formatted(table("access_lists"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                if (accessList.getVersion() == 0) {
                    statement.setString(1, accessList.getOwner().toString());
                    statement.setLong(2, nextVersion);
                    statement.setLong(3, System.currentTimeMillis());
                } else {
                    statement.setLong(1, nextVersion);
                    statement.setLong(2, System.currentTimeMillis());
                    statement.setString(3, accessList.getOwner().toString());
                    statement.setLong(4, accessList.getVersion());
                }
                if (statement.executeUpdate() != 1) {
                    throw new StaleWriteException("access-list", accessList.getOwner().toString(), accessList.getVersion());
                }
            }
            delete(connection, table("access_list_entries"), "owner_id", accessList.getOwner().toString());
            final String entrySql = "INSERT INTO %s (owner_id, subject_type, subject_id, access_type, version, updated_at) VALUES (?, ?, ?, ?, ?, ?)".formatted(table("access_list_entries"));
            try (PreparedStatement statement = connection.prepareStatement(entrySql)) {
                for (final Map.Entry<String, String> entry : accessList.getAccess().entrySet()) {
                    statement.setString(1, accessList.getOwner().toString());
                    statement.setString(2, "source");
                    statement.setString(3, entry.getKey());
                    statement.setString(4, entry.getValue());
                    statement.setLong(5, nextVersion);
                    statement.setLong(6, System.currentTimeMillis());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            enqueueMutationEvent(connection, "access-list.updated", "access-list", accessList.getOwner().toString(), nextVersion, "UPSERT");
            });
            accessList.setVersion(nextVersion);
            return nextVersion;
        });
    }

    @Override
    public void removeAccessList(final AccessList accessList) {
        removeAccessListVersioned(accessList).whenComplete(this::logWriteFailure);
    }

    @Override
    public CompletableFuture<Boolean> removeAccessListVersioned(final AccessList accessList) {
        return submitWriteValue(connection -> {
            final boolean[] removed = {false};
            inTransaction(connection, () -> {
                final String sql = accessList.getVersion() == 0
                        ? "DELETE FROM %s WHERE owner_id = ?".formatted(table("access_lists"))
                        : "DELETE FROM %s WHERE owner_id = ? AND version = ?".formatted(table("access_lists"));
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, accessList.getOwner().toString());
                    if (accessList.getVersion() != 0) {
                        statement.setLong(2, accessList.getVersion());
                    }
                    removed[0] = statement.executeUpdate() == 1;
                }
                if (removed[0]) {
                    delete(connection, table("access_list_entries"), "owner_id", accessList.getOwner().toString());
                    enqueueMutationEvent(connection, "access-list.removed", "access-list", accessList.getOwner().toString(),
                            nextVersion(accessList.getVersion()), "DELETE");
                }
            });
            return removed[0];
        });
    }

    @Override
    public long pendingSave() {
        return pending.get();
    }

    @Override
    public void appendAuditEvent(final AuditEvent event) {
        submitWrite(connection -> {
            final String sql = "INSERT INTO %s (id, actor_id, source_type, source_id, action, protection_id, world_id, x, y, z, item_type, item_amount, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".formatted(table("audit_events"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, event.id().toString());
                if (event.actorId() == null) statement.setNull(2, java.sql.Types.VARCHAR); else statement.setString(2, event.actorId().toString());
                statement.setString(3, event.sourceType());
                if (event.sourceId() == null) statement.setNull(4, java.sql.Types.VARCHAR); else statement.setString(4, event.sourceId());
                statement.setString(5, event.action());
                if (event.protectionId() == null) statement.setNull(6, java.sql.Types.VARCHAR); else statement.setString(6, event.protectionId().toString());
                if (event.world() == null) statement.setNull(7, java.sql.Types.VARCHAR); else statement.setString(7, event.world());
                if (event.x() == null) statement.setNull(8, java.sql.Types.INTEGER); else statement.setInt(8, event.x());
                if (event.y() == null) statement.setNull(9, java.sql.Types.INTEGER); else statement.setInt(9, event.y());
                if (event.z() == null) statement.setNull(10, java.sql.Types.INTEGER); else statement.setInt(10, event.z());
                if (event.itemType() == null) statement.setNull(11, java.sql.Types.VARCHAR); else statement.setString(11, event.itemType());
                if (event.itemAmount() == null) statement.setNull(12, java.sql.Types.INTEGER); else statement.setInt(12, event.itemAmount());
                if (event.metadata() == null) statement.setNull(13, java.sql.Types.VARCHAR); else statement.setString(13, event.metadata());
                statement.setLong(14, event.createdAt());
                statement.executeUpdate();
            }
        });
    }

    @Override
    public CompletableFuture<Void> enqueueOutboxEvent(final OutboxEvent event) {
        if (event.status() != OutboxEvent.Status.PENDING) {
            throw new IllegalArgumentException("New outbox events must start in PENDING status");
        }
        return submitWrite(connection -> {
            insertOutboxEvent(connection, event);
        });
    }

    @Override
    public CompletableFuture<Collection<OutboxEvent>> claimOutboxEvents(final String workerId, final int limit,
                                                                          final long now, final long leaseUntil) {
        requireWorker(workerId);
        if (limit < 1 || limit > 2_000 || now < 0 || leaseUntil <= now) {
            throw new IllegalArgumentException("Invalid outbox claim arguments");
        }
        return submitWriteValue(connection -> {
            final List<OutboxEvent> events = new ArrayList<>();
            inTransaction(connection, () -> {
                final String select = "SELECT id, event_type, aggregate_type, aggregate_id, aggregate_version, payload, status, next_attempt_at, lease_owner, lease_until, last_error, published_at, attempts, created_at FROM %s WHERE ((status IN ('PENDING', 'RETRY_WAIT') AND next_attempt_at <= ?) OR (status = 'LEASED' AND lease_until < ?)) ORDER BY created_at, id LIMIT %d%s".formatted(table("outbox"), limit,
                        databaseType.fileBacked() ? "" : " FOR UPDATE SKIP LOCKED");
                final List<UUID> ids = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement(select)) {
                    statement.setLong(1, now);
                    statement.setLong(2, now);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            ids.add(UUID.fromString(result.getString("id")));
                            events.add(readOutboxEvent(result));
                        }
                    }
                }
                final String update = "UPDATE %s SET status = 'LEASED', lease_owner = ?, lease_until = ?, attempts = attempts + 1 WHERE id = ? AND ((status IN ('PENDING', 'RETRY_WAIT') AND next_attempt_at <= ?) OR (status = 'LEASED' AND lease_until < ?))".formatted(table("outbox"));
                try (PreparedStatement statement = connection.prepareStatement(update)) {
                    for (final UUID id : ids) {
                        statement.setString(1, workerId);
                        statement.setLong(2, leaseUntil);
                        statement.setString(3, id.toString());
                        statement.setLong(4, now);
                        statement.setLong(5, now);
                        if (statement.executeUpdate() != 1) {
                            throw new SQLException("Unable to lease outbox event " + id);
                        }
                    }
                }
            });
            return events.stream().map(event -> new OutboxEvent(event.id(), event.eventType(), event.aggregateType(),
                    event.aggregateId(), event.aggregateVersion(), event.payload(), OutboxEvent.Status.LEASED,
                    event.nextAttemptAt(), workerId, leaseUntil, event.lastError(), event.attempts() + 1,
                    event.createdAt(), event.publishedAt())).toList();
        });
    }

    @Override
    public CompletableFuture<Boolean> acknowledgeOutboxEvent(final UUID eventId, final String workerId, final long publishedAt) {
        Objects.requireNonNull(eventId, "eventId");
        requireWorker(workerId);
        if (publishedAt < 0) {
            throw new IllegalArgumentException("publishedAt must not be negative");
        }
        return submitWriteValue(connection -> {
            final String sql = "UPDATE %s SET status = 'PUBLISHED', published_at = ?, lease_owner = NULL, lease_until = NULL, last_error = NULL WHERE id = ? AND status = 'LEASED' AND lease_owner = ?".formatted(table("outbox"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, publishedAt);
                statement.setString(2, eventId.toString());
                statement.setString(3, workerId);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> retryOutboxEvent(final UUID eventId, final String workerId, final long nextAttemptAt,
                                                       final String error, final boolean deadLetter) {
        Objects.requireNonNull(eventId, "eventId");
        requireWorker(workerId);
        if (nextAttemptAt < 0 || error == null || error.isBlank()) {
            throw new IllegalArgumentException("Invalid outbox retry arguments");
        }
        return submitWriteValue(connection -> {
            final String sql = "UPDATE %s SET status = ?, next_attempt_at = ?, lease_owner = NULL, lease_until = NULL, last_error = ? WHERE id = ? AND status = 'LEASED' AND lease_owner = ?".formatted(table("outbox"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, deadLetter ? OutboxEvent.Status.DEAD_LETTER.name() : OutboxEvent.Status.RETRY_WAIT.name());
                statement.setLong(2, nextAttemptAt);
                statement.setString(3, error);
                statement.setString(4, eventId.toString());
                statement.setString(5, workerId);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public CompletableFuture<Collection<AuditEvent>> loadRecentAuditEvents(final UUID protectionId, final int limit) {
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("Audit limit must be between 1 and 1000");
        }
        return submit(connection -> {
            final String sql = "SELECT id, actor_id, source_type, source_id, action, protection_id, world_id, x, y, z, item_type, item_amount, metadata, created_at FROM %s WHERE protection_id = ? ORDER BY created_at DESC LIMIT %d".formatted(table("audit_events"), limit);
            final List<AuditEvent> events = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, protectionId.toString());
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        events.add(readAuditEvent(result));
                    }
                }
            }
            return events;
        });
    }

    @Override
    public CompletableFuture<Long> purgeAuditEventsBefore(final long cutoffMillis) {
        if (cutoffMillis < 0) {
            throw new IllegalArgumentException("Audit cutoff must not be negative");
        }
        return submitWriteValue(connection -> {
            final String sql = "DELETE FROM %s WHERE created_at < ?".formatted(table("audit_events"));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, cutoffMillis);
                return (long) statement.executeUpdate();
            }
        });
    }

    private AuditEvent readAuditEvent(final ResultSet result) throws SQLException {
        final String actorId = result.getString("actor_id");
        final String eventProtectionId = result.getString("protection_id");
        return new AuditEvent(
                UUID.fromString(result.getString("id")),
                actorId == null ? null : UUID.fromString(actorId),
                result.getString("source_type"),
                result.getString("source_id"),
                result.getString("action"),
                eventProtectionId == null ? null : UUID.fromString(eventProtectionId),
                result.getString("world_id"),
                nullableInt(result, "x"), nullableInt(result, "y"), nullableInt(result, "z"),
                result.getString("item_type"), nullableInt(result, "item_amount"),
                result.getString("metadata"), result.getLong("created_at")
        );
    }

    private void enqueueMutationEvent(final Connection connection, final String eventType, final String aggregateType,
                                      final String aggregateId, final long aggregateVersion, final String operation) throws SQLException {
        final String payload = "{\"operation\":\"%s\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"version\":%d}"
                .formatted(escapeJson(operation), escapeJson(aggregateType), escapeJson(aggregateId), aggregateVersion);
        insertOutboxEvent(connection, OutboxEvent.pending(UUID.randomUUID(), eventType, aggregateType, aggregateId,
                aggregateVersion, payload, System.currentTimeMillis()));
    }

    private void insertOutboxEvent(final Connection connection, final OutboxEvent event) throws SQLException {
        final String sql = "INSERT INTO %s (id, event_type, aggregate_type, aggregate_id, aggregate_version, payload, status, next_attempt_at, lease_owner, lease_until, last_error, published_at, attempts, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".formatted(table("outbox"));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.id().toString());
            statement.setString(2, event.eventType());
            statement.setString(3, event.aggregateType());
            statement.setString(4, event.aggregateId());
            statement.setLong(5, event.aggregateVersion());
            statement.setString(6, event.payload());
            statement.setString(7, event.status().name());
            statement.setLong(8, event.nextAttemptAt());
            setNullableString(statement, 9, event.leaseOwner());
            setNullableLong(statement, 10, event.leaseUntil());
            setNullableString(statement, 11, event.lastError());
            setNullableLong(statement, 12, event.publishedAt());
            statement.setLong(13, event.attempts());
            statement.setLong(14, event.createdAt());
            statement.executeUpdate();
        }
    }

    private static String escapeJson(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\r", "\\r").replace("\n", "\\n");
    }

    private static void requireAggregateIdentity(final String aggregateType, final String aggregateId) {
        if (aggregateType == null || aggregateType.isBlank() || aggregateType.length() > 64
                || aggregateId == null || aggregateId.isBlank() || aggregateId.length() > 255) {
            throw new IllegalArgumentException("Invalid watermark aggregate identity");
        }
    }

    private static OutboxEvent readOutboxEvent(final ResultSet result) throws SQLException {
        final String leaseOwner = result.getString("lease_owner");
        final long leaseUntil = result.getLong("lease_until");
        final Long nullableLeaseUntil = result.wasNull() ? null : leaseUntil;
        final long publishedAt = result.getLong("published_at");
        final Long nullablePublishedAt = result.wasNull() ? null : publishedAt;
        return new OutboxEvent(
                UUID.fromString(result.getString("id")),
                result.getString("event_type"),
                result.getString("aggregate_type"),
                result.getString("aggregate_id"),
                result.getLong("aggregate_version"),
                result.getString("payload"),
                OutboxEvent.Status.valueOf(result.getString("status")),
                result.getLong("next_attempt_at"),
                leaseOwner,
                nullableLeaseUntil,
                result.getString("last_error"),
                result.getLong("attempts"),
                result.getLong("created_at"),
                nullablePublishedAt
        );
    }

    private static void setNullableString(final PreparedStatement statement, final int index, final String value) throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void setNullableLong(final PreparedStatement statement, final int index, final Long value) throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private static void requireWorker(final String workerId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 128 || !workerId.matches("[A-Za-z0-9_.:-]+")) {
            throw new IllegalArgumentException("Invalid outbox worker ID");
        }
    }

    private static Integer nullableInt(final ResultSet result, final String column) throws SQLException {
        final int value = result.getInt(column);
        return result.wasNull() ? null : value;
    }

    @Override
    public CompletableFuture<Void> flush() {
        return CompletableFuture.runAsync(() -> {
            // The single SQL executor is a barrier for all writes submitted before this no-op.
        }, executor);
    }

    private void flushSafely() {
        flush().whenComplete((ignored, exception) -> {
            if (exception != null) {
                LOGGER.log(Level.WARNING, "Scheduled Bolt SQL flush failed", exception);
            }
        });
    }

    public void close() {
        flush().join();
        scheduler.shutdownNow();
        executor.shutdown();
        dataSource.close();
    }

    private <T> CompletableFuture<T> submit(final SqlFunction<T> function) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                return function.apply(connection);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    private CompletableFuture<Void> submitWrite(final SqlConsumer operation) {
        pending.incrementAndGet();
        return CompletableFuture.runAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                operation.accept(connection);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        }, executor).whenComplete((ignored, exception) -> {
            pending.decrementAndGet();
            if (exception != null) {
                LOGGER.log(Level.SEVERE, "Bolt SQL write failed; protection state was not committed", exception);
            }
        });
    }

    private <T> CompletableFuture<T> submitWriteValue(final SqlFunction<T> operation) {
        pending.incrementAndGet();
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                return operation.apply(connection);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        }, executor).whenComplete((ignored, exception) -> {
            pending.decrementAndGet();
            if (exception != null) {
                LOGGER.log(Level.SEVERE, "Bolt SQL versioned write failed; protection state was not committed", exception);
            }
        });
    }

    private void logWriteFailure(final Object ignored, final Throwable exception) {
        if (exception != null && !(exception instanceof java.util.concurrent.CancellationException)) {
            LOGGER.log(Level.SEVERE, "Bolt SQL write failed; protection state was not committed", exception);
        }
    }

    private static long nextVersion(final long currentVersion) {
        if (currentVersion < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        return currentVersion == Long.MAX_VALUE ? Long.MAX_VALUE : currentVersion + 1;
    }

    private static void inTransaction(final Connection connection, final SqlRunnable operation) throws SQLException {
        final boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            operation.run();
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            connection.rollback();
            throw exception;
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    private static void delete(final Connection connection, final String table, final String column, final String value) throws SQLException {
        final String sql = "DELETE FROM %s WHERE %s = ?".formatted(table, column);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, value);
            statement.executeUpdate();
        }
    }

    @FunctionalInterface
    private interface SqlFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    private interface SqlConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    private interface SqlRunnable {
        void run() throws SQLException;
    }
}
