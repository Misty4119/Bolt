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

public class SQLStore implements Store, VersionedStore, AuditStore {
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
