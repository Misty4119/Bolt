package org.popcraft.bolt.access;

import java.util.Map;
import java.util.UUID;

public class AccessList {
    private final UUID owner;
    private final Map<String, String> access;
    private long version;

    public AccessList(UUID owner, Map<String, String> access) {
        this(owner, 0, access);
    }

    public AccessList(UUID owner, long version, Map<String, String> access) {
        this.owner = owner;
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        this.version = version;
        this.access = access;
    }

    public UUID getOwner() {
        return owner;
    }

    public Map<String, String> getAccess() {
        return access;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        this.version = version;
    }

    @Override
    public String toString() {
        return "AccessList{" +
                "owner=" + owner +
                ", version=" + version +
                ", access=" + access +
                '}';
    }
}
