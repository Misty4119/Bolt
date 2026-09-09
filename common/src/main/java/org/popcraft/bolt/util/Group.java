package org.popcraft.bolt.util;

import java.util.List;
import java.util.UUID;

public class Group {
    private final String name;
    private final UUID owner;
    private final List<UUID> members;
    private long version;

    public Group(String name, UUID owner, List<UUID> members) {
        this(name, owner, 0, members);
    }

    public Group(String name, UUID owner, long version, List<UUID> members) {
        this.name = name;
        this.owner = owner;
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative");
        }
        this.version = version;
        this.members = members;
    }

    public String getName() {
        return name;
    }

    public UUID getOwner() {
        return owner;
    }

    public List<UUID> getMembers() {
        return members;
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
        return "Group{" +
                "name='" + name + '\'' +
                ", owner=" + owner +
                ", version=" + version +
                ", members=" + members +
                '}';
    }
}
