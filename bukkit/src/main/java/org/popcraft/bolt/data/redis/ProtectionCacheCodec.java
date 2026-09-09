package org.popcraft.bolt.data.redis;

import com.google.gson.Gson;
import org.popcraft.bolt.access.AccessList;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.protection.EntityProtection;
import org.popcraft.bolt.util.Group;

import java.util.Objects;

public final class ProtectionCacheCodec {
    private final Gson gson;

    public ProtectionCacheCodec() {
        this(new Gson());
    }

    ProtectionCacheCodec(final Gson gson) {
        this.gson = Objects.requireNonNull(gson, "gson");
    }

    public String encode(final BlockProtection protection) {
        return gson.toJson(protection);
    }

    public BlockProtection decodeBlock(final String payload) {
        return gson.fromJson(payload, BlockProtection.class);
    }

    public String encode(final EntityProtection protection) {
        return gson.toJson(protection);
    }

    public EntityProtection decodeEntity(final String payload) {
        return gson.fromJson(payload, EntityProtection.class);
    }

    public String encode(final Group group) {
        return gson.toJson(group);
    }

    public Group decodeGroup(final String payload) {
        return gson.fromJson(payload, Group.class);
    }

    public String encode(final AccessList accessList) {
        return gson.toJson(accessList);
    }

    public AccessList decodeAccessList(final String payload) {
        return gson.fromJson(payload, AccessList.class);
    }
}
