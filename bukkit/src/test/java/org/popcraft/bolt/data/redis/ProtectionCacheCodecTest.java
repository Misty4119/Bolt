package org.popcraft.bolt.data.redis;

import org.junit.jupiter.api.Test;
import org.popcraft.bolt.protection.BlockProtection;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ProtectionCacheCodecTest {
    @Test
    void roundTripsProtectionWithoutDroppingAcl() {
        final UUID id = UUID.randomUUID();
        final UUID owner = UUID.randomUUID();
        final BlockProtection protection = new BlockProtection(
                id, owner, "private", 1, 2, new HashMap<>(Map.of("player:" + owner, "normal")),
                "世界|一", 10, 64, -3, "CHEST"
        );
        final ProtectionCacheCodec codec = new ProtectionCacheCodec();

        final BlockProtection decoded = codec.decodeBlock(codec.encode(protection));

        assertEquals(id, decoded.getId());
        assertEquals(owner, decoded.getOwner());
        assertEquals(protection.getAccess(), decoded.getAccess());
        assertEquals(protection.getWorld(), decoded.getWorld());
    }
}
