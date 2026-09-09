package org.popcraft.bolt.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class InventoryTransportPolicyTest {
    @Test
    void usesIndependentHopperInsertAndExtractPermissions() {
        assertEquals(Permission.HOPPER_EXTRACT, InventoryTransportPolicy.sourcePermission());
        assertEquals(Permission.HOPPER_INSERT, InventoryTransportPolicy.destinationPermission());
    }
}
