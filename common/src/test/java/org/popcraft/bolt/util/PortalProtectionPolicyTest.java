package org.popcraft.bolt.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PortalProtectionPolicyTest {
    @Test
    void cancelsWhenAnyPortalBlockIsProtected() {
        final List<String> blocks = List.of("air", "protected_frame", "portal");

        final boolean cancelled = PortalProtectionPolicy.shouldCancel(
                blocks,
                "protected_frame"::equals
        );

        assertTrue(cancelled);
    }

    @Test
    void allowsEmptyOrUnprotectedPortalChanges() {
        assertFalse(PortalProtectionPolicy.shouldCancel(List.of(), ignored -> true));
        assertFalse(PortalProtectionPolicy.shouldCancel(List.of("air", "obsidian"), "protected"::equals));
    }
}
