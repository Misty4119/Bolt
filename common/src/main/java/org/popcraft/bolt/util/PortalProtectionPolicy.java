package org.popcraft.bolt.util;

import java.util.function.Predicate;

/**
 * Decides whether a portal creation candidate must be rejected.
 *
 * <p>The policy is deliberately independent of Bukkit so that every platform
 * adapter can use the same fail-closed rule and test it without a live server.</p>
 */
public final class PortalProtectionPolicy {
    private PortalProtectionPolicy() {
    }

    public static <T> boolean shouldCancel(final Iterable<T> candidates, final Predicate<T> isProtected) {
        for (final T candidate : candidates) {
            if (isProtected.test(candidate)) {
                return true;
            }
        }
        return false;
    }
}
