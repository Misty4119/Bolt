package org.popcraft.bolt.data.redis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CacheInvalidationMessageTest {
    @Test
    void roundTripsUnicodeAndDelimiterCharacters() {
        final CacheInvalidationMessage message = new CacheInvalidationMessage(
                "network|台灣", "canvas-a", "block", "world|1:2:3", 42,
                CacheInvalidationMessage.Operation.INVALIDATE
        );

        assertEquals(message, CacheInvalidationMessage.decode(message.encode()));
    }

    @Test
    void rejectsMalformedMessages() {
        assertThrows(IllegalArgumentException.class, () -> CacheInvalidationMessage.decode("not-a-message"));
        assertThrows(IllegalArgumentException.class, () -> new CacheInvalidationMessage(
                "network", "server", "block", "id", -1, CacheInvalidationMessage.Operation.DELETE
        ));
    }
}
