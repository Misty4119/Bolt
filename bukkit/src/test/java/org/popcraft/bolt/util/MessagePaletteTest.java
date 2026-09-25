package org.popcraft.bolt.util;

import net.kyori.adventure.text.format.TextColor;
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class MessagePaletteTest {
    @Test
    void remapsLegacyTagsAndKeepsStandardTags() {
        final MessagePalette palette = MessagePalette.defaults("light");
        final MiniMessage miniMessage = MiniMessage.builder().tags(palette.resolver()).build();

        assertEquals(TextColor.fromHexString("#9CC3FF"), miniMessage.deserialize(palette.applyLegacyTags("<yellow>text</yellow>")).color());
        assertEquals("run_command", miniMessage.deserialize("<click:run_command:/bolt info>info</click>").clickEvent().action().toString());
    }

    @Test
    void acceptsConfiguredDarkPalette() {
        final MessagePalette palette = MessagePalette.from("dark", Map.of("primary", "#123456"));
        final MiniMessage miniMessage = MiniMessage.builder().tags(palette.resolver()).build();

        assertEquals(TextColor.fromHexString("#123456"), miniMessage.deserialize("<bolt-primary>text</bolt-primary>").color());
    }

    @Test
    void rejectsInvalidConfiguredColour() {
        assertThrows(IllegalArgumentException.class, () -> MessagePalette.from("light", Map.of("primary", "not-a-colour")));
    }

    @Test
    void rejectsUnknownPaletteMode() {
        assertThrows(IllegalArgumentException.class, () -> MessagePalette.from("neon", Map.of()));
    }
}
