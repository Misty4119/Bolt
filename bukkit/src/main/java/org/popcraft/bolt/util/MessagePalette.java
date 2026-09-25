package org.popcraft.bolt.util;

import net.kyori.adventure.text.format.TextColor;
import net.kyori.adventure.text.minimessage.tag.Tag;
import net.kyori.adventure.text.minimessage.tag.resolver.TagResolver;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Centralized message colours. Legacy MiniMessage names are kept as aliases so
 * existing translations can adopt the palette without a mass rewrite.
 */
public record MessagePalette(String mode, Map<String, TextColor> colors) {
    private static final Map<String, String> LIGHT = Map.ofEntries(
            Map.entry("surface", "#121520"),
            Map.entry("surface-alt", "#1E2030"),
            Map.entry("primary", "#9CC3FF"),
            Map.entry("info", "#6FA8FF"),
            Map.entry("accent", "#D9B3FF"),
            Map.entry("accent-soft", "#B89BE8"),
            Map.entry("muted", "#8C8FA3"),
            Map.entry("text", "#D6DAEA"),
            Map.entry("neon-primary", "#4A8CFF"),
            Map.entry("glow-primary", "#709FFF"),
            Map.entry("neon-accent", "#A078E6"),
            Map.entry("glow-accent", "#C58FFF"),
            Map.entry("night-text", "#E6EAFA"),
            Map.entry("quiet", "#8D93AB")
    );
    private static final Map<String, String> DARK = Map.ofEntries(
            Map.entry("surface", "#121520"),
            Map.entry("surface-alt", "#1E2030"),
            Map.entry("primary", "#4A8CFF"),
            Map.entry("info", "#709FFF"),
            Map.entry("accent", "#A078E6"),
            Map.entry("accent-soft", "#C58FFF"),
            Map.entry("muted", "#8D93AB"),
            Map.entry("text", "#E6EAFA"),
            Map.entry("neon-primary", "#4A8CFF"),
            Map.entry("glow-primary", "#709FFF"),
            Map.entry("neon-accent", "#A078E6"),
            Map.entry("glow-accent", "#C58FFF"),
            Map.entry("night-text", "#E6EAFA"),
            Map.entry("quiet", "#8D93AB")
    );

    public MessagePalette {
        mode = mode == null || mode.isBlank() ? "light" : mode.toLowerCase(Locale.ROOT);
        colors = Map.copyOf(Objects.requireNonNull(colors, "colors"));
    }

    public static MessagePalette defaults(final String mode) {
        return from(mode, Map.of());
    }

    public static MessagePalette from(final String mode, final Map<String, String> overrides) {
        final String normalizedMode = mode == null || mode.isBlank() ? "light" : mode.toLowerCase(Locale.ROOT);
        final Map<String, String> source = switch (normalizedMode) {
            case "light" -> new LinkedHashMap<>(LIGHT);
            case "dark" -> new LinkedHashMap<>(DARK);
            default -> throw new IllegalArgumentException("Unsupported message palette: " + mode + ". Use light or dark.");
        };
        if (overrides != null) {
            overrides.forEach((name, value) -> {
                if (source.containsKey(name) && value != null && !value.isBlank()) {
                    source.put(name, value);
                }
            });
        }
        final Map<String, TextColor> parsed = new LinkedHashMap<>();
        source.forEach((name, value) -> {
            final TextColor color = TextColor.fromHexString(value);
            if (color == null) {
                throw new IllegalArgumentException("Invalid message palette colour for " + name + ": " + value);
            }
            parsed.put(name, color);
        });
        return new MessagePalette(normalizedMode, parsed);
    }

    public TextColor color(final String name) {
        return Objects.requireNonNull(colors.get(name), "Unknown message palette colour: " + name);
    }

    public String applyLegacyTags(final String message) {
        String styled = message == null ? "" : message;
        final Map<String, String> legacyTags = Map.ofEntries(
                Map.entry("yellow", "bolt-primary"),
                Map.entry("gold", "bolt-accent"),
                Map.entry("aqua", "bolt-info"),
                Map.entry("blue", "bolt-info"),
                Map.entry("green", "bolt-accent"),
                Map.entry("red", "bolt-danger"),
                Map.entry("gray", "bolt-muted"),
                Map.entry("dark_gray", "bolt-quiet"),
                Map.entry("white", "bolt-text"),
                Map.entry("light_purple", "bolt-accent"),
                Map.entry("dark_purple", "bolt-warning")
        );
        for (final Map.Entry<String, String> entry : legacyTags.entrySet()) {
            styled = styled.replace("<" + entry.getKey() + ">", "<" + entry.getValue() + ">");
            styled = styled.replace("</" + entry.getKey() + ">", "</" + entry.getValue() + ">");
        }
        return styled;
    }

    public TagResolver resolver() {
        final TagResolver.Builder custom = TagResolver.builder();
        final Map<String, String> semanticTags = Map.ofEntries(
                Map.entry("bolt-primary", "primary"),
                Map.entry("bolt-info", "info"),
                Map.entry("bolt-accent", "accent"),
                Map.entry("bolt-warning", "accent-soft"),
                Map.entry("bolt-danger", "glow-accent"),
                Map.entry("bolt-muted", "muted"),
                Map.entry("bolt-text", "text"),
                Map.entry("bolt-surface", "surface"),
                Map.entry("bolt-surface-alt", "surface-alt"),
                Map.entry("bolt-neon-primary", "neon-primary"),
                Map.entry("bolt-glow-primary", "glow-primary"),
                Map.entry("bolt-neon-accent", "neon-accent"),
                Map.entry("bolt-glow-accent", "glow-accent"),
                Map.entry("bolt-night-text", "night-text"),
                Map.entry("bolt-quiet", "quiet")
        );
        semanticTags.forEach((tag, colour) -> custom.resolver(single(tag, color(colour))));

        // Preserve existing translation files while moving their visual output
        // to the new palette. The custom resolver is intentionally before the
        // Adventure standard resolver.
        return TagResolver.resolver(custom.build(), TagResolver.standard());
    }

    private static TagResolver single(final String name, final TextColor color) {
        return TagResolver.resolver(name, Tag.styling(color));
    }
}
