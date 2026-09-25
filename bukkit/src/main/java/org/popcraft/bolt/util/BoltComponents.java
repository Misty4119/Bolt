package org.popcraft.bolt.util;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.kyori.adventure.text.minimessage.tag.resolver.TagResolver;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.entity.Player;

import java.util.Locale;

import static org.popcraft.bolt.lang.Translator.translate;

public final class BoltComponents {
    private static MiniMessage miniMessage;
    private static MessagePalette messagePalette;

    private BoltComponents() {
    }

    public static void enable() {
        enable(null);
    }

    public static void enable(final ConfigurationSection messages) {
        final String mode = messages == null ? "light" : messages.getString("palette", "light");
        final ConfigurationSection colors = messages == null
                ? null
                : messages.getConfigurationSection("colors." + mode);
        final java.util.Map<String, String> overrides = new java.util.HashMap<>();
        if (colors != null) {
            colors.getKeys(false).forEach(key -> overrides.put(key, colors.getString(key)));
        }
        // The custom resolver is layered before TagResolver.standard(), so the
        // legacy names used by translations receive the configured palette while
        // all standard MiniMessage tags remain available.
        messagePalette = MessagePalette.from(mode, overrides);
        miniMessage = MiniMessage.builder()
                .tags(messagePalette.resolver())
                .build();
    }

    public static void disable() {
        miniMessage = null;
        messagePalette = null;
    }

    private static void sendMessage(final CommandSender sender, final Component component) {
        if (!component.equals(Component.empty())) {
            sender.sendMessage(component);
        }
    }

    public static void sendMessage(final CommandSender sender, String key, TagResolver... placeholders) {
        sendMessage(sender, resolveTranslation(key, sender, placeholders));
    }

    public static void sendMessage(final CommandSender sender, String key, boolean actionBar, TagResolver... placeholders) {
        if (actionBar) {
            sender.sendActionBar(resolveTranslation(key, sender, placeholders));
        } else {
            sendMessage(sender, resolveTranslation(key, sender, placeholders));
        }
    }

    public static void sendClickableMessage(final CommandSender sender, String key, ClickEvent clickEvent, TagResolver... placeholders) {
        sendMessage(sender, resolveTranslation(key, sender, placeholders).clickEvent(clickEvent));
    }

    public static Component resolveTranslation(final String key, final CommandSender sender, TagResolver... placeholders) {
        // Give unstyled translations a readable baseline while allowing nested
        // semantic/legacy colour tags to override it.
        return miniMessage.deserialize("<bolt-text>" + messagePalette.applyLegacyTags(translateRaw(key, sender)) + "</bolt-text>", placeholders);
    }

    /**
     * Translate a message with the given sender's locale. This method returns a string, usually a MiniMessage string,
     * so it doesn't have formatting applied. See {@link #resolveTranslation(String, CommandSender, TagResolver...)}
     * for the method which returns a Component.
     * @param key translation key
     * @param sender player to use the locale of
     * @return localized string
     */
    public static String translateRaw(final String key, final CommandSender sender) {
        return translate(key, getLocaleOf(sender));
    }

    /**
     * Fetch the locale of a {@link CommandSender}. This will use the player's configured locale if the command sender
     * is a {@link Player}, otherwise, it will fall back to an empty locale. The empty locale is translated to the
     * language configured in the plugin config as the default language.
     * @param sender command sender to check the locale of
     * @return a locale, possibly an empty one
     */
    public static Locale getLocaleOf(CommandSender sender) {
        if (sender instanceof Player player) {
            return player.locale();
        } else {
            return Locale.ROOT;
        }
    }
}
