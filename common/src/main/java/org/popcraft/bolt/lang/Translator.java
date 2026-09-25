package org.popcraft.bolt.lang;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;

public final class Translator {
    private static final Logger LOGGER = Logger.getLogger(Translator.class.getName());
    private static final Pattern TAG_PATTERN = Pattern.compile("<([A-Za-z0-9_:-]+)>");
    private static final Set<String> REQUIRED_TAGS = Set.of(
            Translation.Placeholder.ACCESS_LIST, Translation.Placeholder.ACCESS_LIST_SIZE,
            Translation.Placeholder.ACCESS_TYPE, Translation.Placeholder.ACTION,
            Translation.Placeholder.COMMAND, Translation.Placeholder.COMMAND_2,
            Translation.Placeholder.LITERAL, Translation.Placeholder.GROUP,
            Translation.Placeholder.GROUP_MEMBERS, Translation.Placeholder.MODE,
            Translation.Placeholder.NEW_PLAYER, Translation.Placeholder.NEW_PLUGIN,
            Translation.Placeholder.OLD_PLAYER, Translation.Placeholder.OLD_PLUGIN,
            Translation.Placeholder.PLAYER, Translation.Placeholder.PROTECTION,
            Translation.Placeholder.PROTECTION_TYPE, Translation.Placeholder.SOURCE_TYPE,
            Translation.Placeholder.SOURCE_IDENTIFIER, Translation.Placeholder.RAW_PROTECTION,
            Translation.Placeholder.COUNT, Translation.Placeholder.COUNT_BLOCKS,
            Translation.Placeholder.COUNT_ENTITIES, Translation.Placeholder.SECONDS,
            Translation.Placeholder.WORLD, Translation.Placeholder.X,
            Translation.Placeholder.Y, Translation.Placeholder.Z, Translation.Placeholder.TIME,
            Translation.Placeholder.FIRST, Translation.Placeholder.LAST,
            Translation.Placeholder.PAGES, Translation.Placeholder.PAGE,
            Translation.Placeholder.CREATED_TIME, Translation.Placeholder.ACCESSED_TIME,
            Translation.Placeholder.NUMBER, "newline"
    );
    private static final String TRANSLATION_FILE_FORMAT = "lang/%s.properties";
    private static final Properties fallback = loadTranslation("en");
    private static Properties translation = loadTranslation("en");
    private static String selected = "en";
    private static boolean perPlayerLocale = true;
    private static final Map<Locale, Properties> languages = new HashMap<>();

    private Translator() {
    }

    public static boolean isTranslatable(final String key, final Locale locale) {
        if (!perPlayerLocale) {
            return translation.containsKey(key) || fallback.containsKey(key);
        }

        return tryGetProperty(key, locale) != null
            || tryGetProperty(key, Locale.of(locale.getLanguage())) != null
            || translation.containsKey(key)
            || fallback.containsKey(key);
    }

    public static boolean isTranslated(final String key, final Locale locale) {
        if (!perPlayerLocale) {
            return translation.containsKey(key);
        }

        return tryGetProperty(key, locale) != null
                || tryGetProperty(key, Locale.of(locale.getLanguage())) != null
                || translation.containsKey(key);
    }

    public static String translate(final String key, final Locale locale) {
        if (!perPlayerLocale) {
            return Objects.requireNonNullElseGet(translation.getProperty(key),
                () -> Objects.requireNonNullElse(fallback.getProperty(key), key));
        }

        return Objects.requireNonNullElseGet(tryGetProperty(key, locale),
            () -> Objects.requireNonNullElseGet(tryGetProperty(key, Locale.of(locale.getLanguage())),
                () -> Objects.requireNonNullElseGet(translation.getProperty(key),
                    () -> Objects.requireNonNullElse(fallback.getProperty(key), key))));
    }

    private static String tryGetProperty(final String key, final Locale locale) {
        final Properties properties = languages.get(locale);
        if (properties == null) {
            return null;
        }

        return properties.getProperty(key);
    }

    public static String selected() {
        return selected;
    }

    public static void loadAllTranslations(final Path directory, final String preferredLanguage, final boolean perPlayerLocales) {
        final long startTimeNanos = System.nanoTime();

        // Load all the localization files bundled with the jar
        final ClassLoader classLoader = Translator.class.getClassLoader();
        try {
            // This is like using a bazooka to kill a fly (where the bazooka is "FileSystems" and the fly is
            // "just loading all the translation files")
            final URI uri = Objects.requireNonNull(classLoader.getResource("lang/")).toURI();
            try (final FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap());
                 Stream<Path> files = Files.list(fileSystem.getPath("lang/"))) {
                files.forEach(path -> {
                    if (!path.toString().toLowerCase().endsWith(".properties")) {
                        return;
                    }

                    final Locale locale = parseLocale(path.getFileName().toString().split("\\.", 2)[0]);
                    final Properties properties = loadTranslation(locale.toString());
                    languages.put(locale, properties);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        // Use "custom" if the language used doesn't exist in the built-in set of languages
        if (languages.containsKey(parseLocale(preferredLanguage))) {
            selected = preferredLanguage;
        } else {
            selected = "custom";
        }

        // Load user-defined localization files.
        try (Stream<Path> files = Files.list(directory)) {
            files.forEach(path -> {
                if (!path.toString().toLowerCase().endsWith(".properties")) {
                    return;
                }

                final Locale locale = parseLocale(path.getFileName().toString().split("\\.", 2)[0]);
                // If a default locale exists for this language, load it as a base. This allows any translation keys
                // that do not have a custom translation set to still fall through to the built-in translation.
                final Properties properties = languages.getOrDefault(locale, new Properties());
                mergeTranslation(properties, loadTranslationFromFile(path), path);
                languages.put(locale, properties);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Load the preferred fallback language
        translation = languages.getOrDefault(parseLocale(preferredLanguage), fallback);

        perPlayerLocale = perPlayerLocales;

        final long timeNanos = System.nanoTime() - startTimeNanos;
        final double timeMillis = timeNanos / 1e6d;
        LogManager.getLogManager().getLogger("").info(() -> "Loaded %d localization files in %.3f ms".formatted(languages.size(), timeMillis));
    }

    private static Properties loadTranslation(final String language) {
        final ClassLoader classLoader = Translator.class.getClassLoader();
        final Properties properties = new Properties();
        final String translationFile = TRANSLATION_FILE_FORMAT.formatted(language);
        try (final InputStream input = Objects.requireNonNullElseGet(classLoader.getResourceAsStream(translationFile), InputStream::nullInputStream);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            properties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private static Properties loadTranslationFromFile(final Path path) {
        final Properties properties = new Properties();
        if (!Files.exists(path)) {
            return properties;
        }
        try (final BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private static void mergeTranslation(final Properties target, final Properties custom, final Path path) {
        custom.stringPropertyNames().forEach(key -> {
            final String candidate = custom.getProperty(key);
            final String expected = fallback.getProperty(key);
            final Set<String> missing = expected == null ? Set.of() : missingRequiredTags(expected, candidate);
            if (!missing.isEmpty()) {
                LOGGER.warning(() -> "Ignoring translation " + path.getFileName() + "." + key
                        + ": missing required tags " + missing);
                return;
            }
            target.setProperty(key, candidate);
        });
    }

    private static Set<String> missingRequiredTags(final String expected, final String candidate) {
        final Set<String> expectedTags = tags(expected);
        expectedTags.retainAll(REQUIRED_TAGS);
        final Set<String> actualTags = tags(candidate);
        expectedTags.removeAll(actualTags);
        return expectedTags;
    }

    private static Set<String> tags(final String value) {
        final Set<String> tags = new HashSet<>();
        final var matcher = TAG_PATTERN.matcher(value == null ? "" : value);
        while (matcher.find()) {
            tags.add(matcher.group(1));
        }
        return tags;
    }

    public static Locale parseLocale(final String string) {
        final String[] segments = string.split("_", 3);
        return switch (segments.length) {
            case 1 -> Locale.of(string);
            case 2 -> Locale.of(segments[0], segments[1]);
            case 3 -> Locale.of(segments[0], segments[1], segments[2]);
            default -> Locale.ROOT;
        };
    }
}
