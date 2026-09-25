package org.popcraft.bolt.lang;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class TranslationContractTest {
    private static final Pattern TAG = Pattern.compile("<([A-Za-z0-9_:-]+)>");
    private static final Set<String> STYLE_TAGS = Set.of(
            "red", "yellow", "gray", "white", "green", "aqua", "blue", "dark_blue", "dark_aqua",
            "dark_gray", "dark_green", "dark_purple", "gold", "light_purple", "black", "bold", "italic",
            "underlined", "strikethrough", "obfuscated"
    );
    private static final List<String> LANGUAGES = List.of(
            "cs", "de", "en", "es", "fr", "it", "ja", "no_NO", "pl", "pt_BR", "ru", "sk", "vi", "zh"
    );

    @Test
    void bundledTranslationsPreserveRequiredMiniMessageTags() throws IOException {
        final Properties fallback = load("en");
        final Set<String> keys = fallback.stringPropertyNames();
        for (final String language : LANGUAGES) {
            final Properties translation = load(language);
            for (final String key : keys) {
                final Set<String> expected = requiredTags(fallback.getProperty(key));
                final Set<String> actual = tags(translation.getProperty(key, ""));
                expected.removeAll(actual);
                assertTrue(expected.isEmpty(), language + ".properties missing " + expected + " for " + key);
            }
        }
    }

    private static Properties load(final String language) throws IOException {
        final Properties properties = new Properties();
        try (InputStream stream = TranslationContractTest.class.getClassLoader()
                .getResourceAsStream("lang/" + language + ".properties")) {
            if (stream == null) {
                throw new IOException("Missing translation resource: " + language);
            }
            properties.load(stream);
        }
        return properties;
    }

    private static Set<String> requiredTags(final String value) {
        final Set<String> result = tags(value);
        result.removeAll(STYLE_TAGS);
        return result;
    }

    private static Set<String> tags(final String value) {
        final Set<String> result = new HashSet<>();
        final var matcher = TAG.matcher(value == null ? "" : value);
        while (matcher.find()) {
            result.add(matcher.group(1));
        }
        return result;
    }
}
