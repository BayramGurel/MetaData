import java.util.Locale;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of IFileTypeFilter.
 * Filters files by name and extension based on configured ignore rules.
 */
public class DefaultFileTypeFilter implements IFileTypeFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFileTypeFilter.class);

    private final ExtractorConfiguration config;

    /**
     * @param config extraction configuration; must not be null
     * @throws NullPointerException if config is null
     */
    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "ExtractorConfiguration must not be null");
    }

    /**
     * {@inheritDoc}
     *
     * Returns {@code true} if the given archive entry should be processed
     * (i.e. it’s not blank, not in a .gdb, not matching any ignore rule).
     */
    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            LOGGER.debug("Ignoring null or blank entryName");
            return false;
        }

        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) {
            LOGGER.debug("Ignoring entry with empty filename: {}", entryName);
            return false;
        }

        String lowerFilename = filename.toLowerCase(Locale.ROOT);
        String lowerEntry    = entryName.toLowerCase(Locale.ROOT);

        // Ignore any file inside an Esri geodatabase (.gdb)
        if (lowerEntry.contains(".gdb/") || lowerEntry.endsWith(".gdb")) {
            LOGGER.debug("Ignoring Esri geodatabase entry: {}", entryName);
            return false;
        }

        // 1) Ignore any configured prefixes
        if (config.getIgnoredPrefixes().stream()
                .map(p -> p.toLowerCase(Locale.ROOT))
                .anyMatch(lowerFilename::startsWith)) {
            LOGGER.debug("Ignoring entry due to prefix rule: {}", entryName);
            return false;
        }

        // 2) Ignore any exact filenames
        if (config.getIgnoredFilenames().stream()
                .anyMatch(name -> name != null
                        && lowerFilename.equals(name.toLowerCase(Locale.ROOT)))) {
            LOGGER.debug("Ignoring entry due to filename rule: {}", entryName);
            return false;
        }

        // 3) Ignore by extension
        int lastDot = lowerFilename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < lowerFilename.length() - 1) {
            String ext = lowerFilename.substring(lastDot);
            if (config.getIgnoredExtensions().contains(ext)) {
                LOGGER.debug("Ignoring entry due to extension rule ({}): {}", ext, entryName);
                return false;
            }
        }

        return true;
    }
}
