import java.util.Objects;

/**
 * Standard implementation of {@link IFileTypeFilter}.
 * Filters files/entries based on configuration (extensions, prefixes, names).
 */
public class DefaultFileTypeFilter implements IFileTypeFilter {

    private final ExtractorConfiguration config;

    /** Constructor, injects configuration. */
    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
    }

    /** Determines if a file/entry is relevant for processing. */
    @Override
    public boolean isFileTypeRelevant(String entryName) {
        // Basic validity check
        if (entryName == null || entryName.isBlank()) {
            return false;
        }

        // Extract filename (part after last slash)
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) {
            return false; // Likely a directory entry ending with '/'
        }

        String lowerFilename = filename.toLowerCase(); // For case-insensitive checks

        // Check against ignore lists from configuration
        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false; // Ignored prefix found
        }
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false; // Ignored filename found
        }

        // Check ignored extensions (if an extension exists)
        int lastDotIndex = lowerFilename.lastIndexOf('.');
        // Ensure dot exists, is not the first char, and has chars after it
        if (lastDotIndex > 0 && lastDotIndex < lowerFilename.length() - 1) {
            String extension = lowerFilename.substring(lastDotIndex); // Includes the dot
            if (config.getIgnoredExtensions().contains(extension)) {
                return false; // Ignored extension found
            }
        }

        // If no ignore rule matched, the file is relevant
        return true;
    }
}
