import java.util.Locale;
import java.util.Objects;
// Assuming ExtractorConfiguration and AbstractSourceProcessor are defined and accessible elsewhere.

// Defines a contract for filtering file types based on entry names.
@FunctionalInterface
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

// Default implementation of IFileTypeFilter using configuration-based rules
// to determine if a file type is relevant.
public class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config;

    // Initializes with the extractor configuration.
    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    /**
     * Determines if a file/entry is relevant based on its name and the configured
     * ignore lists (prefixes, filenames and extensions).
     */
    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isBlank()) { // Handle null or blank entry names
            return false;
        }

        // Assumes AbstractSourceProcessor.getFilenameFromEntry returns a simple, non-null filename (can be empty)
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) { // If no actual filename part (e.g., entryName was just "/")
            return false;
        }

        String lowerFilename = filename.toLowerCase(Locale.ROOT); // Use locale independent lower-case

        // Check if filename starts with any configured ignored prefixes
        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false;
        }
        // Check if filename exactly matches any configured ignored filenames (case-insensitive)
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false;
        }

        String extension = getExtension(lowerFilename);
        if (!extension.isEmpty() && config.getIgnoredExtensions().contains(extension)) {
            return false;
        }

        return true; // File is relevant if none of the ignore conditions were met
    }

    /** Returns the lowercase file extension (including dot) or empty string. */
    private static String getExtension(String filename) {
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot).toLowerCase(Locale.ROOT);
        }
        return "";
    }
}
