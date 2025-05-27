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

    // Determines if a file/entry is relevant based on its name and configured ignore lists
    // (prefixes, filenames, extensions).
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

        String lowerFilename = filename.toLowerCase(); // Use lowercase for case-insensitive checks

        // Check if filename starts with any configured ignored prefixes
        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false;
        }
        // Check if filename exactly matches any configured ignored filenames (case-insensitive)
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false;
        }

        // Check if the file extension is in the configured ignored extensions list
        int lastDotIndex = lowerFilename.lastIndexOf('.');
        // A valid extension requires '.' to be present, not as the first character,
        // and to have characters following it.
        if (lastDotIndex > 0 && lastDotIndex < lowerFilename.length() - 1) {
            String extension = lowerFilename.substring(lastDotIndex); // Note: extension includes the dot (e.g., ".txt")
            if (config.getIgnoredExtensions().contains(extension)) {
                return false;
            }
        }

        return true; // File is relevant if none of the ignore conditions were met
    }
}