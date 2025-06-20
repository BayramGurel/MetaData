import java.util.Objects;
import java.util.stream.Stream;

public class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config;

    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = (ExtractorConfiguration)Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            return false;
        }
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) {
            return false;
        }

        String lowerFilename = filename.toLowerCase();

        // 1) Ignore any configured prefixes
        if (config.getIgnoredPrefixes().stream()
                .anyMatch(prefix -> lowerFilename.startsWith(prefix))) {
            return false;
        }

        // 2) Ignore any exact filenames
        if (config.getIgnoredFilenames().stream()
                .anyMatch(ignore -> lowerFilename.equalsIgnoreCase(ignore))) {
            return false;
        }

        // 3) Ignore by extension
        int lastDot = lowerFilename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < lowerFilename.length() - 1) {
            String ext = lowerFilename.substring(lastDot);
            if (config.getIgnoredExtensions().contains(ext)) {
                return false;
            }
        }

        return true;
    }
}
