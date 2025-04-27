import java.util.Objects;

interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

public class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config;

    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            return false;
        }

        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) {
            return false;
        }

        String lowerFilename = filename.toLowerCase();

        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false;
        }
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false;
        }

        int lastDotIndex = lowerFilename.lastIndexOf('.');

        if (lastDotIndex > 0 && lastDotIndex < lowerFilename.length() - 1) {
            String extension = lowerFilename.substring(lastDotIndex);
            if (config.getIgnoredExtensions().contains(extension)) {
                return false;
            }
        }

        return true;
    }
}