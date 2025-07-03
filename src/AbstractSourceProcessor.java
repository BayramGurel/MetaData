import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for processors with file filtering and path checks.
 */
public abstract class AbstractSourceProcessor implements ISourceProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSourceProcessor.class);
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");

    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    protected AbstractSourceProcessor(
            IFileTypeFilter fileFilter,
            IMetadataProvider metadataProvider,
            ICkanResourceFormatter resourceFormatter,
            ExtractorConfiguration config
    ) {
        this.fileFilter       = Objects.requireNonNull(fileFilter,       "File filter must not be null");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "Metadata provider must not be null");
        this.resourceFormatter= Objects.requireNonNull(resourceFormatter,"Resource formatter must not be null");
        this.config           = Objects.requireNonNull(config,           "Configuration must not be null");
    }

    @Override
    public abstract void processSource(
            Path sourcePath,
            String containerPath,
            List<CkanResource> results,
            List<ProcessingError> errors,
            List<IgnoredEntry> ignored
    );

    /**
     * Extracts just the filename portion from a ZIP entry name.
     *
     * @param entryName raw entry name as stored inside the archive
     * @return the filename part or an empty string if the entryName is null or blank
     */
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            return "";
        }
        String normalized = entryName.trim().replace('\\', '/');
        int lastSlash = normalized.lastIndexOf('/');
        return (lastSlash >= 0) ? normalized.substring(lastSlash + 1) : normalized;
    }

    /**
     * Checks whether the given ZIP entry refers to an unsafe path (Zip-Slip vulnerability).
     *
     * @param entryName the entry name from the archive
     * @return true if the entry is considered invalid and should be skipped
     */
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            return false; // empty names are ignored but not treated as a security risk
        }

        final String normalized = entryName.trim().replace('\\', '/');

        // Reject absolute paths (Unix or Windows)
        if (normalized.startsWith("/") || WINDOWS_DRIVE_PATTERN.matcher(normalized).matches()) {
            LOGGER.warn("Skipping entry with absolute path: {}", entryName);
            return true;
        }

        // Reject any segment that is exactly ".."
        for (String segment : normalized.split("/")) {
            if ("..".equals(segment)) {
                LOGGER.warn("Skipping entry with path traversal segment: {}", entryName);
                return true;
            }
        }

        return false;
    }
}
