import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
/**
 * Base class for processors with file filtering and path checks.
 */


public abstract class AbstractSourceProcessor implements ISourceProcessor {
    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");

    protected AbstractSourceProcessor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ExtractorConfiguration config) {
        this.fileFilter = (IFileTypeFilter)Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = (IMetadataProvider)Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = (ICkanResourceFormatter)Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = (ExtractorConfiguration)Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    @Override
    public abstract void processSource(Path sourcePath,
                                       String containerPath,
                                       List<CkanResource> results,
                                       List<ProcessingError> errors,
                                       List<IgnoredEntry> ignored);

    /**
     * Extract just the filename portion from a ZIP entry name.
     *
     * @param entryName raw entry name as stored inside the archive
     * @return the filename part or an empty string when the name is blank
     */
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName != null && !entryName.isBlank()) {
            String normalizedName = entryName.trim().replace('\\', '/');
            int lastSlash = normalizedName.lastIndexOf('/');
            return lastSlash >= 0 ? normalizedName.substring(lastSlash + 1) : normalizedName;
        }
        return "";
    }

    /**
     * Checks whether the given entry refers to an unsafe path.
     *
     * @param entryName the entry name from the archive
     * @return {@code true} if the entry is considered invalid
     */
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false;
        }

        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()) {
            return false;
        }

        if (trimmedName.contains("..")) {
            return true;
        }

        try {
            Path path = Paths.get(trimmedName);
            Path normalizedPath = path.normalize();
            if (WINDOWS_DRIVE_PATTERN.matcher(normalizedPath.toString()).matches()) {
                return true;
            }
            return normalizedPath.isAbsolute();
        } catch (InvalidPathException e) {
            System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + trimmedName);
            return true;
        }
    }
}
