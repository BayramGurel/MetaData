import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern; // Needed for drive letter check

// --- Kern Interfaces ---

/** Contract voor bestandsfiltering. */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/** Contract voor metadata/tekst extractie. */
interface IMetadataProvider {
    /** Bundelt extractie output. */
    record ExtractionOutput(Metadata metadata, String text) {}

    /** Voert extractie uit. */
    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;
}

/** Contract voor formattering naar CkanResource. */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/** Contract voor bronverwerking. */
interface ISourceProcessor {
    void processSource(Path sourcePath, String containerPath,
                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}


// --- Abstracte Basisklasse ---

/**
 * Abstracte basis voor ISourceProcessor implementaties.
 * Bevat gedeelde dependencies en hulpmethoden.
 */
public abstract class AbstractSourceProcessor implements ISourceProcessor {

    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    // Pattern to check for Windows drive letters at the start of a path
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");


    /** Injecteert gedeelde dependencies. */
    protected AbstractSourceProcessor(IFileTypeFilter fileFilter,
                                      IMetadataProvider metadataProvider,
                                      ICkanResourceFormatter resourceFormatter,
                                      ExtractorConfiguration config) {
        this.fileFilter = Objects.requireNonNull(fileFilter, "File filter cannot be null");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "Metadata provider cannot be null");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter cannot be null");
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
    }

    /** Abstracte methode voor specifieke bronverwerking (te implementeren door subclasses). */
    @Override
    public abstract void processSource(Path sourcePath, String containerPath,
                                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);

    // --- Hulpmethoden ---

    /** Extraheert bestandsnaam uit een pad. Handles null, empty, blank input. */
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null) {
            return "";
        }
        String trimmedName = entryName.trim(); // Trim whitespace first
        if (trimmedName.isEmpty()) {
            return "";
        }
        String normalizedName = trimmedName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    /**
     * Checks for potentially unsafe paths (directory traversal, absolute paths within archives).
     * Relies partly on host OS `isAbsolute()` but prioritizes `..` check.
     */
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false;
        }
        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()){
            return false;
        }

        // 1. Check for directory traversal sequences (most important)
        if (trimmedName.contains("..")) {
            return true;
        }

        try {
            // 2. Check for invalid path syntax (throws InvalidPathException)
            Path path = Paths.get(trimmedName);
            Path normalizedPath = path.normalize();
            String normalizedString = normalizedPath.toString().replace('\\', '/'); // For drive letter check

            // 3. Explicitly check for paths starting with a drive letter (Windows-style absolute)
            // This is reliable across platforms for identifying Windows absolute paths.
            if (WINDOWS_DRIVE_PATTERN.matcher(normalizedString).matches()) {
                return true;
            }

            // 4. Use the OS-specific isAbsolute() check.
            // This will correctly identify "/" as absolute on Unix and not on Windows.
            // It will identify "C:\" as absolute on Windows.
            if (normalizedPath.isAbsolute()) {
                // Log if needed, as this might behave differently depending on context (e.g., inside ZIP vs direct file system)
                // System.err.println("Waarschuwing: Pad '" + trimmedName + "' als absoluut beschouwd door OS isAbsolute().");
                return true;
            }

            // If none of the above, consider the path safe for now
            return false;

        } catch (InvalidPathException e) {
            // Treat invalid syntax as unsafe/unprocessable
            System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + trimmedName);
            return true;
        }
    }
}
