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
import java.util.regex.Pattern;

// --- Interface Definitions ---

// Extracts metadata and text from an InputStream.
interface IMetadataProvider {
    // Holds extracted metadata and text.
    record ExtractionOutput(Metadata metadata, String text) {}

    // Extracts metadata and text.
    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;
}

// Formats data into a CkanResource.
interface ICkanResourceFormatter {
    // Transforms raw data into a CkanResource.
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

// Processes a data source.
interface ISourceProcessor {
    // Processes a source, populating result, error, or ignored lists.
    void processSource(Path sourcePath, String containerPath,
                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}

// --- Abstract Class Definition ---
public abstract class AbstractSourceProcessor implements ISourceProcessor {

    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    // Pattern to detect Windows-style drive letters in paths like "C:/..." or "D:\..."
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");

    // Initializes with dependencies.
    protected AbstractSourceProcessor(IFileTypeFilter fileFilter,
                                      IMetadataProvider metadataProvider,
                                      ICkanResourceFormatter resourceFormatter,
                                      ExtractorConfiguration config) {
        this.fileFilter = Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    @Override
    public abstract void processSource(Path sourcePath, String containerPath,
                                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);

    // Extracts filename from an entry string (e.g., "path/to/file.txt" -> "file.txt").
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null || entryName.isBlank()) {
            return "";
        }
        String normalizedName = entryName.trim().replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    // Checks if an entry name represents an invalid path based on original logic.
    // This method's logic is structured to match the user's initial version for identical behavior.
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) return false;
        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()) return false;

        if (trimmedName.contains("..")) {
            return true;
        }

        try {
            Path path = Paths.get(trimmedName);
            Path normalizedPath = path.normalize();

            if (WINDOWS_DRIVE_PATTERN.matcher(normalizedPath.toString()).matches()) {
                return true;
            }
            if (normalizedPath.isAbsolute()) {
                return true;
            }
            return false;
        } catch (InvalidPathException e) {
            System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + trimmedName);
            return true;
        }
    }
}