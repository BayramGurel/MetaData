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

interface IMetadataProvider {
    record ExtractionOutput(Metadata metadata, String text) {}

    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;
}

interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

interface ISourceProcessor {
    void processSource(Path sourcePath, String containerPath,
                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}


public abstract class AbstractSourceProcessor implements ISourceProcessor {

    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");


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

    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null) {
            return "";
        }
        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()) {
            return "";
        }
        String normalizedName = trimmedName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false;
        }
        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()){
            return false;
        }

        if (trimmedName.contains("..")) {
            return true;
        }

        try {
            Path path = Paths.get(trimmedName);
            Path normalizedPath = path.normalize();
            String normalizedString = normalizedPath.toString().replace('\\', '/');

            if (WINDOWS_DRIVE_PATTERN.matcher(normalizedString).matches()) {
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