import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.InvalidPathException; // Import toegevoegd
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

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

    /** Extraheert bestandsnaam uit een pad. */
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null || entryName.isEmpty()) {
            return "";
        }
        // Normaliseer naar forward slashes voor consistentie
        String normalizedName = entryName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        // Return deel na laatste slash, of hele string indien geen slash
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    /** Basis check voor potentieel onveilige paden (bv. directory traversal). */
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false; // Null is niet per se ongeldig, maar wordt elders afgehandeld
        }
        // Directe check op meest voorkomende traversal poging
        if (entryName.contains("..")) {
            return true;
        }
        try {
            // Check of het pad absoluut wordt na normalisatie.
            // Dit kan platform-afhankelijk zijn.
            // Gooit InvalidPathException bij ongeldige syntax.
            return Paths.get(entryName).normalize().isAbsolute();
        } catch (InvalidPathException e) {
            // Behandel ongeldige syntax als onveilig/niet-verwerkbaar.
            System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + entryName);
            return true;
        }
    }
}
