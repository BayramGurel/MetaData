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
import java.util.regex.Pattern; // Nodig voor drive letter check

// --- Kern Interfaces ---

/** Contract voor bestandsfiltering. */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/** Contract voor metadata/tekst extractie. */
interface IMetadataProvider {
    /** Record voor extractie output (metadata en tekst). */
    record ExtractionOutput(Metadata metadata, String text) {}

    /** Voert extractie uit. */
    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;
}

/** Contract voor formattering naar CkanResource. */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/** Contract voor bronverwerking (bestand of archief). */
interface ISourceProcessor {
    void processSource(Path sourcePath, String containerPath,
                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}


// --- Abstracte Basisklasse ---

/**
 * Abstracte basisklasse voor {@link ISourceProcessor} implementaties.
 * Bevat gedeelde componenten en hulpmethoden.
 */
public abstract class AbstractSourceProcessor implements ISourceProcessor {

    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    // Patroon om Windows drive letters aan het begin van een pad te herkennen
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");


    /** Constructor voor dependency injection. */
    protected AbstractSourceProcessor(IFileTypeFilter fileFilter,
                                      IMetadataProvider metadataProvider,
                                      ICkanResourceFormatter resourceFormatter,
                                      ExtractorConfiguration config) {
        this.fileFilter = Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    /** Abstracte methode voor specifieke bronverwerking (implementeren in subclass). */
    @Override
    public abstract void processSource(Path sourcePath, String containerPath,
                                       List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);

    // --- Hulpmethoden ---

    /** Extraheert bestandsnaam uit een pad string. Handelt null/lege/blanke input af. */
    protected static String getFilenameFromEntry(String entryName) {
        if (entryName == null) {
            return "";
        }
        String trimmedName = entryName.trim(); // Verwijder eerst witruimte
        if (trimmedName.isEmpty()) {
            return "";
        }
        // Normaliseer padscheidingstekens
        String normalizedName = trimmedName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        // Geef deel na laatste slash terug, of de hele naam
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    /** Controleert op potentieel onveilige paden (bv. directory traversal, absolute paden). */
    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false;
        }
        String trimmedName = entryName.trim();
        if (trimmedName.isEmpty()){
            return false; // Leeg pad is niet direct ongeldig
        }

        // 1. Belangrijkste check: directory traversal
        if (trimmedName.contains("..")) {
            return true;
        }

        try {
            // 2. Controleer op ongeldige pad syntax (gooit InvalidPathException)
            Path path = Paths.get(trimmedName);
            Path normalizedPath = path.normalize(); // Normaliseer bv. a/./b -> a/b
            String normalizedString = normalizedPath.toString().replace('\\', '/'); // Voor drive letter check

            // 3. Expliciete check op Windows drive letters (betrouwbaar cross-platform)
            if (WINDOWS_DRIVE_PATTERN.matcher(normalizedString).matches()) {
                return true;
            }

            // 4. Gebruik OS-specifieke isAbsolute() check.
            //    Identificeert "/" als absoluut op Unix, "C:\" op Windows.
            if (normalizedPath.isAbsolute()) {
                return true;
            }

            // Geen onveilige patronen gevonden
            return false;

        } catch (InvalidPathException e) {
            // Behandel ongeldige syntax als onveilig/niet-verwerkbaar
            System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + trimmedName);
            return true;
        }
    }
}
