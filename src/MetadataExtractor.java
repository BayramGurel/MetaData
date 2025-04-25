import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hoofdklasse (Facade) die het extractieproces orkestreert.
 * Bevat de main methode voor command-line gebruik.
 */
public class MetadataExtractor {

    // Geïnjecteerde dependencies
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    /** Constructor voor dependency injection. */
    public MetadataExtractor(IFileTypeFilter fileFilter,
                             IMetadataProvider metadataProvider,
                             ICkanResourceFormatter resourceFormatter,
                             ExtractorConfiguration config) {
        this.fileFilter = fileFilter;
        this.metadataProvider = metadataProvider;
        this.resourceFormatter = resourceFormatter;
        this.config = config;
    }

    /**
     * Verwerkt de opgegeven bron (bestand of ZIP).
     * Kiest de juiste processor en delegeert.
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();
        Path sourcePath = null;

        try {
            // Valideer en normaliseer input pad
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();

            // Controleer bestaan en type
            if (!Files.exists(sourcePath)) {
                errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
            } else if (Files.isDirectory(sourcePath)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund)."));
            } else {
                // Bepaal processor type (ZIP of enkel bestand)
                ISourceProcessor processor;
                String containerPath = sourcePath.toString(); // Voor enkel bestand is container = bron

                if (isZipFile(sourcePath)) {
                    System.out.println("INFO: Gedetecteerd als ZIP: " + sourcePath);
                    processor = new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                } else {
                    System.out.println("INFO: Gedetecteerd als enkel bestand: " + sourcePath);
                    processor = new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                }
                // Delegeer verwerking
                processor.processSource(sourcePath, containerPath, results, errors, ignored);
            }
        } catch (InvalidPathException ipe) {
            errors.add(new ProcessingError(sourcePathString, "Ongeldige pad syntax: " + ipe.getMessage()));
            System.err.println("FATAL: Ongeldig pad opgegeven: " + sourcePathString);
        } catch (Exception e) {
            // Vang onverwachte kritieke fouten op
            String pathForError = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathForError, "Kritieke fout: " + e.getMessage()));
            System.err.println("FATAL: Onverwachte fout bij '" + pathForError + "': " + e.getMessage());
            e.printStackTrace(System.err); // Print stack trace voor debuggen
        }

        // Finaliseer en retourneer rapport
        return finalizeReport(results, errors, ignored);
    }

    /** Controleert of pad een ondersteund ZIP bestand is. */
    private boolean isZipFile(Path path) {
        // Moet een bestaand, regulier bestand zijn
        if (path == null || !Files.isRegularFile(path)) {
            return false;
        }
        String filenameLower = path.getFileName().toString().toLowerCase();
        // Check tegen geconfigureerde ZIP extensies
        return config.getSupportedZipExtensions().stream().anyMatch(filenameLower::endsWith);
    }

    /** Finaliseert rapport en logt samenvatting naar System.err. */
    private ProcessingReport finalizeReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Log samenvatting
        System.err.printf("--- Verwerking Samenvatting ---%n");
        System.err.printf("Succesvol: %d, Fouten: %d, Genegeerd: %d%n", results.size(), errors.size(), ignored.size());
        // Log foutdetails indien aanwezig
        if (!errors.isEmpty()) {
            System.err.println("\n--- Fout Details ---");
            errors.forEach(e -> System.err.printf("  - [%s]: %s%n", e.source(), e.error()));
        }
        System.err.println("--------------------------");
        // Retourneer onveranderlijk rapport object
        return new ProcessingReport(results, errors, ignored);
    }

    // --- Main Methode (Command-Line Entry Point) ---

    /** CLI entry point. */
    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");

        // Haal bestandspad op (uit args of default)
        String filePath = getFilePathFromArgsOrDefault(args);
        if (filePath == null) {
            System.err.println("FATAL: Geen geldig pad opgegeven. Stoppen.");
            System.exit(1); // Stop met foutcode
        }
        System.out.println("INFO: Te verwerken bron: " + filePath);

        // Initialiseer componenten
        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector languageDetector = loadTikaLanguageDetector(); // Probeer taal detector te laden
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider();
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormat(languageDetector, config);

        // Maak extractor (Facade) instantie
        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, config);

        // Voer extractieproces uit
        System.out.println("\n--- Start Verwerking ---");
        ProcessingReport report = extractor.processSource(filePath);
        System.out.println("--- Verwerking Voltooid ---");

        // Print succesvolle resultaten als JSON
        printReportAsJson(report);

        System.out.println("\n--- Metadata Extractor Klaar ---");
        // Optioneel: exit code gebaseerd op fouten
        if (!report.getErrors().isEmpty()) {
            System.exit(2); // Stop met foutcode indien errors
        }
    }

    /** Haalt bestandspad op uit command-line args of gebruikt hardcoded default. */
    private static String getFilePathFromArgsOrDefault(String[] args) {
        // --- !! BELANGRIJK !! ---
        // Pas dit standaard pad aan naar een bestaand testbestand,
        // of verwijder het en geef altijd een pad mee als argument.
        String defaultPath = ".\\document\\Veg kartering - habitatkaart 2021-2023.zip"; // Voorbeeld - AANPASSEN!
        // String defaultPath = null; // Alternatief: geen default
        // -------------------------

        String pathToCheck = null;
        // Probeer pad uit eerste argument
        if (args.length > 0 && args[0] != null && !args[0].isBlank()) {
            pathToCheck = args[0].trim();
            System.out.println("INFO: Gebruik pad uit argument: " + pathToCheck);
        } else if (defaultPath != null) {
            // Gebruik default pad indien geen argument
            pathToCheck = defaultPath;
            System.out.println("INFO: Geen argument gevonden, gebruik standaard pad: " + pathToCheck);
            System.err.println("WAARSCHUWING: Standaard pad gebruikt. Zorg dat dit correct is!");
        } else {
            // Geen argument en geen default
            System.err.println("FOUT: Geen pad opgegeven. Gebruik: java MetadataExtractor <pad>");
            return null;
        }

        // Valideer gekozen pad
        try {
            Path p = Paths.get(pathToCheck);
            if (!Files.exists(p)) {
                System.err.println("FOUT: Opgegeven pad bestaat niet: " + pathToCheck);
                return null;
            }
            return pathToCheck; // Pad lijkt geldig
        } catch (InvalidPathException ipe) {
            System.err.println("FOUT: Ongeldige pad syntax: '" + pathToCheck + "' - " + ipe.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("FOUT: Onverwachte fout bij valideren pad '" + pathToCheck + "': " + e.getMessage());
            return null;
        }
    }

    /** Laadt Tika taal detector. Geeft null terug bij falen. */
    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            System.out.println("INFO: Laden Tika taalmodellen...");
            // Gebruik aanbevolen Optimaize detector
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels(); // Laad taalprofielen
            System.out.println("INFO: Tika taalmodellen geladen.");
            return detector;
        } catch (NoClassDefFoundError e) {
            System.err.println("FOUT: Kon Tika taaldetectie klassen niet vinden.");
            System.err.println("Zorg dat 'tika-langdetect' (en dependencies) in classpath staan.");
        } catch (IOException e) {
            System.err.println("FOUT: Kon Tika taalmodellen niet laden: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("FOUT: Onverwacht bij initialiseren Tika taaldetector: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        // Geef null terug bij laadfout
        System.err.println("WAARSCHUWING: Taaldetectie uitgeschakeld door laadfout.");
        return null;
    }

    /** Print succesvolle resultaten uit rapport als JSON naar System.out. */
    private static void printReportAsJson(ProcessingReport report) {
        // Controleer of er resultaten zijn
        if (report == null || report.getResults().isEmpty()) {
            System.out.println("\nINFO: Geen succesvolle resultaten om als JSON te tonen.");
            return;
        }

        System.out.println("\n--- Succesvol Verwerkte Resources (JSON Output) ---");
        try {
            // Gebruik Jackson ObjectMapper voor JSON serialisatie
            ObjectMapper mapper = new ObjectMapper();
            // Configureer voor leesbare ("pretty print") output
            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            // Maak root object voor JSON structuur {"resources": [...]}
            Map<String, Object> jsonRoot = new LinkedHashMap<>();

            // Extraheer data maps uit CkanResource objecten
            List<Map<String, Object>> resourceDataList = report.getResults().stream()
                    .map(CkanResource::getData) // Haal interne map op
                    .collect(Collectors.toList());

            jsonRoot.put("resources", resourceDataList);

            // Converteer root map naar JSON en print
            String jsonOutput = mapper.writeValueAsString(jsonRoot);
            System.out.println(jsonOutput);

        } catch (JsonProcessingException e) {
            System.err.println("FOUT: Kon resultaten niet naar JSON converteren: " + e.getMessage());
            e.printStackTrace(System.err); // Nuttig voor debuggen JSON issues
        } catch (Exception e) {
            System.err.println("FOUT: Onverwacht bij genereren JSON output: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
} // Einde MetadataExtractor klasse
