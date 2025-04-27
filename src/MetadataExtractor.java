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

public class MetadataExtractor {
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    public MetadataExtractor(IFileTypeFilter fileFilter,
                             IMetadataProvider metadataProvider,
                             ICkanResourceFormatter resourceFormatter,
                             ExtractorConfiguration config) {
        this.fileFilter = fileFilter;
        this.metadataProvider = metadataProvider;
        this.resourceFormatter = resourceFormatter;
        this.config = config;
    }

    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();
        Path sourcePath = null;

        try {
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();

            if (!Files.exists(sourcePath)) {
                errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
            } else if (Files.isDirectory(sourcePath)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund)."));
            } else {
                ISourceProcessor processor;
                String containerPath = sourcePath.toString();

                if (isZipFile(sourcePath)) {
                    System.out.println("INFO: Gedetecteerd als ZIP: " + sourcePath);
                    processor = new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                } else {
                    System.out.println("INFO: Gedetecteerd als enkel bestand: " + sourcePath);
                    processor = new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                }
                processor.processSource(sourcePath, containerPath, results, errors, ignored);
            }
        } catch (InvalidPathException ipe) {
            errors.add(new ProcessingError(sourcePathString, "Ongeldige pad syntax: " + ipe.getMessage()));
            System.err.println("FATAL: Ongeldig pad opgegeven: " + sourcePathString);
        } catch (Exception e) {
            String pathForError = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathForError, "Kritieke fout: " + e.getMessage()));
            System.err.println("FATAL: Onverwachte fout bij '" + pathForError + "': " + e.getMessage());
            e.printStackTrace(System.err);
        }

        return finalizeReport(results, errors, ignored);
    }

    private boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return false;
        }
        String filenameLower = path.getFileName().toString().toLowerCase();
        return config.getSupportedZipExtensions().stream().anyMatch(filenameLower::endsWith);
    }

    private ProcessingReport finalizeReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        System.err.printf("--- Verwerking Samenvatting ---%n");
        System.err.printf("Succesvol: %d, Fouten: %d, Genegeerd: %d%n", results.size(), errors.size(), ignored.size());
        if (!errors.isEmpty()) {
            System.err.println("\n--- Fout Details ---");
            errors.forEach(e -> System.err.printf("  - [%s]: %s%n", e.source(), e.error()));
        }
        System.err.println("--------------------------");
        return new ProcessingReport(results, errors, ignored);
    }

    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");

        String filePath = getFilePathFromArgsOrDefault(args);
        if (filePath == null) {
            System.err.println("FATAL: Geen geldig pad opgegeven. Stoppen.");
            System.exit(1);
        }
        System.out.println("INFO: Te verwerken bron: " + filePath);

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector languageDetector = loadTikaLanguageDetector();
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider();
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormat(languageDetector, config);

        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, config);

        System.out.println("\n--- Start Verwerking ---");
        ProcessingReport report = extractor.processSource(filePath);
        System.out.println("--- Verwerking Voltooid ---");

        printReportAsJson(report);

        System.out.println("\n--- Metadata Extractor Klaar ---");
        if (!report.getErrors().isEmpty()) {
            System.exit(2);
        }
    }

    private static String getFilePathFromArgsOrDefault(String[] args) {
        String defaultPath = ".\\document\\Veg kartering - habitatkaart 2021-2023.zip";
        // String defaultPath = null;

        String pathToCheck = null;
        if (args.length > 0 && args[0] != null && !args[0].isBlank()) {
            pathToCheck = args[0].trim();
            System.out.println("INFO: Gebruik pad uit argument: " + pathToCheck);
        } else if (defaultPath != null) {
            pathToCheck = defaultPath;
            System.out.println("INFO: Geen argument gevonden, gebruik standaard pad: " + pathToCheck);
            System.err.println("WAARSCHUWING: Standaard pad gebruikt. Zorg dat dit correct is!");
        } else {
            System.err.println("FOUT: Geen pad opgegeven. Gebruik: java MetadataExtractor <pad>");
            return null;
        }

        try {
            Path p = Paths.get(pathToCheck);
            if (!Files.exists(p)) {
                System.err.println("FOUT: Opgegeven pad bestaat niet: " + pathToCheck);
                return null;
            }
            return pathToCheck;
        } catch (InvalidPathException ipe) {
            System.err.println("FOUT: Ongeldige pad syntax: '" + pathToCheck + "' - " + ipe.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("FOUT: Onverwachte fout bij valideren pad '" + pathToCheck + "': " + e.getMessage());
            return null;
        }
    }

    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            System.out.println("INFO: Laden Tika taalmodellen...");
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels();
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
        System.err.println("WAARSCHUWING: Taaldetectie uitgeschakeld door laadfout.");
        return null;
    }

    private static void printReportAsJson(ProcessingReport report) {
        if (report == null || report.getResults().isEmpty()) {
            System.out.println("\nINFO: Geen succesvolle resultaten om als JSON te tonen.");
            return;
        }

        System.out.println("\n--- Succesvol Verwerkte Resources (JSON Output) ---");
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            Map<String, Object> jsonRoot = new LinkedHashMap<>();

            List<Map<String, Object>> resourceDataList = report.getResults().stream()
                    .map(CkanResource::getData)
                    .collect(Collectors.toList());

            jsonRoot.put("resources", resourceDataList);

            String jsonOutput = mapper.writeValueAsString(jsonRoot);
            System.out.println(jsonOutput);

        } catch (JsonProcessingException e) {
            System.err.println("FOUT: Kon resultaten niet naar JSON converteren: " + e.getMessage());
            e.printStackTrace(System.err);
        } catch (Exception e) {
            System.err.println("FOUT: Onverwacht bij genereren JSON output: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}