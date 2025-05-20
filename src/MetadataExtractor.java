import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

// Main class for orchestrating metadata extraction from a hard-coded ZIP.
public class MetadataExtractor {
    private static final String SOURCE_ZIP = ".\\document\\Veg kartering - habitatkaart 2021-2023.zip";

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    public MetadataExtractor(IFileTypeFilter fileFilter,
                             IMetadataProvider metadataProvider,
                             ICkanResourceFormatter resourceFormatter,
                             ExtractorConfiguration config) {
        this.fileFilter        = Objects.requireNonNull(fileFilter,        "File filter mag niet null zijn");
        this.metadataProvider  = Objects.requireNonNull(metadataProvider,  "Metadata provider mag niet null zijn");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config            = Objects.requireNonNull(config,            "Configuratie mag niet null zijn");
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
                return new ProcessingReport(results, errors, ignored);
            }
            if (Files.isDirectory(sourcePath)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund)."));
                return new ProcessingReport(results, errors, ignored);
            }

            ISourceProcessor processor = isZipFile(sourcePath)
                    ? new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config)
                    : new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);

            processor.processSource(sourcePath, sourcePath.toString(), results, errors, ignored);

        } catch (Exception e) {
            String pathForError = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathForError, "Kritieke fout: " + e.getMessage()));
            e.printStackTrace(System.err);
        }

        System.err.printf("Samenvatting: %d resources, %d fouten, %d genegeerd%n",
                results.size(), errors.size(), ignored.size());

        return new ProcessingReport(results, errors, ignored);
    }

    private boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) return false;
        String name = path.getFileName().toString().toLowerCase();
        return config.getSupportedZipExtensions().stream()
                .anyMatch(ext -> name.endsWith(ext.toLowerCase()));
    }

    public static void main(String[] args) {
        Path src = Paths.get(SOURCE_ZIP).toAbsolutePath().normalize();
        if (!Files.exists(src)) {
            System.err.println("FATAL: Hard-coded ZIP niet gevonden: " + src);
            System.exit(1);
        }

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector langDetector = loadTikaLanguageDetector();
        MetadataExtractor extractor = new MetadataExtractor(
                new DefaultFileTypeFilter(config),
                new TikaMetadataProvider(),
                new DefaultCkanResourceFormat(langDetector, config),
                config
        );

        ProcessingReport report = extractor.processSource(SOURCE_ZIP);
        writeJson(report, "report.json");

        if (!report.getErrors().isEmpty()) {
            System.err.println("Voltooid met fouten. Zie report.json");
            System.exit(2);
        } else {
            System.out.println("Klaar! Zie report.json");
        }
    }

    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            LanguageDetector ld = OptimaizeLangDetector.getDefaultLanguageDetector();
            ld.loadModels();
            return ld;
        } catch (Exception e) {
            System.err.println("Waarschuwing: taaldetectie niet geladen: " + e.getMessage());
            return null;
        }
    }

    // UPDATED: only output resources in JSON
    private static void writeJson(ProcessingReport rpt, String fileName) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("resources", rpt.getResults().stream()
                .map(CkanResource::getData)
                .collect(Collectors.toList()));

        try {
            ObjectMapper mapper = new ObjectMapper()
                    .enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(Paths.get(fileName).toFile(), root);
        } catch (IOException e) {
            System.err.println("Fout bij schrijven JSON: " + e.getMessage());
        }
    }
}
