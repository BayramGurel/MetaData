import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;

public class MetadataExtractor {
    /** Default to the `document` directory */
    private static final String SOURCE_PATH = ".\\document";

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    public MetadataExtractor(
            IFileTypeFilter fileFilter,
            IMetadataProvider metadataProvider,
            ICkanResourceFormatter resourceFormatter,
            ExtractorConfiguration config) {
        this.fileFilter = Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();
        Path sourcePath = null;

        try {
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();
            if (!Files.exists(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
                return new ProcessingReport(results, errors, ignored);
            }

            if (Files.isDirectory(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund als enkel bestand)."));
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
        if (path != null && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            String name = path.getFileName().toString().toLowerCase();
            return config.getSupportedZipExtensions().stream()
                    .anyMatch(ext -> name.endsWith(ext.toLowerCase()));
        }
        return false;
    }

    public static void main(String[] args) {
        // Default to the document directory if no argument is given
        String inputPath = (args.length > 0) ? args[0] : SOURCE_PATH;
        Path src = Paths.get(inputPath).toAbsolutePath().normalize();

        if (!Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
            System.err.println("FATAL: Bron niet gevonden: " + src);
            System.exit(1);
        }

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector langDetector = loadTikaLanguageDetector();

        if (Files.isDirectory(src, LinkOption.NOFOLLOW_LINKS)) {
            // Process each regular file in the directory
            try (Stream<Path> stream = Files.list(src)) {
                stream
                        .filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                        .forEach(p -> {
                            System.out.println("Processing " + p.getFileName());

                            // Derive package ID from filename (e.g. "foo.zip" → "foo")
                            String baseName = p.getFileName().toString();
                            int dot = baseName.lastIndexOf('.');
                            String packageId = (dot > 0) ? baseName.substring(0, dot) : baseName;

                            MetadataExtractor extractor = new MetadataExtractor(
                                    new DefaultFileTypeFilter(config),
                                    new TikaMetadataProvider(),
                                    new DefaultCkanResourceFormat(langDetector, config, packageId),
                                    config
                            );

                            ProcessingReport rpt = extractor.processSource(p.toString());
                            String safe = baseName.replaceAll("[^a-zA-Z0-9._-]", "_");
                            String outName = "report-" + safe + ".json";
                            writeJson(rpt, outName);

                            if (!rpt.getErrors().isEmpty()) {
                                System.err.println("Voltooid met fouten voor " + p.getFileName() +
                                        ". Zie " + outName);
                            } else {
                                System.out.println("Klaar! Zie " + outName);
                            }
                        });
            } catch (IOException e) {
                System.err.println("FOUT: Kan map niet lezen: " + e.getMessage());
                System.exit(1);
            }
        } else {
            // Single file → derive packageId the same way
            String baseName = src.getFileName().toString();
            int dot = baseName.lastIndexOf('.');
            String packageId = (dot > 0) ? baseName.substring(0, dot) : baseName;

            MetadataExtractor extractor = new MetadataExtractor(
                    new DefaultFileTypeFilter(config),
                    new TikaMetadataProvider(),
                    new DefaultCkanResourceFormat(langDetector, config, packageId),
                    config
            );

            ProcessingReport report = extractor.processSource(src.toString());
            writeJson(report, "report.json");

            if (!report.getErrors().isEmpty()) {
                System.err.println("Voltooid met fouten. Zie report.json");
                System.exit(2);
            } else {
                System.out.println("Klaar! Zie report.json");
            }
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

    private static void writeJson(ProcessingReport rpt, String fileName) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("resources",
                rpt.getResults().stream()
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
