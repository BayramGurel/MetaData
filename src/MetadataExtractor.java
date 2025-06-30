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
    /** Default output directory */
    private static final String OUTPUT_DIR = "reports";

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

    /**
     * Process a single source (file or zip) and return a report.
     */
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

    /**
     * Write a single JSON containing all packages under their packageId keys.
     */
    private static void writeAllJson(Map<String, List<Map<String, Object>>> master, Path outDir) {
        try {
            if (!Files.exists(outDir)) {
                Files.createDirectories(outDir);
            }
            Path outFile = outDir.resolve("all-reports.json");
            ObjectMapper mapper = new ObjectMapper()
                    .enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(outFile.toFile(), master);
        } catch (IOException e) {
            System.err.println("Fout bij schrijven JSON: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String inputPath = (args.length > 0) ? args[0] : SOURCE_PATH;
        Path src = Paths.get(inputPath).toAbsolutePath().normalize();
        if (!Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
            System.err.println("FATAL: Bron niet gevonden: " + src);
            System.exit(1);
        }

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector langDetector = loadTikaLanguageDetector();
        Map<String, List<Map<String, Object>>> allReports = new LinkedHashMap<>();

        try {
            if (Files.isDirectory(src, LinkOption.NOFOLLOW_LINKS)) {
                try (Stream<Path> stream = Files.list(src)) {
                    stream.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                            .forEach(p -> {
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
                                allReports.put(packageId,
                                        rpt.getResults().stream()
                                                .map(CkanResource::getData)
                                                .collect(Collectors.toList()));

                                if (!rpt.getErrors().isEmpty()) {
                                    System.err.println("Voltooid met fouten voor " + baseName);
                                }
                            });
                }
            } else {
                String baseName = src.getFileName().toString();
                int dot = baseName.lastIndexOf('.');
                String packageId = (dot > 0) ? baseName.substring(0, dot) : baseName;

                MetadataExtractor extractor = new MetadataExtractor(
                        new DefaultFileTypeFilter(config),
                        new TikaMetadataProvider(),
                        new DefaultCkanResourceFormat(langDetector, config, packageId),
                        config
                );
                ProcessingReport rpt = extractor.processSource(src.toString());
                allReports.put(packageId,
                        rpt.getResults().stream()
                                .map(CkanResource::getData)
                                .collect(Collectors.toList()));

                if (!rpt.getErrors().isEmpty()) {
                    System.err.println("Voltooid met fouten voor " + baseName);
                }
            }

            // write to designated OUTPUT_DIR folder
            writeAllJson(allReports, Paths.get(OUTPUT_DIR));
            System.out.println("Klaar! Zie " + OUTPUT_DIR + "/all-reports.json");

        } catch (IOException e) {
            System.err.println("FOUT: Kan map niet lezen: " + e.getMessage());
            System.exit(1);
        }
    }
}
