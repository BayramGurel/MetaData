import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command-line tool to extract metadata and produce CKAN reports.
 */
public class MetadataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataExtractor.class);

    /** Default source directory. */
    private static final String SOURCE_PATH = "./document";
    /** Default output directory for reports. */
    private static final String OUTPUT_DIR = "reports";

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    public MetadataExtractor(
            IFileTypeFilter fileFilter,
            IMetadataProvider metadataProvider,
            ICkanResourceFormatter resourceFormatter,
            ExtractorConfiguration config
    ) {
        this.fileFilter        = Objects.requireNonNull(fileFilter,        "File filter must not be null");
        this.metadataProvider  = Objects.requireNonNull(metadataProvider,  "Metadata provider must not be null");
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter, "Resource formatter must not be null");
        this.config            = Objects.requireNonNull(config,            "Configuration must not be null");
    }

    /**
     * Processes a single source (file or ZIP) and returns the processing report.
     * Prints a Dutch summary line at the end and suppresses internal INFO logs.
     *
     * @param sourcePathString path to the file or archive
     * @return report containing resources, errors, and ignored entries
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();

        Path sourcePath = null;
        try {
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();

            if (!Files.exists(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                // only log at WARN
                LOGGER.warn("Source not found: {}", sourcePathString);
                errors.add(new ProcessingError(sourcePathString, "Source not found."));
                return new ProcessingReport(results, errors, ignored);
            }
            if (Files.isDirectory(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                LOGGER.warn("Skipping directory: {}", sourcePath);
                ignored.add(new IgnoredEntry(sourcePathString, "Source is a directory (not supported)."));
                return new ProcessingReport(results, errors, ignored);
            }

            ISourceProcessor processor = isZipFile(sourcePath)
                    ? new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config)
                    : new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);

            processor.processSource(sourcePath, sourcePathString, results, errors, ignored);

        } catch (Exception e) {
            String pathForError = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            String msg = (e.getMessage() != null) ? e.getMessage() : "Unexpected error";
            errors.add(new ProcessingError(pathForError, "Critical error: " + msg));
            LOGGER.error("❗ Critical error processing {}:", pathForError, e);
        }

        // Dutch summary line, printed to stdout
        System.out.printf(
                "Samenvatting: %d resources, %d fouten, %d genegeerd%n",
                results.size(), errors.size(), ignored.size()
        );

        return new ProcessingReport(results, errors, ignored);
    }

    /**
     * Checks if the given path is a regular file with a supported ZIP extension.
     */
    private boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return config.getSupportedZipExtensions().stream()
                .map(String::toLowerCase)
                .anyMatch(name::endsWith);
    }

    /**
     * Loads the Tika language detector, logs only on failure.
     */
    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels();
            // suppress success INFO
            return detector;
        } catch (Exception e) {
            LOGGER.warn("Language detector could not be initialized: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Writes a combined JSON report to `<OUTPUT_DIR>/all-reports.json`.
     * Logs only on error.
     */
    private static void writeAllJson(Map<String, List<Map<String, Object>>> masterMap, Path outDir) {
        try {
            if (Files.notExists(outDir)) {
                Files.createDirectories(outDir);
            }
            Path outFile = outDir.resolve("all-reports.json");
            ObjectMapper mapper = new ObjectMapper()
                    .enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(outFile.toFile(), masterMap);
        } catch (IOException e) {
            LOGGER.error("Failed to write JSON report: {}", e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        String inputPath = (args.length > 0) ? args[0] : SOURCE_PATH;
        Path src = Paths.get(inputPath).toAbsolutePath().normalize();

        if (!Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
            LOGGER.error("FATAL: Source not found: {}", src);
            System.exit(1);
        }

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector langDetector = loadTikaLanguageDetector();
        Map<String, List<Map<String, Object>>> allReports = new LinkedHashMap<>();

        try {
            if (Files.isDirectory(src, LinkOption.NOFOLLOW_LINKS)) {
                try (Stream<Path> stream = Files.list(src)) {
                    stream.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                            .forEach(p -> processPath(p, config, langDetector, allReports));
                }
            } else {
                processPath(src, config, langDetector, allReports);
            }
            writeAllJson(allReports, Paths.get(OUTPUT_DIR));
        } catch (IOException e) {
            LOGGER.error("FATAL: Could not read directory {}: {}", src, e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Processes one file path and accumulates its report.
     */
    private static void processPath(
            Path path,
            ExtractorConfiguration config,
            LanguageDetector langDetector,
            Map<String, List<Map<String, Object>>> allReports
    ) {
        String baseName = path.getFileName().toString();
        int dot = baseName.lastIndexOf('.');
        String packageId = (dot > 0) ? baseName.substring(0, dot) : baseName;

        MetadataExtractor extractor = new MetadataExtractor(
                new DefaultFileTypeFilter(config),
                new TikaMetadataProvider(),
                new DefaultCkanResourceFormat(langDetector, config, packageId),
                config
        );

        ProcessingReport report = extractor.processSource(path.toString());
        allReports.put(packageId,
                report.getResults().stream()
                        .map(CkanResource::getData)
                        .collect(Collectors.toList()));

        if (!report.getErrors().isEmpty()) {
            LOGGER.warn("⚠ Completed with errors for '{}'", packageId);
        }
    }
}
