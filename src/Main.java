// Main.java

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry point for the metadata extractor CLI. Contains main() plus
 * all static helpers and orchestration logic.
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String SOURCE_PATH = "./document";
    private static final String OUTPUT_DIR  = "reports";

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
     * Loads the Tika language detector, logs only on failure.
     */
    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels();
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
