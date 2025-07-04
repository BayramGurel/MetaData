// MetadataExtractor.java

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Core logic for metadata extraction. Does not contain a main method
 * or any static utilities.
 */
public class MetadataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataExtractor.class);

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
}
