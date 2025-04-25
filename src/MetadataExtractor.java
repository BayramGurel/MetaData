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
 * Main class orchestrating the metadata extraction process (Facade).
 * Includes the main method for command-line execution.
 */
public class MetadataExtractor {

    // Injected dependencies
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    /** Constructor injecting dependencies. */
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
     * Processes the specified source (file or ZIP archive).
     * Selects the appropriate processor and delegates the work.
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();
        Path sourcePath = null;

        try {
            // Validate and normalize the input path
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();

            // Check existence and type
            if (!Files.exists(sourcePath)) {
                errors.add(new ProcessingError(sourcePathString, "Source not found."));
            } else if (Files.isDirectory(sourcePath)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Source is a directory (not supported)."));
            } else {
                // Determine processor type (ZIP or single file)
                ISourceProcessor processor;
                String containerPath = sourcePath.toString(); // For single file, container is the file itself

                if (isZipFile(sourcePath)) {
                    System.out.println("INFO: Detected ZIP archive: " + sourcePath);
                    processor = new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                } else {
                    System.out.println("INFO: Detected single file: " + sourcePath);
                    processor = new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                }
                // Delegate processing
                processor.processSource(sourcePath, containerPath, results, errors, ignored);
            }
        } catch (InvalidPathException ipe) {
            errors.add(new ProcessingError(sourcePathString, "Invalid path syntax: " + ipe.getMessage()));
            System.err.println("FATAL: Invalid path provided: " + sourcePathString);
        } catch (Exception e) {
            // Catch unexpected critical errors during setup or processing
            String pathForError = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathForError, "Critical error: " + e.getMessage()));
            System.err.println("FATAL: Unexpected error processing '" + pathForError + "': " + e.getMessage());
            e.printStackTrace(System.err); // Print stack trace for debugging
        }

        // Finalize and return the report
        return finalizeReport(results, errors, ignored);
    }

    /** Checks if a path points to a supported ZIP file. */
    private boolean isZipFile(Path path) {
        // Must be an existing regular file
        if (path == null || !Files.isRegularFile(path)) {
            return false;
        }
        String filenameLower = path.getFileName().toString().toLowerCase();
        // Check against configured ZIP extensions
        return config.getSupportedZipExtensions().stream().anyMatch(filenameLower::endsWith);
    }

    /** Finalizes the report and logs a summary to System.err. */
    private ProcessingReport finalizeReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Log summary
        System.err.printf("--- Processing Summary ---%n");
        System.err.printf("Successful: %d, Errors: %d, Ignored: %d%n", results.size(), errors.size(), ignored.size());
        // Log error details if any
        if (!errors.isEmpty()) {
            System.err.println("\n--- Error Details ---");
            errors.forEach(e -> System.err.printf("  - [%s]: %s%n", e.source(), e.error()));
        }
        System.err.println("--------------------------");
        // Return the immutable report object
        return new ProcessingReport(results, errors, ignored);
    }

    // --- Main Method (Command-Line Entry Point) ---

    /** CLI entry point. */
    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");

        // Get file path from arguments or use default
        String filePath = getFilePathFromArgsOrDefault(args);
        if (filePath == null) {
            System.err.println("FATAL: No valid file path provided. Exiting.");
            System.exit(1); // Exit with error code
        }
        System.out.println("INFO: Processing source: " + filePath);

        // Initialize components
        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector languageDetector = loadTikaLanguageDetector(); // Attempt to load language detector
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider();
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormat(languageDetector, config);

        // Create the main extractor instance (Facade)
        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, config);

        // Execute the extraction process
        System.out.println("\n--- Starting Processing ---");
        ProcessingReport report = extractor.processSource(filePath);
        System.out.println("--- Processing Finished ---");

        // Print successful results as JSON
        printReportAsJson(report);

        System.out.println("\n--- Metadata Extractor Finished ---");
        // Optionally set exit code based on errors
        if (!report.getErrors().isEmpty()) {
            System.exit(2); // Exit with error code if errors occurred
        }
    }

    /** Gets the file path from command-line args or uses a hardcoded default. */
    private static String getFilePathFromArgsOrDefault(String[] args) {
        // --- !! IMPORTANT !! ---
        // Adjust this default path to an existing test file on your system,
        // or remove it and always provide a path as an argument.
        String defaultPath = ".\\document\\Veg kartering - habitatkaart 2021-2023.zip";
        // String defaultPath = null; // Alternative: no default
        // -------------------------

        String pathToCheck = null;
        // Try path from the first argument
        if (args.length > 0 && args[0] != null && !args[0].isBlank()) {
            pathToCheck = args[0].trim();
            System.out.println("INFO: Using path from command-line argument: " + pathToCheck);
        } else if (defaultPath != null) {
            // Use default path if no argument is given
            pathToCheck = defaultPath;
            System.out.println("INFO: No argument found, using default path: " + pathToCheck);
            System.err.println("WARNING: Using default path. Ensure this is correct or provide a path!");
        } else {
            // No argument and no default path
            System.err.println("ERROR: No file path provided as argument and no default path configured.");
            System.err.println("Usage: java MetadataExtractor <path_to_file_or_zip>");
            return null;
        }

        // Validate the chosen path
        try {
            Path p = Paths.get(pathToCheck);
            if (!Files.exists(p)) {
                System.err.println("ERROR: Provided path does not exist: " + pathToCheck);
                return null;
            }
            // Optional: Check read permissions
            // if (!Files.isReadable(p)) {
            //     System.err.println("ERROR: Cannot read path: " + pathToCheck);
            //     return null;
            // }
            return pathToCheck; // Path seems valid
        } catch (InvalidPathException ipe) {
            System.err.println("ERROR: Invalid path syntax: '" + pathToCheck + "' - " + ipe.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("ERROR: Unexpected error validating path '" + pathToCheck + "': " + e.getMessage());
            return null;
        }
    }

    /** Loads the Tika language detector. Returns null on failure. */
    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            System.out.println("INFO: Loading Tika language models (might take a moment)...");
            // Use the recommended Optimaize detector
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels(); // Load language profiles
            System.out.println("INFO: Tika language models loaded successfully.");
            return detector;
        } catch (NoClassDefFoundError e) {
            System.err.println("ERROR: Could not find Tika language detection classes.");
            System.err.println("Ensure 'tika-langdetect' (and its dependencies) are in the classpath.");
        } catch (IOException e) {
            System.err.println("ERROR: Could not load Tika language models from disk: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("ERROR: Unexpected error initializing Tika language detector: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        // Return null if loading failed
        System.err.println("WARNING: Language detection is disabled due to a loading error.");
        return null;
    }

    /** Prints the successful results from the report as JSON to System.out. */
    private static void printReportAsJson(ProcessingReport report) {
        // Check if there are results to print
        if (report == null || report.getResults().isEmpty()) {
            System.out.println("\nINFO: No successful results found to display as JSON.");
            return;
        }

        System.out.println("\n--- Successfully Processed Resources (JSON Output) ---");
        try {
            // Use Jackson ObjectMapper for JSON serialization
            ObjectMapper mapper = new ObjectMapper();
            // Configure for pretty-printing
            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            // Create a root object for the JSON structure {"resources": [...]}
            Map<String, Object> jsonRoot = new LinkedHashMap<>();

            // Extract the data maps from the CkanResource objects
            List<Map<String, Object>> resourceDataList = report.getResults().stream()
                    .map(CkanResource::getData) // Get the internal map from each CkanResource
                    .collect(Collectors.toList());

            jsonRoot.put("resources", resourceDataList);

            // Convert the root map to JSON and print
            String jsonOutput = mapper.writeValueAsString(jsonRoot);
            System.out.println(jsonOutput);

        } catch (JsonProcessingException e) {
            System.err.println("ERROR: Could not convert results to JSON: " + e.getMessage());
            e.printStackTrace(System.err); // Useful for debugging JSON issues
        } catch (Exception e) {
            System.err.println("ERROR: Unexpected error during JSON generation: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
} // End of MetadataExtractor class
