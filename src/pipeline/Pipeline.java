// Save as: src/pipeline/Pipeline.java
// FIX: Standardized package name
package pipeline;

// FIX: Updated imports for assumed new locations
import config.ConfigLoader;
import processing.ZipProcessor;
import client.CkanHandler;
import client.CkanExceptions.*; // Includes CkanAuthorizationException, CkanNotFoundException etc.
import util.LoggingUtil;

import org.slf4j.Logger;

import java.util.Objects;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Orchestrates the data processing pipeline: finds ZIP files,
 * processes each using ZipProcessor, publishes results via CkanHandler,
 * handles overall workflow, error counting, and logging.
 */
public class Pipeline {

    private static final Logger logger = LoggingUtil.getLogger(Pipeline.class);

    // Pattern for characters NOT allowed in CKAN IDs (slugs)
    private static final Pattern INVALID_ID_CHARS = Pattern.compile("[^a-z0-9_\\-]+");
    // Pattern to replace multiple/leading/trailing hyphens/underscores
    private static final Pattern NORMALIZE_ID_SEPARATORS = Pattern.compile("[-_]{2,}|^-|-$|^_|-_$"); // Combined pattern
    private static final int MAX_ID_LENGTH = 100; // CKAN default max package/resource name length

    private final ConfigLoader config;
    private final CkanHandler ckanHandler;
    private int totalFilesFound = 0;
    private int totalProcessedZips = 0;
    private int totalErrorZips = 0;

    /**
     * Initializes the pipeline with necessary configuration and handlers.
     * @param config The loaded application configuration.
     */
    public Pipeline(ConfigLoader config) {
        this.config = Objects.requireNonNull(config, "ConfigLoader cannot be null");
        // Initialize CkanHandler - constructor handles its own validation
        this.ckanHandler = new CkanHandler(config.getCkanUrl(), config.getCkanApiKey());
        logger.info("Pipeline initialized.");
        // Optional: Test CKAN connection early
        // try { ckanHandler.testConnection(); } catch (Exception e) { logger.error("Initial CKAN connection test failed!", e); /* Decide if fatal */ }
    }

    /**
     * Runs the main pipeline process: scan source, process ZIPs, log summary.
     */
    public void run() {
        Instant runStartTime = Instant.now();
        logger.info("==================================================");
        logger.info("--- Starting Data Pipeline Run ---");
        logger.info("Timestamp: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
        logConfigurationSummary(); // Log resolved config values
        logger.info("==================================================");

        // Reset counters for the run
        totalFilesFound = 0;
        totalProcessedZips = 0;
        totalErrorZips = 0;
        List<Path> inputFiles = Collections.emptyList();

        try {
            validateSourceDirectory(); // Check source dir upfront
            inputFiles = detectInputFiles();
            totalFilesFound = inputFiles.size();

            if (inputFiles.isEmpty()) {
                logger.info("No input ZIP files found in source directory '{}'.", config.getSourceDir());
            } else {
                logger.info("Processing {} ZIP file(s) found in '{}'...", inputFiles.size(), config.getSourceDir());
                // Process files sequentially
                for (int i = 0; i < inputFiles.size(); i++) {
                    processSingleZip(inputFiles.get(i), i + 1, inputFiles.size());
                    logger.info("--- Progress: {}/{} files dispatched ---", i + 1, inputFiles.size());
                    // Optional: Add delay between files if needed
                    // try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); logger.warn("Delay interrupted."); }
                }
            }
        } catch (IOException | SecurityException e) {
            // Errors during directory validation or initial listing
            logger.error("CRITICAL FAILURE accessing source directory '{}': {}. Aborting run.", config.getSourceDir(), e.getMessage(), e);
            totalErrorZips = Math.max(0, totalFilesFound - totalProcessedZips); // Estimate remaining as errors
        } catch (Exception e) {
            // Catch any other unexpected errors during the main loop setup
            logger.error("CRITICAL UNEXPECTED failure during pipeline execution: {}. Aborting run.", e.getMessage(), e);
            totalErrorZips = Math.max(0, totalFilesFound - totalProcessedZips); // Estimate errors
        } finally {
            Instant runEndTime = Instant.now();
            Duration runDuration = Duration.between(runStartTime, runEndTime);
            logRunSummary(runDuration);
        }
    }

    /** Logs a summary of the configuration being used (using resolved absolute paths). */
    private void logConfigurationSummary() {
        logger.info("--- Configuration Summary ---");
        logger.info("Config File : '{}'", config.getConfigPath()); // Absolute
        logger.info("Source Dir  : '{}'", config.getSourceDir()); // Absolute
        logger.info("Staging Dir : '{}'", config.getStagingDir()); // Absolute
        Path processedDir = config.getProcessedDir(); // Absolute or null
        logger.info("Processed Dir: {} {}", processedDir != null ? processedDir : "N/A", config.isMoveProcessed() ? "(Move Enabled)" : "(Move Disabled)");
        logger.info("Log File    : '{}'", config.getLogFile()); // Absolute
        logger.info("CKAN URL    : '{}'", config.getCkanUrl());
        // FIX: Assumes getCkanOrgPrefix() added to ConfigLoader
        // logger.info("CKAN Org Pfx: '{}'", config.getCkanOrgPrefix());
        logger.info("Create Orgs : {} {}", config.isCreateOrganizations(), (config.isCreateOrganizations() ? "(Sysadmin Key Required!)" : ""));
        List<String> extensions = config.getRelevantExtensions();
        logger.info("Relevant Ext: {}", extensions.isEmpty() ? "All files" : extensions);
        logger.info("Extract Nested: {}", config.isExtractNestedZips());
        logger.info("-----------------------------");
    }

    /** Validates the source directory exists, is a directory, and is readable. */
    private void validateSourceDirectory() throws IOException {
        Path sourceDir = config.getSourceDir(); // Assumes absolute from ConfigLoader
        if (!Files.exists(sourceDir)) { throw new FileNotFoundException("Source directory not found: " + sourceDir); }
        if (!Files.isDirectory(sourceDir)) { throw new NotDirectoryException("Source path is not a directory: " + sourceDir); }
        if (!Files.isReadable(sourceDir)) { throw new AccessDeniedException("Cannot read source directory: " + sourceDir); }
        logger.debug("Source directory '{}' validated successfully.", sourceDir);
    }

    /** Scans the source directory for readable ".zip" files. */
    private List<Path> detectInputFiles() throws IOException { // Let IOExceptions propagate
        logger.info("Scanning for input ZIPs in: {}", config.getSourceDir());
        List<Path> zipFiles;

        // Use try-with-resources for DirectoryStream to ensure it's closed
        try (Stream<Path> stream = Files.list(config.getSourceDir())) {
            zipFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> getExtension(p.getFileName().toString()).equals("zip")) // Use helper
                    .filter(p -> {
                        if (!Files.isReadable(p)) {
                            logger.warn("Skipping unreadable file: {}", p.getFileName());
                            return false;
                        }
                        return true;
                    })
                    .sorted() // Process files consistently
                    .collect(Collectors.toList());
            logger.info("{} readable input ZIP file(s) found.", zipFiles.size());
        } catch (IOException | SecurityException e) { // Catch potential exceptions during listing
            logger.error("Error reading source directory contents '{}': {}", config.getSourceDir(), e.getMessage(), e);
            throw e; // Re-throw to be caught by run() method
        }
        return zipFiles;
    }

    /** Processes a single ZIP file through staging, extraction, CKAN publishing, and cleanup. */
    private void processSingleZip(Path zipPath, int index, int totalFiles) {
        // Create a new ZipProcessor for each file
        ZipProcessor processor = new ZipProcessor(zipPath, config);
        String logPrefix = processor.getLogPrefix(); // Get prefix "[zipfilename] "
        Instant zipStartTime = Instant.now();
        logger.info("--- [{}/{}] Start processing ZIP: '{}' ---", index, totalFiles, processor.getOriginalName());
        boolean zipProcessedSuccessfully = false;

        try {
            // 1. Stage
            processor.stage(); // throws IOException

            // 2. Extract
            List<Path> filesToUpload = processor.extract(); // throws IOException

            // 3. Publish
            if (!filesToUpload.isEmpty()) {
                publishToCkan(processor.getOriginalName(), processor.getExtractSubdir(), filesToUpload, logPrefix); // throws CkanException, IOException
            } else {
                logger.info("{}No relevant files found to publish based on configuration.", logPrefix);
            }

            // Mark success ONLY if all previous steps completed without exception
            zipProcessedSuccessfully = true;
            totalProcessedZips++;
            logger.info("{}Successfully processed.", logPrefix);

        } catch (CkanAuthorizationException e) {
            totalErrorZips++; logger.error("{}CKAN Authorization Error: {}. Check API key permissions.", logPrefix, e.getMessage());
        } catch (CkanNotFoundException e) {
            totalErrorZips++; logger.error("{}CKAN Not Found Error: {}. Target organization/dataset/resource may not exist or ID is incorrect.", logPrefix, e.getMessage());
        } catch (CkanValidationException e) {
            totalErrorZips++; logger.error("{}CKAN Validation Error: {}. Details: {}", logPrefix, e.getMessage(), e.getErrorDict());
        } catch (CkanException e) { // Catch other specific CKAN API errors
            totalErrorZips++; logger.error("{}CKAN API Error: {}.", logPrefix, e.getMessage()); logger.debug("{}CKAN API Error Stack Trace:", logPrefix, e);
        } catch (IOException e) { // Catch file/IO errors (staging, extraction, moving, listing)
            totalErrorZips++; logger.error("{}File I/O Error: {}.", logPrefix, e.getMessage()); logger.debug("{}I/O Error Stack Trace:", logPrefix, e);
        } catch (RuntimeException e) { // Catch unexpected runtime errors (e.g., from ZipProcessor constructor)
            totalErrorZips++; logger.error("{}Unexpected Runtime Error: {}.", logPrefix, e.getMessage()); logger.error("{}Runtime Error Stack Trace:", logPrefix, e); // Log stack trace for unexpected runtime issues
        } finally {
            // 4. Cleanup Staging (always attempt)
            try { processor.cleanupStaging(); }
            catch (Exception cleanupEx) { logger.error("{}Error during staging cleanup: {}", logPrefix, cleanupEx.getMessage(), cleanupEx); }

            // 5. Move Original (only if successful & enabled)
            if (config.isMoveProcessed()) {
                if (zipProcessedSuccessfully) {
                    try { processor.moveToProcessed(); } // throws IOException
                    catch (IOException moveEx) { logger.error("{}Failed to move successfully processed file '{}' to processed directory: {}", logPrefix, processor.getOriginalName(), moveEx.getMessage(), moveEx); }
                } else { logger.warn("{}Skipping move to processed for '{}' due to processing errors.", logPrefix, processor.getOriginalName()); }
            }

            Instant zipEndTime = Instant.now();
            Duration zipDuration = Duration.between(zipStartTime, zipEndTime);
            String status = zipProcessedSuccessfully ? "COMPLETED" : "FAILED";
            logger.info("--- [{}/{}] Finished ZIP: '{}'. Status: {}. Duration: {:.3f} sec. ---",
                    index, totalFiles, processor.getOriginalName(), status, zipDuration.toMillis() / 1000.0);
        }
    }

    /** Generates a CKAN-compliant ID (slug) from a filename. */
    private String createCkanId(String name) {
        // FIX: Use helper method instead of Guava
        String baseName = getBaseName(name);

        // Convert to lowercase, replace space with hyphen, remove invalid chars
        String cleaned = baseName.toLowerCase()
                .replace(' ', '-')
                .replaceAll(INVALID_ID_CHARS.pattern(), ""); // Use pre-compiled pattern

        // Normalize separators (repeatedly apply)
        String previous;
        do {
            previous = cleaned;
            // Replace multiple hyphens/underscores with single ones, remove leading/trailing
            cleaned = NORMALIZE_ID_SEPARATORS.matcher(cleaned).replaceAll(match -> {
                // Keep hyphen if hyphen was present, keep underscore if underscore was present
                // Handle leading/trailing matches or mixed sequences like "-_" by replacing with empty string implicitly
                if (match.group().contains("-")) return "-";
                if (match.group().contains("_")) return "_";
                return ""; // Should cover leading/trailing cases matched by ^-$ or ^_$ etc.
            });
            // Remove any leading/trailing separator potentially missed if only one existed initially
            if (cleaned.startsWith("-") || cleaned.startsWith("_")) cleaned = cleaned.substring(1);
            if (cleaned.endsWith("-") || cleaned.endsWith("_")) cleaned = cleaned.substring(0, cleaned.length() - 1);
        } while (!previous.equals(cleaned));


        // Enforce max length AFTER cleaning separators
        if (cleaned.length() > MAX_ID_LENGTH) {
            cleaned = cleaned.substring(0, MAX_ID_LENGTH);
            // Re-trim after substring, just in case (though less likely needed now)
            if (cleaned.endsWith("-") || cleaned.endsWith("_")) {
                cleaned = cleaned.substring(0, cleaned.length() - 1);
            }
        }

        // Ensure it's not empty or just a separator after cleaning
        if (cleaned.isEmpty() || cleaned.equals("-") || cleaned.equals("_")) {
            logger.warn("Could not derive valid CKAN ID from '{}'. Generating fallback ID.", name);
            return "item-" + Instant.now().toEpochMilli(); // Robust fallback
        }

        logger.trace("Converted filename '{}' to CKAN ID: '{}'", name, cleaned);
        return cleaned;
    }

    /** Generates a human-readable title from a filename. */
    private String createCkanTitle(String name) {
        // FIX: Use helper method instead of Guava
        String baseName = getBaseName(name);
        // Replace separators with space
        String title = baseName.replace('_', ' ').replace('-', ' ');
        // Simple title casing (capitalize first letter of each word)
        // Consider using Apache Commons Text WordUtils.capitalizeFully for more robust title casing if needed.
        return Pattern.compile("\\b(.)(.*?)\\b")
                .matcher(title.toLowerCase()) // Start from lowercase
                .replaceAll(match -> match.group(1).toUpperCase() + match.group(2));
    }


    /** Publishes extracted files to CKAN, managing organization and dataset creation/update. */
    private void publishToCkan(String originalZipName, Path extractSubdir, List<Path> filesToUpload, String logPrefix)
            throws CkanException, IOException { // Let exceptions propagate
        logger.info("{}Starting CKAN publication for {} file(s)...", logPrefix, filesToUpload.size());

        // --- Determine Organization and Dataset IDs/Titles ---
        String baseId = createCkanId(originalZipName);
        String baseTitle = createCkanTitle(originalZipName);

        // FIX: Use org prefix from ConfigLoader (assumes getter exists)
        // String orgPrefix = config.getCkanOrgPrefix(); // Example
        String orgPrefix = "org-"; // Fallback if getter not implemented yet
        // TODO: Add getCkanOrgPrefix() to ConfigLoader, loading "CKAN.org_prefix", defaulting to "org-"
        // Example addition to ConfigLoader:
        // private static final String KEY_CKAN_ORG_PREFIX = "CKAN.org_prefix";
        // private String ckanOrgPrefix;
        // // In loadAndValidateSettings:
        // ckanOrgPrefix = properties.getProperty(KEY_CKAN_ORG_PREFIX, "org-").trim();
        // // Add getter:
        // public String getCkanOrgPrefix() { return ckanOrgPrefix; }

        String targetOrgId = orgPrefix + baseId;
        String targetOrgTitle = baseTitle;
        String datasetId = baseId; // Dataset ID usually doesn't have prefix
        String datasetTitle = baseTitle;

        // Re-validate/truncate derived IDs after prefixing
        if (targetOrgId.length() > MAX_ID_LENGTH) {
            logger.warn("{}Derived Org ID ('{}') exceeds max length ({}), truncating.", logPrefix, targetOrgId, MAX_ID_LENGTH);
            targetOrgId = targetOrgId.substring(0, MAX_ID_LENGTH);
            // Ensure no trailing separator after truncation
            if (targetOrgId.endsWith("-") || targetOrgId.endsWith("_")) targetOrgId = targetOrgId.substring(0, MAX_ID_LENGTH - 1);
        }
        if (datasetId.length() > MAX_ID_LENGTH) {
            logger.warn("{}Derived Dataset ID ('{}') exceeds max length ({}), truncating.", logPrefix, datasetId, MAX_ID_LENGTH);
            datasetId = datasetId.substring(0, MAX_ID_LENGTH);
            if (datasetId.endsWith("-") || datasetId.endsWith("_")) datasetId = datasetId.substring(0, MAX_ID_LENGTH - 1);
        }
        // Ensure IDs are not empty after potential truncation/prefixing issues
        if (targetOrgId.isEmpty() || targetOrgId.equals(orgPrefix) || targetOrgId.equals("-") || targetOrgId.equals("_")){
            throw new CkanException(logPrefix + "Failed to derive a valid non-empty Organization ID from " + originalZipName);
        }
        if (datasetId.isEmpty() || datasetId.equals("-") || datasetId.equals("_")){
            throw new CkanException(logPrefix + "Failed to derive a valid non-empty Dataset ID from " + originalZipName);
        }


        logger.info("{}Target Organization ID: '{}', Title: '{}'", logPrefix, targetOrgId, targetOrgTitle);
        logger.info("{}Target Dataset ID     : '{}', Title: '{}'", logPrefix, datasetId, datasetTitle);

        String finalOwnerOrgId;

        // --- Step 1: Ensure Organization Exists ---
        Optional<Map<String, Object>> existingOrgOpt = ckanHandler.checkOrganizationExists(targetOrgId);
        if (existingOrgOpt.isPresent()) {
            finalOwnerOrgId = (String) existingOrgOpt.get().get("id");
            logger.info("{}Using existing organization: ID '{}'", logPrefix, finalOwnerOrgId);
        } else {
            logger.info("{}Organization '{}' not found.", logPrefix, targetOrgId);
            if (config.isCreateOrganizations()) {
                logger.warn("{}Attempting auto-creation of organization '{}' (requires sysadmin API key!).", logPrefix, targetOrgId);
                // createOrganization throws exceptions on failure
                Map<String, Object> createdOrg = ckanHandler.createOrganization(targetOrgId, targetOrgTitle);
                finalOwnerOrgId = (String) createdOrg.get("id");
                logger.info("{}Successfully created organization: ID '{}'", logPrefix, finalOwnerOrgId);
            } else {
                String errorMsg = String.format("%sTarget organization '%s' not found and auto-creation disabled. Cannot proceed.", logPrefix, targetOrgId);
                logger.error(errorMsg);
                throw new CkanNotFoundException(errorMsg); // Use specific exception
            }
        }
        logger.info("{}Using Organization ID: '{}' for dataset.", logPrefix, finalOwnerOrgId);

        // --- Step 2: Get or Create Dataset ---
        // getOrCreateDataset throws exceptions on failure
        Map<String, Object> dataset = ckanHandler.getOrCreateDataset(datasetId, datasetTitle, finalOwnerOrgId, originalZipName);
        String packageId = (String) dataset.get("id");
        logger.info("{}Using Dataset ID: '{}' for resources.", logPrefix, packageId);

        // --- Step 3: Get Existing Resources ---
        Map<String, String> existingResources = ckanHandler.getExistingResourceNamesAndIds(packageId);

        // --- Step 4: Upload/Update Resources ---
        int successfulUploads = 0;
        int failedUploads = 0;
        List<String> errorMessages = new ArrayList<>();

        for (Path filePath : filesToUpload) {
            String originalResourceName = filePath.getFileName().toString();
            String resourceName = originalResourceName; // Start with original

            // Sanitize resource name if necessary (CKAN allows more chars than IDs, but length limit applies)
            if (resourceName.length() > MAX_ID_LENGTH) {
                String ext = getExtension(resourceName);
                String base = getBaseName(resourceName);
                // Truncate base name to fit limit, preserving extension
                int maxBaseLength = MAX_ID_LENGTH - (ext.isEmpty() ? 0 : ext.length() + 1);
                if (maxBaseLength < 1) { // Handle cases where extension itself is too long or near limit
                    logger.error("{}Cannot create valid resource name for '{}': exceeds max length ({}) even after truncation attempt.", logPrefix, originalResourceName, MAX_ID_LENGTH);
                    failedUploads++;
                    errorMessages.add("Resource name too long: " + originalResourceName);
                    continue; // Skip this file
                }
                resourceName = base.substring(0, maxBaseLength) + (ext.isEmpty() ? "" : "." + ext);
                logger.warn("{}Resource filename '{}' exceeds max length ({}), truncated to '{}'. Note: Truncation might cause name collisions.", logPrefix, originalResourceName, MAX_ID_LENGTH, resourceName);
            }
            String existingResourceId = existingResources.get(resourceName); // Use potentially truncated name to check existence

            try {
                Path relativePath = extractSubdir.relativize(filePath);
                String description = String.format("File '%s' from archive '%s', processed on %s.",
                        relativePath, originalZipName, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")));
                String format = getExtension(originalResourceName); // Get format from original name
                format = (format.isEmpty()) ? "data" : format.toUpperCase(); // Default format, use UPPERCASE convention

                ckanHandler.uploadOrUpdateResource(packageId, filePath, resourceName, description, format, existingResourceId);
                successfulUploads++;
            } catch (Exception e) { // Catch broadly per resource
                failedUploads++;
                String errorMsg = String.format("%sFailed to process resource '%s' (from '%s'): %s", logPrefix, resourceName, originalResourceName, e.getMessage());
                logger.error(errorMsg, e); // Log full stack trace
                errorMessages.add(String.format("Resource '%s': %s", resourceName, e.getMessage()));
            }
        }

        logger.info("{}CKAN resource processing finished. Successful: {}, Failed: {}", logPrefix, successfulUploads, failedUploads);

        // Fail the whole ZIP if any resource failed
        if (failedUploads > 0) {
            throw new CkanException(String.format("%s%d resource(s) failed to upload/update. First error: %s", logPrefix, failedUploads, (errorMessages.isEmpty() ? "Unknown" : errorMessages.get(0))));
        }
    }

    /** Logs final summary statistics of the pipeline run. */
    private void logRunSummary(Duration runDuration) {
        logger.info("==================================================");
        logger.info("--- Data Pipeline Run Summary ---");
        logger.info("Total ZIPs found: {}", totalFilesFound);
        logger.info("Successful ZIPs : {}", totalProcessedZips);
        logger.info("Failed ZIPs     : {}", totalErrorZips);
        logger.info("Total Duration  : {:.3f} seconds.", runDuration.toMillis() / 1000.0);
        logger.info("==================================================");
    }

    // --- Helper Methods for Filename Splitting (Replaces Guava) ---
    // Copied from ZipProcessor - consider moving to a shared Util class

    private static String getBaseName(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex <= 0) return filename; else return filename.substring(0, dotIndex);
    }

    private static String getExtension(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex <= 0 || dotIndex == filename.length() - 1) return ""; else return filename.substring(dotIndex + 1).toLowerCase();
    }

    // --- TODO: Additions needed in ConfigLoader.java ---
    /*
    // In ConfigLoader constants:
    private static final String KEY_CKAN_ORG_PREFIX = "CKAN.org_prefix";

    // In ConfigLoader fields:
    private String ckanOrgPrefix;

    // In ConfigLoader.loadAndValidateSettings() method:
    ckanOrgPrefix = properties.getProperty(KEY_CKAN_ORG_PREFIX, "org-").trim();
    // Optional: Add validation for prefix format if needed

    // In ConfigLoader, add getter:
    public String getCkanOrgPrefix() {
        return ckanOrgPrefix;
    }
    */
}