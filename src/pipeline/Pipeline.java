// Save as: src/pipeline/Pipeline.java
package pipeline;

import config.ConfigLoader;
import processing.ZipProcessor;
import client.CkanHandler;
import client.CkanExceptions.*; // Includes CkanAuthorizationException, CkanNotFoundException etc.
import util.LoggingUtil;

import org.slf4j.Logger;

import java.util.Objects;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*; // Includes Files, Path, Paths, AccessDeniedException, NotDirectoryException
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale; // <-- FIX: Added import
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher; // <-- FIX: Added import
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Orchestrates the data processing pipeline: finds ZIP files,
 * processes each using ZipProcessor, publishes results via CkanHandler,
 * handles overall workflow, error counting, and logging.
 * This class is final and not intended for subclassing.
 */
public final class Pipeline { // Made class final

    private static final Logger logger = LoggingUtil.getLogger(Pipeline.class);

    // --- Constants for CKAN ID generation ---
    // Allow lowercase letters, numbers, underscore, hyphen
    private static final Pattern INVALID_ID_CHARS = Pattern.compile("[^a-z0-9_\\-]+");
    // Pattern to find consecutive separators (hyphen or underscore)
    private static final Pattern MULTIPLE_SEPARATORS = Pattern.compile("[-_]{2,}");
    // Pattern for leading/trailing hyphens (after replacing underscores)
    private static final Pattern LEADING_TRAILING_HYPHEN = Pattern.compile("^-|-$");
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
        // Optional: Test CKAN connection early - uncomment if desired
        // try {
        //     logger.info("Performing initial CKAN connection test...");
        //     ckanHandler.testConnection();
        //     logger.info("Initial CKAN connection test successful.");
        // } catch (Exception e) {
        //     logger.error("INITIAL CKAN CONNECTION TEST FAILED! Check URL and API Key/Permissions.", e);
        //     // Depending on requirements, might want to throw RuntimeException here to halt startup
        // }
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
                    // Optional: Add delay between files if needed for rate limiting etc.
                    // try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); logger.warn("Delay interrupted."); }
                }
            }
        } catch (IOException | SecurityException e) {
            // Errors during directory validation or initial listing
            logger.error("CRITICAL FAILURE accessing source directory '{}': {}. Aborting run.", config.getSourceDir(), e.getMessage(), e);
            totalErrorZips = Math.max(0, totalFilesFound - totalProcessedZips); // Estimate remaining as errors
        } catch (Exception e) {
            // Catch any other unexpected errors during the main loop setup or processing initiation
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
        logger.info("Config File : '{}'", config.getConfigPath());
        logger.info("Source Dir  : '{}'", config.getSourceDir());
        logger.info("Staging Dir : '{}'", config.getStagingDir());
        Path processedDir = config.getProcessedDir();
        logger.info("Processed Dir: {} {}", processedDir != null ? processedDir : "N/A", config.isMoveProcessed() ? "(Move Enabled)" : "(Move Disabled)");
        logger.info("Log File    : '{}'", config.getLogFile());
        logger.info("CKAN URL    : '{}'", config.getCkanUrl());
        // FIX: Reverted to placeholder - Add getCkanOrgPrefix() to ConfigLoader as per TODO below
        String orgPrefixPlaceholder = "org-"; // Default if not configured
        logger.info("CKAN Org Pfx: '{}' (NOTE: Hardcoded default used, see TODO)", orgPrefixPlaceholder);
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
                    // Use helper method for extension check
                    .filter(p -> "zip".equals(getExtension(p.getFileName().toString())))
                    .filter(p -> { // Check readability after identifying as potential zip
                        if (!Files.isReadable(p)) {
                            logger.warn("Skipping unreadable file: {}", p.getFileName());
                            return false;
                        }
                        return true;
                    })
                    .sorted() // Process files consistently (alphabetical)
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
        // Create a new ZipProcessor for each file - ensures clean state
        ZipProcessor processor = null;
        try {
            processor = new ZipProcessor(zipPath, config);
        } catch (RuntimeException e) {
            // Catch potential RuntimeExceptions from ZipProcessor constructor (e.g., staging dir creation failed)
            totalErrorZips++;
            logger.error("--- [{}/{}] FAILED to initialize processor for ZIP: '{}': {} ---", index, totalFiles, zipPath.getFileName(), e.getMessage(), e);
            // Cannot proceed with this file, no cleanup needed as processor wasn't fully created
            return;
        }

        String logPrefix = processor.getLogPrefix(); // Get prefix "[zipfilename] "
        Instant zipStartTime = Instant.now();
        logger.info("--- [{}/{}] Start processing ZIP: '{}' ---", index, totalFiles, processor.getOriginalName());
        boolean zipProcessedSuccessfully = false;

        try {
            // 1. Stage the file
            processor.stage(); // throws IOException

            // 2. Extract relevant files
            List<Path> filesToUpload = processor.extract(); // throws IOException

            // 3. Publish extracted files to CKAN
            if (!filesToUpload.isEmpty()) {
                publishToCkan(processor.getOriginalName(), processor.getExtractSubdir(), filesToUpload, logPrefix); // throws CkanException, IOException
            } else {
                logger.info("{}No relevant files found to publish based on configuration.", logPrefix);
            }

            // Mark success ONLY if all previous steps completed without throwing an exception
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
        } catch (RuntimeException e) { // Catch unexpected runtime errors during processing steps
            totalErrorZips++; logger.error("{}Unexpected Runtime Error during processing: {}.", logPrefix, e.getMessage()); logger.error("{}Runtime Error Stack Trace:", logPrefix, e);
        } finally {
            // Ensure processor was initialized before attempting cleanup/move
            if (processor != null) {
                // 4. Cleanup Staging (always attempt)
                try {
                    processor.cleanupStaging();
                } catch (Exception cleanupEx) { // Catch broad exception during cleanup
                    logger.error("{}Error during staging cleanup: {}", logPrefix, cleanupEx.getMessage(), cleanupEx);
                }

                // 5. Move Original (only if successful & enabled)
                if (config.isMoveProcessed()) {
                    if (zipProcessedSuccessfully) {
                        try {
                            processor.moveToProcessed(); // throws IOException
                        } catch (IOException moveEx) {
                            logger.error("{}Failed to move successfully processed file '{}' to processed directory: {}", logPrefix, processor.getOriginalName(), moveEx.getMessage(), moveEx);
                        }
                    } else {
                        logger.warn("{}Skipping move to processed for '{}' due to processing errors.", logPrefix, processor.getOriginalName());
                    }
                }
            } // end if processor != null

            Instant zipEndTime = Instant.now();
            Duration zipDuration = Duration.between(zipStartTime, zipEndTime);
            String status = zipProcessedSuccessfully ? "COMPLETED" : "FAILED";
            logger.info("--- [{}/{}] Finished ZIP: '{}'. Status: {}. Duration: {:.3f} sec. ---",
                    index, totalFiles, (processor != null ? processor.getOriginalName() : zipPath.getFileName()), // Handle case where processor init failed
                    status, zipDuration.toMillis() / 1000.0);
        }
    }

    /**
     * Generates a CKAN-compliant ID (slug) from a name (e.g., filename).
     * Simplified logic: lowercase, replaces spaces with hyphens, removes invalid chars,
     * collapses separators to single hyphen, trims hyphens, truncates.
     */
    private String createCkanId(String name) {
        // Use helper method to get base name (part before last dot)
        String baseName = getBaseName(name);
        if (baseName.isEmpty()) baseName = name; // Handle names with no extension

        // 1. Lowercase using English locale to avoid locale-specific case conversion issues
        String cleaned = baseName.toLowerCase(Locale.ENGLISH);
        // 2. Replace spaces with hyphens
        cleaned = cleaned.replace(' ', '-');
        // 3. Remove all invalid characters (allow only a-z, 0-9, _, -)
        cleaned = INVALID_ID_CHARS.matcher(cleaned).replaceAll("");
        // 4. Collapse consecutive separators (hyphen or underscore) to a single hyphen
        cleaned = MULTIPLE_SEPARATORS.matcher(cleaned).replaceAll("-");
        // 5. Remove leading or trailing hyphens
        cleaned = LEADING_TRAILING_HYPHEN.matcher(cleaned).replaceAll("");

        // 6. Enforce max length
        if (cleaned.length() > MAX_ID_LENGTH) {
            cleaned = cleaned.substring(0, MAX_ID_LENGTH);
            // 7. Re-check and remove trailing hyphen possibly introduced by truncation
            cleaned = LEADING_TRAILING_HYPHEN.matcher(cleaned).replaceAll(""); // Use pattern again for simplicity
        }

        // 8. Ensure it's not empty after cleaning
        if (cleaned.isEmpty()) {
            logger.warn("Could not derive valid CKAN ID from '{}'. Generating fallback ID.", name);
            // Fallback using timestamp (more robust than just random)
            return "item-" + Instant.now().toEpochMilli();
        }

        logger.trace("Converted name '{}' to CKAN ID: '{}'", name, cleaned);
        return cleaned;
    }

    /** Generates a human-readable title from a name (e.g., filename). */
    private String createCkanTitle(String name) {
        // Use helper method to get base name
        String baseName = getBaseName(name);
        if (baseName.isEmpty()) baseName = name; // Handle names with no extension

        // Replace common separators with space
        String title = baseName.replace('_', ' ').replace('-', ' ');
        title = title.replaceAll("\\s+", " ").trim(); // Add this line

        // Basic Title Casing: Capitalize the first letter of each word.
        // Uses Java 8 compatible loop instead of Java 9+ replaceAll lambda.
        // For more advanced or locale-aware title casing, consider Apache Commons Text: WordUtils.capitalizeFully(title).
        Matcher matcher = Pattern.compile("\\b(.)(.*?)\\b").matcher(title.toLowerCase(Locale.ENGLISH));
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group(1).toUpperCase(Locale.ENGLISH) + matcher.group(2));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }


    /** Publishes extracted files to CKAN, managing organization and dataset creation/update. */
    private void publishToCkan(String originalZipName, Path extractSubdir, List<Path> filesToUpload, String logPrefix)
            throws CkanException, IOException { // Let exceptions propagate up to processSingleZip
        logger.info("{}Starting CKAN publication for {} file(s)...", logPrefix, filesToUpload.size());

        // --- Determine Organization and Dataset IDs/Titles ---
        String baseId = createCkanId(originalZipName); // Use simplified ID creation
        String baseTitle = createCkanTitle(originalZipName);

        // FIX: Use placeholder prefix until ConfigLoader is updated. See TODO below.
        String orgPrefix = "org-"; // Default prefix
        // String orgPrefix = config.getCkanOrgPrefix(); // Use this line once ConfigLoader is updated

        String targetOrgId = orgPrefix + baseId;
        String targetOrgTitle = baseTitle; // Reusing base title for org, adjust if needed
        String datasetId = baseId; // Dataset ID usually doesn't have prefix
        String datasetTitle = baseTitle;

        // --- TODO: Add getCkanOrgPrefix() to ConfigLoader ---
        /*
        Instructions remain the same: Add KEY_CKAN_ORG_PREFIX constant, orgPrefix field,
        load it in loadAndValidateSettings (e.g., properties.getProperty(KEY_CKAN_ORG_PREFIX, "org-").trim()),
        and add public String getCkanOrgPrefix() getter to ConfigLoader.java.
        */

        // Re-validate/truncate derived IDs AFTER prefixing
        targetOrgId = validateAndTruncateId(targetOrgId, "Org ID", logPrefix);
        datasetId = validateAndTruncateId(datasetId, "Dataset ID", logPrefix);

        // Ensure IDs are still valid after potential truncation/prefixing issues
        if (targetOrgId.isEmpty() || targetOrgId.equals(orgPrefix) || targetOrgId.equals("-")) {
            // Handle case where only prefix remains or ID is empty/invalid
            throw new CkanException(logPrefix + "Failed to derive a valid non-empty Organization ID from " + originalZipName + " with prefix '" + orgPrefix + "'");
        }
        if (datasetId.isEmpty() || datasetId.equals("-")) {
            throw new CkanException(logPrefix + "Failed to derive a valid non-empty Dataset ID from " + originalZipName);
        }

        logger.info("{}Target Organization ID: '{}', Title: '{}'", logPrefix, targetOrgId, targetOrgTitle);
        logger.info("{}Target Dataset ID     : '{}', Title: '{}'", logPrefix, datasetId, datasetTitle);

        String finalOwnerOrgId;

        // --- Step 1: Ensure Organization Exists ---
        Optional<Map<String, Object>> existingOrgOpt = ckanHandler.checkOrganizationExists(targetOrgId);
        if (existingOrgOpt.isPresent()) {
            finalOwnerOrgId = (String) existingOrgOpt.get().get("id"); // Use the actual ID returned by CKAN
            logger.info("{}Using existing organization: ID '{}'", logPrefix, finalOwnerOrgId);
        } else {
            logger.info("{}Organization '{}' not found.", logPrefix, targetOrgId);
            if (config.isCreateOrganizations()) {
                logger.warn("{}Attempting auto-creation of organization '{}' (requires sysadmin API key!).", logPrefix, targetOrgId);
                Map<String, Object> createdOrg = ckanHandler.createOrganization(targetOrgId, targetOrgTitle); // Throws exceptions on failure
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
        Map<String, Object> dataset = ckanHandler.getOrCreateDataset(datasetId, datasetTitle, finalOwnerOrgId, originalZipName); // Throws exceptions on failure
        String packageId = (String) dataset.get("id"); // Use actual dataset ID from CKAN response
        logger.info("{}Using Dataset ID: '{}' for resources.", logPrefix, packageId);

        // --- Step 3: Get Existing Resources (using actual packageId) ---
        Map<String, String> existingResources = ckanHandler.getExistingResourceNamesAndIds(packageId);

        // --- Step 4: Upload/Update Resources ---
        int successfulUploads = 0;
        int failedUploads = 0;
        List<String> errorMessages = new ArrayList<>();

        for (Path filePath : filesToUpload) {
            String originalResourceName = filePath.getFileName().toString();
            String resourceName = originalResourceName; // Start with original filename

            // Sanitize/Truncate resource name if necessary for CKAN (length limit usually 100)
            resourceName = validateAndTruncateResourceName(resourceName, logPrefix);
            if (resourceName.isEmpty()) {
                logger.error("{}Cannot derive valid resource name for file '{}'. Skipping.", logPrefix, originalResourceName);
                failedUploads++;
                errorMessages.add("Invalid resource name derived from: " + originalResourceName);
                continue; // Skip this file
            }

            // Check if resource exists using the potentially modified name
            String existingResourceId = existingResources.get(resourceName);

            try {
                // Create a description for the resource
                Path relativePath = extractSubdir.relativize(filePath);
                String description = String.format("File '%s' from archive '%s', processed on %s.",
                        relativePath, originalZipName, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")));

                // Determine format from original filename extension
                String format = getExtension(originalResourceName); // Get format from original name
                format = (format.isEmpty()) ? "data" : format.toUpperCase(); // Default format, use UPPERCASE convention

                // Call CKAN handler to upload or update
                ckanHandler.uploadOrUpdateResource(packageId, filePath, resourceName, description, format, existingResourceId);
                successfulUploads++;
            } catch (Exception e) { // Catch broadly per resource, let CkanHandler throw specific CkanExceptions if needed
                failedUploads++;
                String errorMsg = String.format("%sFailed to process resource '%s' (from '%s'): %s", logPrefix, resourceName, originalResourceName, e.getMessage());
                logger.error(errorMsg, e); // Log full stack trace for resource errors
                errorMessages.add(String.format("Resource '%s': %s", resourceName, e.getMessage()));
            }
        }

        logger.info("{}CKAN resource processing finished. Successful: {}, Failed: {}", logPrefix, successfulUploads, failedUploads);

        // Fail the whole ZIP's publish step if any resource failed
        if (failedUploads > 0) {
            throw new CkanException(String.format("%s%d resource(s) failed to upload/update. First error: %s",
                    logPrefix, failedUploads, (errorMessages.isEmpty() ? "Unknown reason" : errorMessages.get(0))));
        }
    }

    // Helper to validate and truncate IDs (Org/Dataset)
    private String validateAndTruncateId(String id, String idType, String logPrefix) {
        if (id == null || id.isEmpty()) {
            logger.warn("{}{} is null or empty, this should not happen.", logPrefix, idType);
            return ""; // Or throw? Returning empty will likely cause failure later.
        }
        if (id.length() > MAX_ID_LENGTH) {
            logger.warn("{}{} ('{}') exceeds max length ({}), truncating.", logPrefix, idType, id, MAX_ID_LENGTH);
            id = id.substring(0, MAX_ID_LENGTH);
            // Remove trailing hyphen if truncation created one
            // Use pattern for consistency, though simple check is fine too
            id = LEADING_TRAILING_HYPHEN.matcher(id).replaceAll("");
        }
        // Final check if it became empty after truncation/prefixing
        if (id.isEmpty() || id.equals("-")) { // Check for single hyphen too
            logger.warn("{}{} became invalid after potential truncation/prefixing: '{}'", logPrefix, idType, id);
            return ""; // Let caller handle invalid ID
        }
        return id;
    }

    // Helper to validate and truncate Resource Names
    private String validateAndTruncateResourceName(String resourceName, String logPrefix) {
        if (resourceName == null || resourceName.isEmpty()) return "";

        if (resourceName.length() > MAX_ID_LENGTH) {
            String originalNameForLog = resourceName;
            String ext = getExtension(resourceName);
            String base = getBaseName(resourceName);
            int maxBaseLength = MAX_ID_LENGTH - (ext.isEmpty() ? 0 : ext.length() + 1);

            if (maxBaseLength < 1) { // Handle cases where extension itself is too long
                logger.warn("{}Resource name '{}' cannot be truncated effectively due to long extension/limit. Using first {} chars.", logPrefix, originalNameForLog, MAX_ID_LENGTH);
                // Fallback: just truncate the whole thing - might lose extension
                resourceName = resourceName.substring(0, MAX_ID_LENGTH);
            } else {
                resourceName = base.substring(0, maxBaseLength) + (ext.isEmpty() ? "" : "." + ext);
                logger.warn("{}Resource filename '{}' exceeds max length ({}), truncated to '{}'. Note: Truncation might cause name collisions.", logPrefix, originalNameForLog, MAX_ID_LENGTH, resourceName);
            }
        }
        // Basic check for empty result after potential truncation
        if (resourceName.isEmpty()){
            logger.error("{}Resource name became empty after truncation attempt.", logPrefix);
            return "";
        }
        return resourceName;
    }


    /** Logs final summary statistics of the pipeline run. */
    private void logRunSummary(Duration runDuration) {
        logger.info("==================================================");
        logger.info("--- Data Pipeline Run Summary ---");
        logger.info("Total ZIPs found: {}", totalFilesFound);
        logger.info("Successful ZIPs : {}", totalProcessedZips);
        logger.info("Failed ZIPs     : {}", totalErrorZips);
        // Calculate and format duration before logging
        double durationSeconds = runDuration.toMillis() / 1000.0;
        logger.info("Total Duration  : {} seconds.", String.format("%.3f", durationSeconds));
        logger.info("==================================================");
    }

    // --- Filename Helper Methods ---
    // RECOMMENDATION: Move getBaseName and getExtension to a shared utility class
    // (e.g., util.FilenameUtils) to avoid duplication with ZipProcessor.java

    /** Gets the base name of a file (name without last extension). */
    private static String getBaseName(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        // Handles no dot, dot at start, dot at end correctly
        if (dotIndex <= 0) return filename;
        else return filename.substring(0, dotIndex);
    }

    /** Gets the file extension (without the dot, lowercase). */
    private static String getExtension(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        // Handles no dot, dot at start, dot at end correctly
        if (dotIndex <= 0 || dotIndex == filename.length() - 1) return "";
        else return filename.substring(dotIndex + 1).toLowerCase(Locale.ENGLISH);
    }

    // --- TODO: Additions needed in ConfigLoader.java ---
    /*
    // In ConfigLoader constants:
    private static final String KEY_CKAN_ORG_PREFIX = "CKAN.org_prefix";

    // In ConfigLoader fields (make final if assigned in constructor):
    private final String ckanOrgPrefix;

    // In ConfigLoader constructor (after properties are loaded):
    // Example: Load from properties, provide default "org-"
    this.ckanOrgPrefix = properties.getProperty(KEY_CKAN_ORG_PREFIX, "org-").trim();
    // Optional: Add validation for prefix format if needed (e.g., ensure it ends with '-')

    // In ConfigLoader, add getter:
    public String getCkanOrgPrefix() {
        return ckanOrgPrefix;
    }
    */
} // End of Pipeline class