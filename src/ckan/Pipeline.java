// src/ckan/Pipeline.java
package ckan;

// Imports remain the same as in the previous detailed step for Pipeline
import ckan.CkanExceptions; // Use the container class
import ckan.CkanExceptions.CkanAuthorizationException;
import ckan.CkanExceptions.CkanException;
import ckan.CkanExceptions.CkanNotFoundException;
import ckan.LoggingUtil;
import org.slf4j.Logger; // Use SLF4J logger
import com.google.common.io.Files as GuavaFiles; // Requires Guava

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

public class Pipeline {

    private static final Logger logger = LoggingUtil.getLogger(Pipeline.class);
    // Pattern for characters NOT allowed in CKAN IDs (slugs)
    // Allows lowercase alphanumeric, underscore, hyphen.
    private static final Pattern INVALID_ID_CHARS = Pattern.compile("[^a-z0-9_\\-]+");
    // Pattern to replace multiple hyphens/underscores or leading/trailing ones
    private static final Pattern NORMALIZE_ID_SEPARATORS = Pattern.compile("-{2,}|_{2,}|^-|-$|^_|-_$");
    private static final int MAX_ID_LENGTH = 100; // CKAN default max package/resource name length

    private final ConfigLoader config;
    private final CkanHandler ckanHandler;
    private int totalFilesFound = 0;
    private int totalProcessedZips = 0;
    private int totalErrorZips = 0;

    public Pipeline(ConfigLoader config) {
        // Pipeline constructor logic remains the same...
        this.config = config;
        // Initialize CkanHandler - constructor handles basic validation
        this.ckanHandler = new CkanHandler(config.getCkanUrl(), config.getCkanApiKey());
        logger.info("Pipeline initialized.");
    }

    public void run() {
        // run method logic remains the same...
        Instant runStartTime = Instant.now();
        logger.info("==================================================");
        logger.info("--- Starting Data Pipeline Run ---");
        logger.info("Timestamp: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
        logConfigurationSummary();
        logger.info("==================================================");

        totalFilesFound = 0;
        totalProcessedZips = 0;
        totalErrorZips = 0;
        List<Path> inputFiles = Collections.emptyList();

        try {
            // Check source directory readiness just before processing
            validateSourceDirectory(); // Throws IOException if validation fails
            inputFiles = detectInputFiles();
            totalFilesFound = inputFiles.size(); // Store total found

            if (inputFiles.isEmpty()) {
                logger.info("No input ZIP files found in '{}' matching criteria.", config.getSourceDir());
            } else {
                logger.info("Processing {} ZIP file(s)...", inputFiles.size());
                for (int i = 0; i < inputFiles.size(); i++) {
                    processSingleZip(inputFiles.get(i), i + 1, inputFiles.size());
                    // Optional: Add short delay between files if needed to avoid overwhelming CKAN?
                    // Thread.sleep(1000); // Example: 1 second delay
                    logger.info("--- Progress: {}/{} files processed ---", i + 1, inputFiles.size()); // Progress indicator
                }
            }
        } catch (IOException | IllegalArgumentException | SecurityException e) {
            logger.error("Critical error accessing source directory or initial setup: {}. Aborting run.", e.getMessage(), e);
            // Estimate remaining as errors if file list was populated
            totalErrorZips = Math.max(0, totalFilesFound - totalProcessedZips);
        } catch (Exception e) {
            // Catch unexpected errors during the main loop
            logger.error("Unexpected critical error during pipeline execution: {}", e.getMessage(), e);
            totalErrorZips = Math.max(0, totalFilesFound - totalProcessedZips); // Estimate errors
        } finally {
            Instant runEndTime = Instant.now();
            Duration runDuration = Duration.between(runStartTime, runEndTime);
            logRunSummary(runDuration); // Pass duration to summary method
        }
    }

    private void logConfigurationSummary() {
        // logConfigurationSummary logic remains the same...
        logger.info("Config File: '{}'", config.getConfigPath());
        logger.info("Source Dir : '{}'", config.getSourceDir());
        logger.info("Staging Dir: '{}'", config.getStagingDir());
        logger.info("Processed Dir: {} {}", config.getProcessedDir() != null ? config.getProcessedDir() : "N/A", config.isMoveProcessed() ? "(Move Enabled)" : "(Move Disabled)");
        logger.info("Log File   : '{}'", config.getLogFile());
        logger.info("CKAN URL   : '{}'", config.getCkanUrl());
        logger.info("Create Orgs: {} {}", config.isCreateOrganizations(), (config.isCreateOrganizations() ? "(Sysadmin Key Required!)" : ""));
        logger.info("Relevant Ext: {}", config.getRelevantExtensions().isEmpty() ? "All files" : config.getRelevantExtensions());
        logger.info("Extract Nested: {}", config.isExtractNestedZips());
    }

    private void validateSourceDirectory() throws IOException {
        // validateSourceDirectory logic remains the same...
        Path sourceDir = config.getSourceDir();
        if (!Files.exists(sourceDir)) {
            logger.error("Source directory '{}' does not exist.", sourceDir);
            throw new FileNotFoundException("Source directory not found: " + sourceDir);
        }
        if (!Files.isDirectory(sourceDir)) {
            logger.error("Source path '{}' is not a directory.", sourceDir);
            throw new NotDirectoryException("Source path is not a directory: " + sourceDir);
        }
        if (!Files.isReadable(sourceDir)) {
            logger.error("Source directory '{}' is not readable.", sourceDir);
            throw new AccessDeniedException("Cannot read source directory: " + sourceDir);
        }
        logger.debug("Source directory '{}' validated successfully.", sourceDir);
    }

    private List<Path> detectInputFiles() {
        // detectInputFiles logic remains the same...
        logger.info("Scanning for input ZIPs in: {}", config.getSourceDir());
        List<Path> zipFiles = new ArrayList<>();

        // Use try-with-resources for DirectoryStream to ensure it's closed
        try (Stream<Path> stream = Files.list(config.getSourceDir())) {
            zipFiles = stream
                    .filter(Files::isRegularFile) // Ensure it's a file
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".zip")) // Check extension
                    .filter(p -> { // Add check for readability
                        if (!Files.isReadable(p)) {
                            logger.warn("Skipping unreadable file: {}", p.getFileName());
                            return false;
                        }
                        // Optional: Add file size check here if needed
                        // try { if (Files.size(p) == 0) { logger.warn("Skipping empty file: {}", p.getFileName()); return false; } } catch (IOException ioex) {/* handle */}
                        return true;
                    })
                    .sorted() // Process files in a consistent order (alphabetical)
                    .collect(Collectors.toList());
            logger.info("{} readable input ZIP file(s) found.", zipFiles.size());
            return zipFiles;
        } catch (IOException e) {
            logger.error("Error reading source directory contents '{}': {}", config.getSourceDir(), e.getMessage(), e);
            return Collections.emptyList(); // Return empty list on error
        }
    }

    private void processSingleZip(Path zipPath, int index, int totalFiles) {
        // processSingleZip logic remains the same...
        // Ensure CkanException and subtypes are caught correctly using CkanExceptions.*
        ZipProcessor processor = new ZipProcessor(zipPath, config); // Handles its own logging prefix
        Instant zipStartTime = Instant.now();
        logger.info("--- [{}/{}] Start processing ZIP: '{}' ---", index, totalFiles, processor.getOriginalName());
        boolean zipProcessedSuccessfully = false;

        try {
            // 1. Stage the ZIP file
            processor.stage(); // throws IOException

            // 2. Extract relevant files
            List<Path> filesToUpload = processor.extract(); // throws IOException

            // 3. Publish extracted files to CKAN
            if (!filesToUpload.isEmpty()) {
                publishToCkan(processor.getOriginalName(), processor.getExtractSubdir(), filesToUpload, processor.getLogPrefix()); // throws CkanException, IOException
            } else {
                logger.info("{}No relevant files found after extraction based on configuration, skipping CKAN publication.", processor.getLogPrefix());
            }

            // If all steps above complete without throwing an exception, mark as successful
            zipProcessedSuccessfully = true;
            totalProcessedZips++; // Increment success counter ONLY if all steps passed
            logger.info("{}Successfully processed.", processor.getLogPrefix());

        } catch (CkanAuthorizationException e) { // Catch specific CKAN exceptions first
            totalErrorZips++;
            logger.error("{}CKAN Authorization Error: {}. Check API key permissions. File processing aborted.", processor.getLogPrefix(), e.getMessage());
        } catch (CkanNotFoundException e) {
            totalErrorZips++;
            logger.error("{}CKAN Not Found Error: {}. Target organization/dataset/resource may not exist or ID is incorrect. File processing aborted.", processor.getLogPrefix(), e.getMessage());
        } catch (CkanException e) { // Catch other CKAN API errors
            totalErrorZips++;
            logger.error("{}CKAN API Error: {}. File processing aborted.", processor.getLogPrefix(), e.getMessage());
            logger.debug("{}CKAN API Error Stack Trace:", processor.getLogPrefix(), e); // Debug level for stack trace
        } catch (IOException e) { // Catch file/IO errors (staging, extraction, moving)
            totalErrorZips++;
            logger.error("{}File I/O Error during processing: {}. File processing aborted.", processor.getLogPrefix(), e.getMessage());
            logger.debug("{}I/O Error Stack Trace:", processor.getLogPrefix(), e); // Debug level for stack trace
        } catch (RuntimeException e) { // Catch unexpected runtime errors
            totalErrorZips++;
            logger.error("{}Unexpected Runtime Error during processing: {}. File processing aborted.", processor.getLogPrefix(), e.getMessage());
            logger.error("{}Runtime Error Stack Trace:", processor.getLogPrefix(), e); // Error level for unexpected runtime stack trace
        } finally {
            // 4. Cleanup Staging Area (always attempt this)
            try {
                processor.cleanupStaging();
            } catch (Exception cleanupEx) {
                logger.error("{}Error during staging area cleanup: {}", processor.getLogPrefix(), cleanupEx.getMessage(), cleanupEx);
                // Log cleanup error but don't increment totalErrorZips again for this
            }

            // 5. Move Original to Processed Directory (only if main processing was successful and move is enabled)
            if (config.isMoveProcessed()) {
                if (zipProcessedSuccessfully) {
                    try {
                        processor.moveToProcessed(); // throws IOException
                    } catch (IOException moveEx) {
                        // Log error, but the main processing was still successful overall for counting purposes
                        logger.error("{}Failed to move successfully processed file '{}' to processed directory: {}",
                                processor.getLogPrefix(), processor.getOriginalName(), moveEx.getMessage(), moveEx);
                        // Do not increment totalErrorZips here, as the core task succeeded.
                    }
                } else {
                    logger.warn("{}Skipping move to processed directory for '{}' due to processing errors.", processor.getLogPrefix(), processor.getOriginalName());
                }
            }

            Instant zipEndTime = Instant.now();
            Duration zipDuration = Duration.between(zipStartTime, zipEndTime);
            String status = zipProcessedSuccessfully ? "COMPLETED" : "FAILED";
            logger.info("--- [{}/{}] Finished processing ZIP: '{}'. Status: {}. Duration: {:.2f} sec. ---",
                    index, totalFiles, processor.getOriginalName(), status, zipDuration.toMillis() / 1000.0);
        }
    }

    // Convert filename to CKAN-friendly ID (slug)
    private String createCkanId(String name) {
        // createCkanId logic remains the same...
        // Use Guava for name without extension, or implement manually
        String baseName = GuavaFiles.getNameWithoutExtension(name);

        String cleaned = baseName.toLowerCase()
                .replace(' ', '-') // Replace spaces with hyphens
                .replaceAll("[^a-z0-9_\\-]+", ""); // Remove invalid characters (keep only lowercase alphanumeric, underscore, hyphen)

        // Normalize separators: replace multiple hyphens/underscores with single ones, remove leading/trailing
        // Repeatedly apply normalization until no changes occur
        String previous;
        do {
            previous = cleaned;
            cleaned = NORMALIZE_ID_SEPARATORS.matcher(cleaned).replaceAll(match -> {
                if (match.group().contains("-")) return "-";
                if (match.group().contains("_")) return "_";
                return ""; // For leading/trailing matches or adjacent -_ combinations
            });
        } while (!previous.equals(cleaned));


        // Enforce max length AFTER cleaning separators
        if (cleaned.length() > MAX_ID_LENGTH) {
            cleaned = cleaned.substring(0, MAX_ID_LENGTH);
            // Re-trim in case cut left a trailing separator (less likely now due to loop above)
            cleaned = NORMALIZE_ID_SEPARATORS.matcher(cleaned).replaceAll(match -> match.group().startsWith("-") || match.group().startsWith("_") ? "" : match.group());
        }

        // Ensure it's not empty after cleaning
        if (cleaned.isEmpty() || cleaned.equals("-") || cleaned.equals("_")) {
            logger.warn("Could not derive valid CKAN ID from '{}'. Generating fallback ID.", name);
            // Generate a more robust fallback ID using timestamp
            return "item-" + Instant.now().toEpochMilli();
        }

        logger.trace("Converted '{}' to CKAN ID: '{}'", name, cleaned);
        return cleaned;
    }

    // Create human-readable title from filename
    private String createCkanTitle(String name) {
        // createCkanTitle logic remains the same...
        // Use Guava or manual implementation
        String baseName = GuavaFiles.getNameWithoutExtension(name);
        // Simple title case: replace underscores/hyphens with space, capitalize words
        String title = baseName.replace('_', ' ').replace('-', ' ');
        // Basic title casing (can be improved with libraries like Apache Commons Text WordUtils if needed)
        return Pattern.compile("\\b(.)(.*?)\\b")
                .matcher(title.toLowerCase())
                .replaceAll(match -> match.group(1).toUpperCase() + match.group(2));
    }


    private void publishToCkan(String originalZipName, Path extractSubdir, List<Path> filesToUpload, String logPrefix)
            throws CkanExceptions.CkanException, IOException { // Use qualified throws
        // publishToCkan logic remains the same...
        // Ensure CkanException and subtypes are caught/thrown correctly using CkanExceptions.*
        logger.info("{}Starting CKAN publication process for {} file(s)...", logPrefix, filesToUpload.size());

        // --- Determine Organization and Dataset IDs/Titles ---
        // Use the filename of the original ZIP to derive IDs and titles
        String baseId = createCkanId(originalZipName);
        String baseTitle = createCkanTitle(originalZipName);

        // Define conventions (e.g., prefix for orgs) - make configurable?
        String orgPrefix = config.properties.getProperty("CKAN.org_prefix", "org-"); // Example: get prefix from config, default "org-"
        String targetOrgId = orgPrefix + baseId;
        String targetOrgTitle = baseTitle; // Org title often same as dataset

        // Dataset ID usually doesn't need the prefix
        String datasetId = baseId;
        String datasetTitle = baseTitle;

        // Re-validate derived IDs (createCkanId should handle most cases)
        if (targetOrgId.length() > MAX_ID_LENGTH || datasetId.length() > MAX_ID_LENGTH) {
            logger.warn("{}Derived Org ID ('{}') or Dataset ID ('{}') exceeds max length ({}) after prefixing/cleaning. Consider shorter source filenames or different naming logic.", logPrefix, targetOrgId, datasetId, MAX_ID_LENGTH);
            // Option: Truncate again or handle error depending on policy
            targetOrgId = targetOrgId.length() > MAX_ID_LENGTH ? targetOrgId.substring(0, MAX_ID_LENGTH) : targetOrgId;
            datasetId = datasetId.length() > MAX_ID_LENGTH ? datasetId.substring(0, MAX_ID_LENGTH) : datasetId;
            logger.warn("{}IDs truncated to Org: '{}', Dataset: '{}'", logPrefix, targetOrgId, datasetId);
        }

        logger.info("{}Target Organization ID (derived): '{}', Title: '{}'", logPrefix, targetOrgId, targetOrgTitle);
        logger.info("{}Target Dataset ID (derived)     : '{}', Title: '{}'", logPrefix, datasetId, datasetTitle);


        String finalOwnerOrgId; // The actual ID confirmed or created in CKAN

        // --- Step 1: Ensure Organization Exists ---
        Optional<Map<String, Object>> existingOrgOpt = ckanHandler.checkOrganizationExists(targetOrgId);

        if (existingOrgOpt.isPresent()) {
            finalOwnerOrgId = (String) existingOrgOpt.get().get("id"); // Use the confirmed ID from CKAN
            logger.info("{}Found existing organization: ID '{}'", logPrefix, finalOwnerOrgId);
        } else {
            // Organization does not exist
            logger.info("{}Organization '{}' not found.", logPrefix, targetOrgId);
            if (config.isCreateOrganizations()) {
                logger.warn("{}Attempting to automatically create organization '{}' (requires sysadmin API key!)...", logPrefix, targetOrgId);
                try {
                    Map<String, Object> createdOrg = ckanHandler.createOrganization(targetOrgId, targetOrgTitle);
                    finalOwnerOrgId = (String) createdOrg.get("id");
                    logger.info("{}Successfully created organization: ID '{}'", logPrefix, finalOwnerOrgId);
                } catch (CkanExceptions.CkanException createEx) { // Qualified catch
                    logger.error("{}Failed to create organization '{}': {}", logPrefix, targetOrgId, createEx.getMessage());
                    throw createEx; // Propagate the creation error - cannot continue without org
                }
            } else {
                // Not found and not allowed to create
                String errorMsg = logPrefix + "Target organization '" + targetOrgId + "' not found, and 'Behaviour.create_organizations' is disabled in config. Cannot proceed.";
                logger.error(errorMsg);
                throw new CkanExceptions.CkanNotFoundException(errorMsg); // Throw specific exception
            }
        }

        // We must have a valid org ID by now
        logger.info("{}Using Organization ID: '{}' for dataset.", logPrefix, finalOwnerOrgId);


        // --- Step 2: Get or Create Dataset ---
        Map<String, Object> dataset = ckanHandler.getOrCreateDataset(datasetId, datasetTitle, finalOwnerOrgId, originalZipName);
        String packageId = (String) dataset.get("id"); // Get the confirmed package ID (might differ from requested datasetId if CKAN modifies it)
        logger.info("{}Using Dataset ID: '{}' for resources.", logPrefix, packageId);


        // --- Step 3: Get Existing Resources (by name) to check for updates ---
        Map<String, String> existingResources = ckanHandler.getExistingResourceNamesAndIds(packageId);
        logger.debug("{}Found existing resources: {}", logPrefix, existingResources);


        // --- Step 4: Upload/Update Resources ---
        int successfulUploads = 0;
        int failedUploads = 0;
        List<String> errorMessages = new ArrayList<>();

        for (Path filePath : filesToUpload) {
            String resourceName = filePath.getFileName().toString();
            // Sanitize resource name if necessary, CKAN resource names have length limits but fewer character restrictions than IDs
            if (resourceName.length() > MAX_ID_LENGTH) {
                logger.warn("{}Resource filename '{}' exceeds max length ({}), truncating.", logPrefix, resourceName, MAX_ID_LENGTH);
                // Basic truncation, ensure uniqueness might be needed if truncations collide
                resourceName = resourceName.substring(0, MAX_ID_LENGTH);
                // Consider adding a hash or counter if simple truncation causes name collisions
            }
            String existingResourceId = existingResources.get(resourceName); // Check if resource exists by this name

            try {
                // Generate description and format
                Path relativePath = extractSubdir.relativize(filePath); // Get path relative to extraction dir
                String description = String.format("File '%s' from archive '%s', processed on %s.",
                        relativePath,
                        originalZipName,
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")));

                // Use Guava or manual implementation for extension
                String format = GuavaFiles.getFileExtension(resourceName);
                // Normalize format: lowercase, handle common cases, default if empty
                if (format == null || format.isEmpty()) {
                    format = "data"; // Default if no extension
                } else {
                    format = format.toLowerCase();
                    // Optional: Map common extensions to CKAN-preferred formats (e.g., "xlsx" -> "XLSX")
                }


                // Perform the upload/update call
                ckanHandler.uploadOrUpdateResource(packageId, filePath, resourceName, description, format, existingResourceId);
                successfulUploads++;
            } catch (Exception e) { // Catch broadly here to report failures per resource
                failedUploads++;
                String errorMsg = String.format("%sFailed to process resource '%s': %s", logPrefix, resourceName, e.getMessage());
                // Log full stack trace for resource failures at ERROR level for visibility
                logger.error(errorMsg, e);
                errorMessages.add("Resource '" + resourceName + "': " + e.getMessage());
                // Decide whether to continue with other resources or fail the whole zip
                // Current logic continues processing other resources
            }
        }

        logger.info("{}CKAN resource processing finished. Successful: {}, Failed: {}", logPrefix, successfulUploads, failedUploads);

        // If any resource uploads failed, throw an exception to mark the whole ZIP process as failed
        if (failedUploads > 0) {
            throw new CkanExceptions.CkanException(logPrefix + failedUploads + " resource(s) failed to upload/update. First error: " + (errorMessages.isEmpty() ? "Unknown" : errorMessages.get(0)));
        }
    }

    private void logRunSummary(Duration runDuration) { // Accept duration as parameter
        logger.info("==================================================");
        logger.info("--- Data Pipeline Run Finished ---");
        logger.info("Total ZIPs found in source: {}", totalFilesFound);
        logger.info("Total ZIPs successfully processed: {}", totalProcessedZips);
        logger.info("Total ZIPs with errors: {}", totalErrorZips);
        logger.info("Total run duration: {:.2f} seconds.", runDuration.toMillis() / 1000.0);
        logger.info("==================================================");
    }
}