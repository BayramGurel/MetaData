// Save as: src/processing/ZipProcessor.java
// FIX: Standardized package name
package processing;

// FIX: Updated imports for assumed new locations
import config.ConfigLoader;
import util.LoggingUtil;

import org.apache.commons.io.FileUtils; // Requires commons-io dependency
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneOffset; // Needed for timestamp formatting
import java.time.format.DateTimeFormatter; // Needed for timestamp formatting
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.Objects; // For null checks

/**
 * Handles staging, extraction, filtering, and cleanup/processing of source ZIP files.
 * Uses configuration from ConfigLoader to determine behavior.
 */
public class ZipProcessor {

    private static final Logger logger = LoggingUtil.getLogger(ZipProcessor.class);
    // FIX: Formatter for unique timestamp suffix in moveToProcessed
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").withZone(ZoneOffset.UTC);

    private final Path sourceZipPath;
    private final ConfigLoader config;
    private final String originalName;
    private final String logPrefix; // For logging context [zipfilename]

    private Path stagedZipPath; // Path where the zip is copied in staging
    private Path extractSubdir; // Directory where contents are extracted


    /**
     * Creates a processor for a specific source ZIP file.
     * @param sourceZipPath The path to the source ZIP file.
     * @param config The application configuration.
     * @throws NullPointerException if sourceZipPath or config is null.
     * @throws RuntimeException if the staging directory cannot be created.
     */
    public ZipProcessor(Path sourceZipPath, ConfigLoader config) {
        this.sourceZipPath = Objects.requireNonNull(sourceZipPath, "sourceZipPath cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.originalName = sourceZipPath.getFileName().toString();
        this.logPrefix = "[" + this.originalName + "] ";

        // Define unique staging paths using timestamp
        long timestamp = Instant.now().toEpochMilli();
        // FIX: Replaced Guava file name splitting with manual implementation
        String baseName = getBaseName(originalName);
        String extension = getExtension(originalName);
        String uniqueSuffix = baseName + "_" + timestamp;

        // Ensure staging dir exists before resolving paths into it
        try {
            // Ensure staging dir is absolute before resolving into it
            Path stagingDirAbsolute = config.getStagingDir().toAbsolutePath();
            Files.createDirectories(stagingDirAbsolute);

            // Resolve paths within the absolute staging directory
            this.stagedZipPath = stagingDirAbsolute.resolve(uniqueSuffix + (extension.isEmpty() ? "" : "." + extension));
            this.extractSubdir = stagingDirAbsolute.resolve("extracted_" + uniqueSuffix);

            logger.debug("{}Initialized ZipProcessor. Staged path: {}, Extract dir: {}", logPrefix, stagedZipPath, extractSubdir);

        } catch (IOException e) {
            // This is critical - cannot proceed without staging dir
            logger.error("{}FATAL: Could not create staging directory: {}", logPrefix, config.getStagingDir(), e);
            throw new RuntimeException("Failed to create staging directory " + config.getStagingDir(), e);
        } catch (InvalidPathException e) {
            logger.error("{}FATAL: Invalid staging path derived. Base: {}, UniqueSuffix: {}, Ext: {}", logPrefix, baseName, uniqueSuffix, extension, e);
            throw new RuntimeException("Invalid path constructed for staging: " + e.getMessage(), e);
        }
    }

    /**
     * Copies the source ZIP file to a unique location within the staging directory.
     * Attempts to preserve the last modified time attribute.
     *
     * @return The path to the staged ZIP file copy.
     * @throws IOException If copying fails.
     */
    public Path stage() throws IOException {
        logger.info("{}Copying to staging area -> {}", logPrefix, stagedZipPath);
        try {
            Files.copy(sourceZipPath, stagedZipPath, StandardCopyOption.REPLACE_EXISTING);
            // Try to copy basic file attributes like last modified time
            try {
                FileTime lastModifiedTime = Files.getLastModifiedTime(sourceZipPath);
                Files.setLastModifiedTime(stagedZipPath, lastModifiedTime);
            } catch (IOException | SecurityException attrEx) {
                logger.warn("{}Could not copy file attributes: {}", logPrefix, attrEx.getMessage());
            }
            // Log relative path for brevity
            logger.info("{}Copied to '{}'", logPrefix, config.getStagingDir().relativize(stagedZipPath));
            return stagedZipPath;
        } catch (IOException e) {
            logger.error("{}Error copying '{}' to staging path '{}': {}", logPrefix, sourceZipPath, stagedZipPath, e.getMessage(), e);
            // Clean up partially copied file if it exists
            Files.deleteIfExists(stagedZipPath); // Use deleteIfExists for safety
            throw new IOException(logPrefix + "Error copying to staging: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the contents of the staged ZIP file into a unique subdirectory.
     * Filters the extracted files based on relevant extensions configured.
     * Handles nested ZIP exclusion based on configuration (does not recursively extract).
     * Includes path traversal protection.
     *
     * @return An unmodifiable list of paths to the extracted files selected for further processing (e.g., upload).
     * @throws IOException If the staged ZIP is not found/readable, is corrupt, or extraction fails.
     */
    public List<Path> extract() throws IOException {
        if (stagedZipPath == null || !Files.isReadable(stagedZipPath)) {
            throw new FileNotFoundException(logPrefix + "Staged ZIP file not found or not readable for extraction: " + (stagedZipPath != null ? stagedZipPath : "null"));
        }

        logger.info("{}Starting extraction to '{}'", logPrefix, config.getStagingDir().relativize(extractSubdir));
        List<Path> extractedFilesAll = new ArrayList<>();
        List<Path> filesToUpload; // Initialize later based on filtering

        try {
            Files.createDirectories(extractSubdir); // Ensure extraction target exists

            // Use ZipFile for better handling (random access, entry list)
            // Ensure we use the Path object converted to File for ZipFile constructor
            try (ZipFile zipFile = new ZipFile(stagedZipPath.toFile())) {
                var entries = zipFile.entries(); // Returns Enumeration<? extends ZipEntry>
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    // Resolve entry name against extraction dir, then normalize
                    Path entryDestination = extractSubdir.resolve(entry.getName()).normalize();

                    // **Security:** Prevent path traversal attacks
                    // Check if the normalized destination path is still within the extraction directory
                    if (!entryDestination.startsWith(extractSubdir)) {
                        logger.error("{}SECURITY RISK: ZIP entry attempted path traversal: '{}'. Skipping entry.", logPrefix, entry.getName());
                        continue; // Skip this potentially malicious entry
                    }

                    if (entry.isDirectory()) {
                        Files.createDirectories(entryDestination);
                    } else {
                        // Ensure parent directory exists for the file before writing
                        Path parentDir = entryDestination.getParent();
                        if (parentDir != null) { // Check if parent is not null (e.g., file in root of zip)
                            Files.createDirectories(parentDir);
                        }
                        // Extract the file content using try-with-resources
                        try (InputStream in = zipFile.getInputStream(entry);
                             OutputStream out = Files.newOutputStream(entryDestination)) { // Default options: CREATE, TRUNCATE_EXISTING
                            in.transferTo(out); // Efficient stream copying (Java 9+)
                        }
                        extractedFilesAll.add(entryDestination);
                    }
                }
            } // ZipFile closed automatically via try-with-resources

            logger.info("{}Extraction complete. {} total item(s) extracted.", logPrefix, extractedFilesAll.size());

            // Filter extracted files based on configured relevant extensions
            final List<String> relevantExtensions = config.getRelevantExtensions(); // Assumes list is lowercase
            if (relevantExtensions != null && !relevantExtensions.isEmpty()) {
                filesToUpload = extractedFilesAll.stream()
                        .filter(Files::isRegularFile) // Process only files
                        .filter(f -> {
                            String fileNameLower = f.getFileName().toString().toLowerCase();
                            // Check if file name ends with any configured relevant extension
                            return relevantExtensions.stream().anyMatch(fileNameLower::endsWith);
                        })
                        .collect(Collectors.toList());
                logger.info("{} {} file(s) selected for upload based on extensions: {}", logPrefix, filesToUpload.size(), relevantExtensions);
            } else {
                // If no extensions are specified, consider all extracted regular files
                filesToUpload = extractedFilesAll.stream()
                        .filter(Files::isRegularFile)
                        .collect(Collectors.toList());
                logger.info("{}All {} extracted regular file(s) will be considered for upload (no extension filter specified).", logPrefix, filesToUpload.size());
            }

            // Handle nested ZIPs exclusion (extraction of nested ZIPs is NOT implemented here)
            if (!config.isExtractNestedZips()) {
                int originalCount = filesToUpload.size();
                filesToUpload.removeIf(f -> f.getFileName().toString().toLowerCase().endsWith(".zip"));
                int removedCount = originalCount - filesToUpload.size();
                if (removedCount > 0) {
                    logger.debug("{} {} nested .zip file(s) excluded from upload list as '{}' is false.", logPrefix, removedCount, "ZipHandling.extract_nested_zips");
                }
            } else {
                // Warn if nested extraction enabled but not implemented
                boolean nestedZipsPresent = filesToUpload.stream()
                        .anyMatch(f -> f.getFileName().toString().toLowerCase().endsWith(".zip"));
                if (nestedZipsPresent) {
                    logger.warn("{}[NOT IMPLEMENTED] Extraction of nested ZIPs is enabled ('{}'=true) but not implemented. Nested ZIPs will be uploaded as-is.", logPrefix, "ZipHandling.extract_nested_zips");
                    // TODO: Implement recursive extraction using ZipProcessor if needed
                }
            }

            return Collections.unmodifiableList(filesToUpload); // Return immutable list

        } catch (ZipException e) {
            logger.error("{}Corrupt or invalid ZIP file encountered: {}", logPrefix, e.getMessage(), e);
            safeRemoveDir(extractSubdir); // Clean up potentially partial extraction
            throw new IOException(logPrefix + "Corrupt or invalid ZIP file: " + originalName, e);
        } catch (IOException e) {
            logger.error("{}Error during extraction process: {}", logPrefix, e.getMessage(), e);
            safeRemoveDir(extractSubdir); // Clean up
            throw new IOException(logPrefix + "Extraction failed for " + originalName + ": " + e.getMessage(), e);
        } catch (SecurityException e) {
            logger.error("{}Security permission error during extraction: {}", logPrefix, e.getMessage(), e);
            safeRemoveDir(extractSubdir); // Clean up
            throw new IOException(logPrefix + "Permission error during extraction for " + originalName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Removes the staged ZIP file copy and the extraction subdirectory.
     */
    public void cleanupStaging() {
        logger.info("{}Cleaning up staging area...", logPrefix);

        // Delete staged ZIP file
        if (stagedZipPath != null) { // Check if path variable itself is null
            try {
                boolean deleted = Files.deleteIfExists(stagedZipPath);
                if (deleted) {
                    logger.debug("{}Deleted staged ZIP: '{}'", logPrefix, stagedZipPath.getFileName());
                } else {
                    logger.debug("{}Staged ZIP not found, nothing to delete at: {}", logPrefix, stagedZipPath);
                }
            } catch (IOException e) {
                logger.warn("{}Could not delete staged ZIP '{}': {}", logPrefix, stagedZipPath.getFileName(), e.getMessage(), e);
            } catch (SecurityException e) {
                logger.warn("{}Permission error deleting staged ZIP '{}': {}", logPrefix, stagedZipPath.getFileName(), e.getMessage(), e);
            }
        } else {
            logger.warn("{}Staged ZIP path variable is null, cannot clean up.", logPrefix);
        }

        // Delete extraction directory using the helper method
        safeRemoveDir(extractSubdir);
    }

    // Safely remove a directory, ensuring it's within the staging area
    private void safeRemoveDir(Path dirToRemove) {
        if (dirToRemove == null) {
            logger.debug("{}Extraction directory path variable is null, nothing to remove.", logPrefix);
            return;
        }
        // Use try-exists for safety, avoid extra Files.exists check if dirToRemove is null
        try {
            if (!Files.exists(dirToRemove)) {
                logger.debug("{}Extraction directory '{}' not found, nothing to remove.", logPrefix, dirToRemove.getFileName());
                return;
            }
        } catch (SecurityException e) {
            logger.warn("{}Permission error checking existence of directory '{}': {}", logPrefix, dirToRemove, e.getMessage());
            return; // Can't check, can't delete safely
        }


        if (Files.isDirectory(dirToRemove)) {
            // **Security/Safety Check:** Ensure the directory is inside the staging directory before deleting
            Path stagingRoot = config.getStagingDir().toAbsolutePath().normalize();
            Path dirToRemoveAbsolute = dirToRemove.toAbsolutePath().normalize();

            if (dirToRemoveAbsolute.startsWith(stagingRoot) && !dirToRemoveAbsolute.equals(stagingRoot)) { // Don't delete staging root itself
                try {
                    // Use Apache Commons IO for robust recursive delete
                    logger.debug("{}Attempting to remove directory recursively: '{}'", logPrefix, dirToRemove);
                    FileUtils.deleteDirectory(dirToRemove.toFile());
                    logger.debug("{}Removed extraction directory: '{}'", logPrefix, dirToRemove.getFileName());
                } catch (IOException e) {
                    logger.warn("{}Could not remove extraction directory '{}': {}", logPrefix, dirToRemove.getFileName(), e.getMessage(), e);
                } catch (IllegalArgumentException e) {
                    // FileUtils.deleteDirectory throws this if path is not a directory (should be caught by isDirectory check)
                    logger.error("{}Path '{}' is not a directory, cannot remove recursively (should not happen here): {}", logPrefix, dirToRemove.getFileName(), e.getMessage());
                } catch (SecurityException e) {
                    logger.warn("{}Permission error removing directory '{}': {}", logPrefix, dirToRemove.getFileName(), e.getMessage(), e);
                }
            } else {
                logger.error("{}SAFETY PREVENTED DELETE: Attempted to remove directory '{}' which is not strictly within the staging area root '{}'.",
                        logPrefix, dirToRemoveAbsolute, stagingRoot);
            }
        } else {
            // Path exists but is not a directory
            logger.warn("{}Path '{}' exists but is not a directory, cannot remove as directory.", logPrefix, dirToRemove.getFileName());
        }
    }


    /**
     * Moves the original source ZIP file to the configured processed directory.
     * If moving is disabled or the processed directory is not configured, this method does nothing.
     * Handles filename conflicts in the destination by appending a timestamp.
     *
     * @throws IOException If the source file doesn't exist or the move operation fails.
     */
    public void moveToProcessed() throws IOException {
        Path targetProcessedDir = config.getProcessedDir();

        if (!config.isMoveProcessed() || targetProcessedDir == null) {
            logger.debug("{}Skipping move to processed directory (disabled or not configured).", logPrefix);
            return;
        }

        // Check if source file still exists (it might have been deleted manually or failed staging)
        if (!Files.isReadable(sourceZipPath)) { // Check readability as well
            logger.warn("{}Source file '{}' no longer exists or is not readable. Cannot move to processed.", logPrefix, sourceZipPath);
            // Consider if this should be an error/exception depending on workflow needs
            return;
        }

        // Ensure target dir is absolute for resolving
        Path targetProcessedDirAbsolute = targetProcessedDir.toAbsolutePath();
        logger.info("{}Moving original file '{}' to processed directory: {}", logPrefix, sourceZipPath.getFileName(), targetProcessedDirAbsolute);

        try {
            Files.createDirectories(targetProcessedDirAbsolute); // Ensure target dir exists

            // FIX: Replaced Guava with manual implementation
            String baseName = getBaseName(originalName);
            String extension = getExtension(originalName);
            Path destinationPath = targetProcessedDirAbsolute.resolve(originalName);

            // Handle potential naming conflicts by appending a timestamp
            if (Files.exists(destinationPath)) {
                String timestampSuffix = TIMESTAMP_FORMATTER.format(Instant.now());
                String newName = String.format("%s_%s%s", baseName, timestampSuffix, (extension.isEmpty() ? "" : "." + extension));
                destinationPath = targetProcessedDirAbsolute.resolve(newName);
                logger.warn("{}File '{}' already existed in processed directory. Renaming original to '{}'.", logPrefix, originalName, destinationPath.getFileName());
            }

            // Attempt atomic move first, fallback if needed
            try {
                logger.debug("{}Attempting atomic move: '{}' -> '{}'", logPrefix, sourceZipPath, destinationPath);
                Files.move(sourceZipPath, destinationPath, StandardCopyOption.ATOMIC_MOVE);
                logger.info("{}Original file successfully moved atomically to '{}'.", logPrefix, destinationPath);
            } catch (AtomicMoveNotSupportedException e) {
                logger.warn("{}Atomic move not supported (likely cross-filesystem). Attempting standard move.", logPrefix);
                try {
                    // Standard move - Ensure REPLACE_EXISTING is NOT used unless intended overwrite
                    // Our conflict handling above should prevent needing REPLACE_EXISTING here.
                    logger.debug("{}Attempting standard move: '{}' -> '{}'", logPrefix, sourceZipPath, destinationPath);
                    Files.move(sourceZipPath, destinationPath);
                    logger.info("{}Original file successfully moved (non-atomically) to '{}'.", logPrefix, destinationPath);
                } catch (IOException moveEx) {
                    logger.error("{}Error moving file (non-atomic fallback) to processed directory: {}. File remains in source directory.", logPrefix, moveEx.getMessage(), moveEx);
                    throw moveEx; // Re-throw the exception to indicate move failure
                }
            } catch (IOException moveEx) {
                // Catch other IO errors during atomic move attempt
                logger.error("{}Error moving file (atomic attempt) to processed directory: {}. File remains in source directory.", logPrefix, moveEx.getMessage(), moveEx);
                throw moveEx;
            }
        } catch (IOException e) {
            // Catch errors from createDirectories or general move issues
            logger.error("{}Error preparing move to processed directory '{}': {}. File remains in source directory.", logPrefix, targetProcessedDirAbsolute, e.getMessage(), e);
            throw e; // Re-throw the exception
        } catch (SecurityException e) {
            logger.error("{}Permission error during move to processed directory '{}': {}. File remains in source directory.", logPrefix, targetProcessedDirAbsolute, e.getMessage(), e);
            // Wrap in IOException? Or let SecurityException propagate? Let's wrap for consistency.
            throw new IOException("Permission error moving file to processed directory: " + e.getMessage(), e);
        }
    }

    // --- Getters ---
    public String getOriginalName() { return originalName; }
    public Path getExtractSubdir() { return extractSubdir; }
    public String getLogPrefix() { return logPrefix; }

    // --- Helper Methods for Filename Splitting (Replaces Guava) ---

    /**
     * Gets the base name of a file (name without extension).
     * @param filename The full filename.
     * @return The base name, or the full filename if no extension is found.
     */
    private static String getBaseName(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        // Handle cases: no dot, dot at start (.bashrc), dot at end (archive.)
        if (dotIndex <= 0) {
            return filename;
        } else {
            return filename.substring(0, dotIndex);
        }
    }

    /**
     * Gets the file extension (without the dot).
     * @param filename The full filename.
     * @return The extension (lowercase), or an empty string if no extension is found.
     */
    private static String getExtension(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        // Handle cases: no dot, dot at start (.bashrc), dot at end (archive.)
        if (dotIndex <= 0 || dotIndex == filename.length() - 1) {
            return "";
        } else {
            return filename.substring(dotIndex + 1).toLowerCase(); // Return lowercase extension
        }
    }
}