// Save as: src/processing/ZipProcessor.java
package processing;

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.Objects;

/**
 * Handles staging, extraction, filtering, and cleanup/processing of source ZIP files.
 * Uses configuration from ConfigLoader to determine behavior.
 * This class is final as it's not designed for extension.
 */
public final class ZipProcessor { // Made class final

    private static final Logger logger = LoggingUtil.getLogger(ZipProcessor.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").withZone(ZoneOffset.UTC);

    private final Path sourceZipPath;
    private final ConfigLoader config;
    private final String originalName;
    private final String logPrefix; // For logging context [zipfilename]

    // Staging locations are determined at construction and are fixed for this instance
    private final Path stagedZipPath; // Path where the zip is copied in staging
    private final Path extractSubdir; // Directory where contents are extracted


    /**
     * Creates a processor for a specific source ZIP file.
     * @param sourceZipPath The path to the source ZIP file.
     * @param config The application configuration.
     * @throws NullPointerException if sourceZipPath or config is null.
     * @throws RuntimeException if the staging directory cannot be created or initial paths are invalid.
     */
    public ZipProcessor(Path sourceZipPath, ConfigLoader config) {
        this.sourceZipPath = Objects.requireNonNull(sourceZipPath, "sourceZipPath cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.originalName = sourceZipPath.getFileName().toString();
        this.logPrefix = "[" + this.originalName + "] ";

        // Temporary variables for path construction before assigning to final fields
        Path tempStagedZipPath;
        Path tempExtractSubdir;

        try {
            // Define unique staging paths using timestamp
            long timestamp = Instant.now().toEpochMilli();
            String baseName = getBaseName(originalName);
            String extension = getExtension(originalName);
            String uniqueSuffix = baseName + "_" + timestamp;

            // Ensure staging dir exists and is absolute before resolving paths into it
            Path stagingDirAbsolute = config.getStagingDir().toAbsolutePath();
            Files.createDirectories(stagingDirAbsolute);

            // Resolve paths within the absolute staging directory
            tempStagedZipPath = stagingDirAbsolute.resolve(uniqueSuffix + (extension.isEmpty() ? "" : "." + extension));
            tempExtractSubdir = stagingDirAbsolute.resolve("extracted_" + uniqueSuffix);

            logger.debug("{}Initialized ZipProcessor. Staged path: {}, Extract dir: {}", logPrefix, tempStagedZipPath, tempExtractSubdir);

        } catch (IOException e) {
            // This is critical - cannot proceed without staging dir
            logger.error("{}FATAL: Could not create staging directory: {}", logPrefix, config.getStagingDir(), e);
            throw new RuntimeException("Failed to create staging directory " + config.getStagingDir(), e);
        } catch (InvalidPathException e) {
            String derivedStagingPath = config.getStagingDir().toString() + "/" + getBaseName(originalName) + "_<ts>" + "." + getExtension(originalName); // Approximate path for logging
            logger.error("{}FATAL: Invalid staging path derived. Attempted path structure like: {}", logPrefix, derivedStagingPath, e);
            throw new RuntimeException("Invalid path constructed for staging: " + e.getMessage(), e);
        }

        // Assign to final fields after successful creation
        this.stagedZipPath = tempStagedZipPath;
        this.extractSubdir = tempExtractSubdir;
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
        // stagedZipPath is final, so no null check needed after constructor succeeds
        if (!Files.isReadable(stagedZipPath)) {
            throw new FileNotFoundException(logPrefix + "Staged ZIP file not readable for extraction: " + stagedZipPath);
        }

        logger.info("{}Starting extraction to '{}'", logPrefix, config.getStagingDir().relativize(extractSubdir));
        List<Path> extractedFilesAll = new ArrayList<>();
        List<Path> filesToUpload; // Initialize later based on filtering

        try {
            Files.createDirectories(extractSubdir); // Ensure extraction target exists

            // Use ZipFile for better handling (random access, entry list)
            try (ZipFile zipFile = new ZipFile(stagedZipPath.toFile())) {
                var entries = zipFile.entries(); // Returns Enumeration<? extends ZipEntry>
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    Path entryDestination = extractSubdir.resolve(entry.getName()).normalize();

                    // **Security:** Prevent path traversal attacks
                    if (!entryDestination.startsWith(extractSubdir)) {
                        logger.error("{}SECURITY RISK: ZIP entry attempted path traversal: '{}'. Skipping entry.", logPrefix, entry.getName());
                        continue;
                    }

                    if (entry.isDirectory()) {
                        Files.createDirectories(entryDestination);
                    } else {
                        Path parentDir = entryDestination.getParent();
                        if (parentDir != null) {
                            Files.createDirectories(parentDir);
                        }
                        try (InputStream in = zipFile.getInputStream(entry);
                             OutputStream out = Files.newOutputStream(entryDestination)) {
                            in.transferTo(out);
                        }
                        extractedFilesAll.add(entryDestination);
                    }
                }
            } // ZipFile closed automatically

            logger.info("{}Extraction complete. {} total item(s) extracted.", logPrefix, extractedFilesAll.size());

            // Filter extracted files based on configured relevant extensions
            final List<String> relevantExtensions = config.getRelevantExtensions();
            if (!relevantExtensions.isEmpty()) { // getRelevantExtensions returns unmodifiable emptyList if null/empty
                filesToUpload = extractedFilesAll.stream()
                        .filter(Files::isRegularFile)
                        .filter(f -> {
                            String fileNameLower = f.getFileName().toString().toLowerCase();
                            return relevantExtensions.stream().anyMatch(fileNameLower::endsWith);
                        })
                        .collect(Collectors.toList());
                logger.info("{} {} file(s) selected for upload based on extensions: {}", logPrefix, filesToUpload.size(), relevantExtensions);
            } else {
                filesToUpload = extractedFilesAll.stream()
                        .filter(Files::isRegularFile)
                        .collect(Collectors.toList());
                logger.info("{}All {} extracted regular file(s) will be considered for upload (no extension filter specified).", logPrefix, filesToUpload.size());
            }

            // Handle nested ZIPs exclusion
            if (!config.isExtractNestedZips()) {
                int originalCount = filesToUpload.size();
                filesToUpload.removeIf(f -> f.getFileName().toString().toLowerCase().endsWith(".zip"));
                int removedCount = originalCount - filesToUpload.size();
                if (removedCount > 0) {
                    logger.debug("{} {} nested .zip file(s) excluded from upload list as '{}' is false.", logPrefix, removedCount, "ZipHandling.extract_nested_zips");
                }
            } else {
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

        // Delete staged ZIP file (stagedZipPath is final, non-null)
        try {
            boolean deleted = Files.deleteIfExists(stagedZipPath);
            if (deleted) {
                logger.debug("{}Deleted staged ZIP: '{}'", logPrefix, stagedZipPath.getFileName());
            } else {
                // Should generally exist if stage() succeeded, but log if not found
                logger.debug("{}Staged ZIP not found, nothing to delete at: {}", logPrefix, stagedZipPath);
            }
        } catch (IOException e) {
            logger.warn("{}Could not delete staged ZIP '{}': {}", logPrefix, stagedZipPath.getFileName(), e.getMessage(), e);
        } catch (SecurityException e) {
            logger.warn("{}Permission error deleting staged ZIP '{}': {}", logPrefix, stagedZipPath.getFileName(), e.getMessage(), e);
        }

        // Delete extraction directory using the helper method (extractSubdir is final, non-null)
        safeRemoveDir(extractSubdir);
    }

    // Safely remove a directory, ensuring it's within the staging area
    private void safeRemoveDir(Path dirToRemove) {
        // dirToRemove is final field, assigned in constructor, no null check needed here
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

            if (dirToRemoveAbsolute.startsWith(stagingRoot) && !dirToRemoveAbsolute.equals(stagingRoot)) {
                try {
                    // Use Apache Commons IO for robust recursive delete
                    logger.debug("{}Attempting to remove directory recursively: '{}'", logPrefix, dirToRemove);
                    FileUtils.deleteDirectory(dirToRemove.toFile()); // Requires commons-io dependency
                    logger.debug("{}Removed extraction directory: '{}'", logPrefix, dirToRemove.getFileName());
                } catch (IOException e) {
                    logger.warn("{}Could not remove extraction directory '{}': {}", logPrefix, dirToRemove.getFileName(), e.getMessage(), e);
                } catch (IllegalArgumentException e) {
                    logger.error("{}Path '{}' is not a directory, cannot remove recursively (FileUtils error): {}", logPrefix, dirToRemove.getFileName(), e.getMessage());
                } catch (SecurityException e) {
                    logger.warn("{}Permission error removing directory '{}': {}", logPrefix, dirToRemove.getFileName(), e.getMessage(), e);
                }
            } else {
                logger.error("{}SAFETY PREVENTED DELETE: Attempted to remove directory '{}' which is not strictly within the staging area root '{}'.",
                        logPrefix, dirToRemoveAbsolute, stagingRoot);
            }
        } else {
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

        if (!Files.isReadable(sourceZipPath)) {
            logger.warn("{}Source file '{}' no longer exists or is not readable. Cannot move to processed.", logPrefix, sourceZipPath);
            return;
        }

        Path targetProcessedDirAbsolute = targetProcessedDir.toAbsolutePath();
        logger.info("{}Moving original file '{}' to processed directory: {}", logPrefix, sourceZipPath.getFileName(), targetProcessedDirAbsolute);

        try {
            Files.createDirectories(targetProcessedDirAbsolute);

            String baseName = getBaseName(originalName);
            String extension = getExtension(originalName);
            Path destinationPath = targetProcessedDirAbsolute.resolve(originalName);

            // Handle potential naming conflicts
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
                    logger.debug("{}Attempting standard move: '{}' -> '{}'", logPrefix, sourceZipPath, destinationPath);
                    Files.move(sourceZipPath, destinationPath);
                    logger.info("{}Original file successfully moved (non-atomically) to '{}'.", logPrefix, destinationPath);
                } catch (IOException moveEx) {
                    logger.error("{}Error moving file (non-atomic fallback) to processed directory: {}. File remains in source directory.", logPrefix, moveEx.getMessage(), moveEx);
                    throw moveEx;
                }
            } catch (IOException moveEx) {
                logger.error("{}Error moving file (atomic attempt) to processed directory: {}. File remains in source directory.", logPrefix, moveEx.getMessage(), moveEx);
                throw moveEx;
            }
        } catch (IOException e) {
            logger.error("{}Error preparing move to processed directory '{}': {}. File remains in source directory.", logPrefix, targetProcessedDirAbsolute, e.getMessage(), e);
            throw e;
        } catch (SecurityException e) {
            logger.error("{}Permission error during move to processed directory '{}': {}. File remains in source directory.", logPrefix, targetProcessedDirAbsolute, e.getMessage(), e);
            throw new IOException("Permission error moving file to processed directory: " + e.getMessage(), e);
        }
    }

    // --- Getters ---
    public String getOriginalName() { return originalName; }
    public Path getExtractSubdir() { return extractSubdir; }
    public String getLogPrefix() { return logPrefix; }

    // --- Helper Methods for Filename Splitting (Unchanged) ---

    private static String getBaseName(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex <= 0) {
            return filename;
        } else {
            return filename.substring(0, dotIndex);
        }
    }

    private static String getExtension(String filename) {
        if (filename == null) return "";
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex <= 0 || dotIndex == filename.length() - 1) {
            return "";
        } else {
            return filename.substring(dotIndex + 1).toLowerCase();
        }
    }
}