// src/ckan/ZipProcessor.java
package ckan;

// Imports remain the same as in the previous detailed step for ZipProcessor
import ckan.LoggingUtil;
import org.apache.commons.io.FileUtils; // Requires commons-io dependency
import org.slf4j.Logger; // Use SLF4J logger
import com.google.common.io.Files as GuavaFiles; // Requires Guava (for name splitting)

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
        import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime; // Import FileTime
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


public class ZipProcessor {

    private static final Logger logger = LoggingUtil.getLogger(ZipProcessor.class);

    private final Path sourceZipPath;
    private final ConfigLoader config;
    private final String originalName;
    private final String logPrefix; // For logging context [zipfilename]

    private Path stagedZipPath; // Path where the zip is copied in staging
    private Path extractSubdir; // Directory where contents are extracted


    public ZipProcessor(Path sourceZipPath, ConfigLoader config) {
        // Constructor logic remains the same...
        this.sourceZipPath = sourceZipPath;
        this.config = config;
        this.originalName = sourceZipPath.getFileName().toString();
        this.logPrefix = "[" + this.originalName + "] ";

        // Define unique staging paths using timestamp
        long timestamp = Instant.now().toEpochMilli(); // Use milliseconds for higher uniqueness
        // Use Guava Files.getNameWithoutExtension or implement manually
        String baseName = GuavaFiles.getNameWithoutExtension(originalName);
        String extension = GuavaFiles.getFileExtension(originalName);
        String uniqueSuffix = baseName + "_" + timestamp;

        // Ensure staging dir exists before resolving paths into it
        try {
            Files.createDirectories(config.getStagingDir());
        } catch (IOException e) {
            // This is critical - cannot proceed without staging dir
            logger.error("{}FATAL: Could not create staging directory: {}", logPrefix, config.getStagingDir(), e);
            throw new RuntimeException("Failed to create staging directory " + config.getStagingDir(), e);
        }

        this.stagedZipPath = config.getStagingDir().resolve(uniqueSuffix + "." + extension);
        this.extractSubdir = config.getStagingDir().resolve("extracted_" + uniqueSuffix);
        logger.debug("{}Initialized ZipProcessor. Staged path: {}, Extract dir: {}", logPrefix, stagedZipPath, extractSubdir);
    }

    public Path stage() throws IOException {
        // Staging logic remains the same...
        logger.info("{}Copying to staging area...", logPrefix);
        try {
            Files.copy(sourceZipPath, stagedZipPath, StandardCopyOption.REPLACE_EXISTING);
            // Try to copy basic file attributes like last modified time
            try {
                FileTime lastModifiedTime = Files.getLastModifiedTime(sourceZipPath);
                Files.setLastModifiedTime(stagedZipPath, lastModifiedTime);
            } catch (IOException | SecurityException attrEx) {
                logger.warn("{}Could not copy file attributes: {}", logPrefix, attrEx.getMessage());
            }
            logger.info("{}Copied to '{}'", logPrefix, config.getStagingDir().relativize(stagedZipPath));
            return stagedZipPath;
        } catch (IOException e) {
            logger.error("{}Error copying to staging: {}", logPrefix, e.getMessage(), e);
            // Clean up partially copied file if it exists
            Files.deleteIfExists(stagedZipPath);
            throw new IOException(logPrefix + "Error copying to staging: " + e.getMessage(), e);
        }
    }

    // Returns list of files selected for upload based on config
    public List<Path> extract() throws IOException {
        // Extraction logic remains the same...
        if (stagedZipPath == null || !Files.isReadable(stagedZipPath)) {
            throw new FileNotFoundException(logPrefix + "Staged ZIP file not found or not readable for extraction: " + (stagedZipPath != null ? stagedZipPath : "null"));
        }

        logger.info("{}Starting extraction to '{}'", logPrefix, config.getStagingDir().relativize(extractSubdir));
        List<Path> extractedFilesAll = new ArrayList<>();
        List<Path> filesToUpload = new ArrayList<>();

        try {
            Files.createDirectories(extractSubdir); // Ensure extraction target exists

            // Use ZipFile for better handling (random access, entry list)
            try (ZipFile zipFile = new ZipFile(stagedZipPath.toFile())) { // Use java.io.File constructor
                var entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    Path entryDestination = extractSubdir.resolve(entry.getName()).normalize(); // Normalize to handle relative paths safely

                    // **Security:** Prevent path traversal attacks (e.g., entries like ../../etc/passwd)
                    if (!entryDestination.startsWith(extractSubdir)) {
                        logger.error("{}SECURITY RISK: ZIP entry attempted path traversal: '{}'. Skipping entry.", logPrefix, entry.getName());
                        continue; // Skip this potentially malicious entry
                    }

                    if (entry.isDirectory()) {
                        Files.createDirectories(entryDestination);
                    } else {
                        // Ensure parent directory exists for the file
                        Files.createDirectories(entryDestination.getParent());
                        // Extract the file content
                        try (InputStream in = zipFile.getInputStream(entry);
                             OutputStream out = Files.newOutputStream(entryDestination)) { // Default is CREATE, TRUNCATE_EXISTING
                            in.transferTo(out); // Efficient stream copying (Java 9+)
                        }
                        extractedFilesAll.add(entryDestination);
                    }
                }
            } // ZipFile closed automatically

            logger.info("{}Extraction complete. {} total item(s) extracted.", logPrefix, extractedFilesAll.size());

            // Filter extracted files based on configured relevant extensions
            final List<String> relevantExtensions = config.getRelevantExtensions(); // Lowercase list from config
            if (!relevantExtensions.isEmpty()) {
                filesToUpload = extractedFilesAll.stream()
                        .filter(Files::isRegularFile) // Only consider actual files
                        .filter(f -> {
                            String fileName = f.getFileName().toString().toLowerCase();
                            // Check if file name ends with any of the relevant extensions
                            return relevantExtensions.stream().anyMatch(fileName::endsWith);
                        })
                        .collect(Collectors.toList());
                logger.info("{} {} file(s) selected for upload based on extensions: {}", logPrefix, filesToUpload.size(), relevantExtensions);
            } else {
                // If no extensions are specified, consider all extracted files
                filesToUpload.addAll(extractedFilesAll.stream().filter(Files::isRegularFile).collect(Collectors.toList()));
                logger.info("{}All {} extracted file(s) will be considered for upload (no extension filter specified).", logPrefix, filesToUpload.size());
            }

            // Handle nested ZIPs exclusion (extraction of nested ZIPs is NOT implemented here)
            if (!config.isExtractNestedZips()) {
                int originalCount = filesToUpload.size();
                filesToUpload.removeIf(f -> f.getFileName().toString().toLowerCase().endsWith(".zip"));
                int removedCount = originalCount - filesToUpload.size();
                if (removedCount > 0) {
                    logger.debug("{} {} nested .zip file(s) excluded from upload list as 'extract_nested_zips' is false.", logPrefix, removedCount);
                }
            } else {
                // Check if any .zip files remain in the upload list and warn if nested extraction is not implemented
                boolean nestedZipsPresent = filesToUpload.stream()
                        .anyMatch(f -> f.getFileName().toString().toLowerCase().endsWith(".zip"));
                if (nestedZipsPresent) {
                    logger.warn("{}Extraction of nested ZIPs is enabled in config but NOT IMPLEMENTED. Nested ZIPs will be uploaded as files.", logPrefix);
                    // TODO: Implement recursive extraction if needed
                }
            }

            return Collections.unmodifiableList(filesToUpload); // Return immutable list

        } catch (ZipException e) {
            logger.error("{}Corrupt or invalid ZIP file encountered: {}", logPrefix, e.getMessage());
            safeRemoveDir(extractSubdir); // Clean up potentially partial extraction
            throw new IOException(logPrefix + "Corrupt or invalid ZIP file: " + originalName, e); // Wrap in IOException
        } catch (IOException e) {
            logger.error("{}Error during extraction process: {}", logPrefix, e.getMessage(), e);
            safeRemoveDir(extractSubdir); // Clean up
            throw new IOException(logPrefix + "Extraction failed: " + e.getMessage(), e); // Re-throw as IOException
        }
    }

    public void cleanupStaging() {
        // Cleanup logic remains the same...
        logger.info("{}Cleaning up staging area...", logPrefix);

        // Delete staged ZIP file
        if (stagedZipPath != null && Files.exists(stagedZipPath)) {
            try {
                Files.delete(stagedZipPath);
                logger.debug("{}Deleted staged ZIP: '{}'", logPrefix, stagedZipPath.getFileName());
            } catch (IOException e) {
                logger.warn("{}Could not delete staged ZIP '{}': {}", logPrefix, stagedZipPath.getFileName(), e.getMessage(), e);
            }
        } else {
            logger.debug("{}No staged ZIP file found to delete at {}", logPrefix, stagedZipPath);
        }

        // Delete extraction directory using the helper method
        safeRemoveDir(extractSubdir);
    }

    // Safely remove a directory, ensuring it's within the staging area
    private void safeRemoveDir(Path dirToRemove) {
        // safeRemoveDir logic remains the same...
        if (dirToRemove == null || !Files.exists(dirToRemove)) {
            logger.debug("{}Extraction directory '{}' not found, nothing to remove.", logPrefix, dirToRemove != null ? dirToRemove.getFileName() : "null");
            return;
        }

        if (Files.isDirectory(dirToRemove)) {
            // **Security/Safety Check:** Ensure the directory is inside the staging directory before deleting
            if (dirToRemove.toAbsolutePath().normalize().startsWith(config.getStagingDir().toAbsolutePath().normalize())) {
                try {
                    // Use Apache Commons IO for robust recursive delete
                    FileUtils.deleteDirectory(dirToRemove.toFile());
                    logger.debug("{}Removed extraction directory: '{}'", logPrefix, dirToRemove.getFileName());
                } catch (IOException e) {
                    logger.warn("{}Could not remove extraction directory '{}': {}", logPrefix, dirToRemove.getFileName(), e.getMessage(), e);
                } catch (IllegalArgumentException e) {
                    // FileUtils.deleteDirectory throws this if path is not a directory
                    logger.warn("{}Path '{}' is not a directory, cannot remove recursively: {}", logPrefix, dirToRemove.getFileName(), e.getMessage());
                }
            } else {
                // This should ideally not happen if paths are constructed correctly, but safety first
                logger.error("{}SAFETY PREVENTED DELETE: Attempted to remove directory outside staging area: '{}'. Staging root: '{}'",
                        logPrefix, dirToRemove, config.getStagingDir());
            }
        } else {
            // Path exists but is not a directory
            logger.warn("{}Path '{}' exists but is not a directory, cannot remove as directory.", logPrefix, dirToRemove.getFileName());
        }
    }


    public void moveToProcessed() throws IOException { // Declare IOException for clarity
        // moveToProcessed logic remains the same...
        Path targetProcessedDir = config.getProcessedDir();

        if (!config.isMoveProcessed() || targetProcessedDir == null) {
            logger.debug("{}Skipping move to processed directory (disabled or not configured).", logPrefix);
            return;
        }

        // Check if source file still exists (it might have been deleted manually or failed staging)
        if (!Files.exists(sourceZipPath)) {
            logger.warn("{}Source file '{}' no longer exists. Cannot move to processed.", logPrefix, sourceZipPath);
            return;
        }

        logger.info("{}Moving original file to processed directory: {}", logPrefix, targetProcessedDir);
        try {
            Files.createDirectories(targetProcessedDir); // Ensure target dir exists

            // Use Guava again for name splitting
            String baseName = GuavaFiles.getNameWithoutExtension(originalName);
            String extension = GuavaFiles.getFileExtension(originalName);
            Path destinationPath = targetProcessedDir.resolve(originalName);
            int counter = 1;

            // Handle potential naming conflicts by appending a counter
            // A timestamp might be better for uniqueness across runs, but counter is simpler
            while (Files.exists(destinationPath)) {
                // Append _1, _2 etc before the extension
                String newName = String.format("%s_%d.%s", baseName, counter++, extension);
                destinationPath = targetProcessedDir.resolve(newName);
                if (counter > 100) { // Safety break to prevent infinite loop
                    logger.error("{}Failed to find unique name in processed directory after {} attempts for base: {}", logPrefix, counter -1, baseName);
                    throw new IOException("Too many naming conflicts in processed directory: " + targetProcessedDir);
                }
            }

            if (!destinationPath.getFileName().toString().equalsIgnoreCase(originalName)) {
                logger.warn("{}File already existed in processed directory. Renaming original to '{}'.", logPrefix, destinationPath.getFileName());
            }

            // Attempt atomic move first, fallback if needed
            try {
                Files.move(sourceZipPath, destinationPath, StandardCopyOption.ATOMIC_MOVE);
                logger.info("{}Original file successfully moved atomically to '{}'.", logPrefix, destinationPath);
            } catch (AtomicMoveNotSupportedException e) {
                logger.warn("{}Atomic move not supported (likely cross-filesystem). Attempting standard move.", logPrefix);
                try {
                    // Non-atomic fallback: Ensure REPLACE_EXISTING is NOT used unless intended overwrite
                    Files.move(sourceZipPath, destinationPath);
                    logger.info("{}Original file successfully moved (non-atomically) to '{}'.", logPrefix, destinationPath);
                } catch (IOException moveEx) {
                    logger.error("{}Error moving file (non-atomic fallback) to processed directory: {}", logPrefix, moveEx.getMessage(), moveEx);
                    // File remains in source directory
                    throw moveEx; // Re-throw the exception to indicate move failure
                }
            }
        } catch (IOException e) {
            logger.error("{}Error moving file to processed directory: {}. File remains in source directory.", logPrefix, e.getMessage(), e);
            // File remains in source directory
            throw e; // Re-throw the exception
        }
    }

    // --- Getters ---
    public String getOriginalName() {
        return originalName;
    }
    public Path getExtractSubdir() {
        return extractSubdir;
    }
    public String getLogPrefix() {
        return logPrefix;
    }
}