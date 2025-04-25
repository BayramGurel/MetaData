import org.apache.tika.metadata.Metadata; // Required for IMetadataProvider.ExtractionOutput

import java.io.*; // Required for FilterInputStream, IOException, InputStream
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException; // Still needed for the outer catch
import java.util.zip.ZipInputStream;

// --- Utility Inner Class ---

/**
 * InputStream wrapper that ignores close(). Essential for ZipInputStream entries.
 */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }
    @Override public void close() throws IOException { /* Ignore close */ }
    @Override public synchronized void mark(int readlimit) { in.mark(readlimit); }
    @Override public synchronized void reset() throws IOException { in.reset(); }
    @Override public boolean markSupported() { return in.markSupported(); }
}

// --- Zip Processor Class ---

/**
 * Processes ZIP archives, including nested ones.
 * Extends {@link AbstractSourceProcessor}.
 */
public class ZipSourceProcessor extends AbstractSourceProcessor {

    /** Constructor, calls super to inject dependencies. */
    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /** Processes a ZIP archive. */
    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Use try-with-resources for automatic closing of streams
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            // Iterate through each entry in the ZIP
            // getNextEntry() can throw ZipException or IOException here
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName; // Path for reporting

                try { // Try block for processing a single entry
                    // --- Basic checks ---
                    if (isInvalidPath(entryName)) {
                        errors.add(new ProcessingError(fullEntryPath, "Unsafe path detected."));
                        continue; // Skip potentially harmful entries
                    }
                    if (entry.isDirectory()) {
                        continue; // Skip directories themselves
                    }
                    if (!fileFilter.isFileTypeRelevant(entryName)) {
                        ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant file type/name."));
                        continue; // Skip irrelevant files
                    }

                    // --- Process entry (nested ZIP or regular file) ---
                    if (isZipEntry(entry)) {
                        processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                    } else {
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(
                                        results::add, // Add resource on success
                                        () -> errors.add(new ProcessingError(fullEntryPath, "Could not process entry.")) // Add error on failure
                                );
                    }
                    // Removed the specific catch (ZipException ze) inside the loop as it's unlikely to be thrown here
                    // and causes a compiler warning. The general Exception catch below handles issues during entry processing.
                } catch (Exception e) {
                    // Catch other unexpected errors per entry
                    errors.add(new ProcessingError(fullEntryPath, "Unexpected entry error: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
                    System.err.println("ERROR: Unexpected error processing entry '" + fullEntryPath + "': " + e.getMessage());
                } finally {
                    // IMPORTANT: Always close the current entry to proceed to the next
                    // closeEntry() can also throw ZipException or IOException
                    try { zis.closeEntry(); } catch (IOException ioe) {
                        System.err.println("WARN: Could not properly close ZIP entry: " + fullEntryPath + " - " + ioe.getMessage());
                        // Optionally add to errors list if this is considered critical
                        // errors.add(new ProcessingError(fullEntryPath, "Error closing entry: " + ioe.getMessage()));
                    }
                }
            } // End while loop over entries

        } catch (ZipException ze) {
            // This catch block IS necessary for errors opening/reading the main ZIP structure
            errors.add(new ProcessingError(containerPath, "Error opening/reading ZIP (possibly corrupt): " + ze.getMessage()));
            System.err.println("ERROR: Cannot read ZIP file '" + containerPath + "': " + ze.getMessage());
        } catch (IOException e) {
            // General I/O error reading the main ZIP
            errors.add(new ProcessingError(containerPath, "I/O Error reading ZIP: " + e.getMessage()));
            System.err.println("ERROR: I/O problem reading ZIP '" + containerPath + "': " + e.getMessage());
        } catch (Exception e) {
            // Catch other unexpected critical errors at the top level
            errors.add(new ProcessingError(containerPath, "Unexpected critical ZIP error: " + e.getMessage()));
            System.err.println("CRITICAL ERROR processing ZIP '" + containerPath + "': " + e.getMessage());
            e.printStackTrace(System.err); // Print stack trace for debugging
        }
    }

    /** Processes a nested ZIP: extracts to temp, calls processSource recursively, cleans up temp file. */
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("INFO: Processing nested ZIP: " + fullEntryPath);
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Error processing nested ZIP: " + e.getMessage()));
            System.err.println("ERROR: Failed processing nested ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("WARN: Could not delete temp file: " + tempZip);
                }
            }
        }
    }

    /** Processes a regular file entry within the ZIP using base class dependencies. */
    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try {
            InputStream entryStream = new NonClosingInputStream(zis);
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(entryStream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            return Optional.of(resource);
        } catch (Exception e) {
            System.err.println("ERROR: Processing entry '" + fullEntryPath + "' failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return Optional.empty();
        }
    }

    /** Helper method to check if a ZipEntry is itself a ZIP file based on extension. */
    private boolean isZipEntry(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) {
            return false;
        }
        String nameLower = entry.getName().toLowerCase();
        return config.getSupportedZipExtensions().stream().anyMatch(nameLower::endsWith);
    }
}
