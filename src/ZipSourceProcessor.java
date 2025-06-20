import java.io.*; // Using wildcard as in original for brevity
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

// Wrapper around an InputStream that prevents its close() method from closing the underlying stream.
// Essential for ZipInputStream when passing entry streams to consumers that might close them.
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }

    @Override public void close() throws IOException { /* No-op: Keep underlying stream open */ }

    // Delegate other relevant InputStream methods directly to the wrapped stream.
    @Override public synchronized void mark(int readlimit) { in.mark(readlimit); }
    @Override public synchronized void reset() throws IOException { in.reset(); }
    @Override public boolean markSupported() { return in.markSupported(); }
}

// Processor implementation for handling ZIP archives, including nested ZIPs and individual file entries.
public class ZipSourceProcessor extends AbstractSourceProcessor {

    // Initializes the ZIP processor with shared dependencies.
    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    // Processes a ZIP archive, iterating through its entries.
    // 'containerPath' is the identifier for the current ZIP being processed (can be nested).
    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Use try-with-resources for the main ZIP file streams.
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            // Loop through each entry within the current ZIP archive.
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                // Construct a full path identifier for the entry, including its container(s).
                String fullEntryPath = containerPath + "!/" + entryName;

                // Isolate processing for each entry to prevent one bad entry from halting everything.
                try {
                    if (isInvalidPath(entryName)) { // Check for unsafe path components.
                        errors.add(new ProcessingError(fullEntryPath, "Onveilig pad gedetecteerd."));
                        continue;
                    }
                    if (entry.isDirectory()) { // Skip directory entries.
                        continue;
                    }
                    if (!fileFilter.isFileTypeRelevant(entryName)) { // Skip irrelevant file types/names.
                        ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant bestandstype/naam."));
                        continue;
                    }

                    if (isZipEntry(entry)) { // If the entry is itself a ZIP file, process it recursively.
                        processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                    } else { // Otherwise, process it as a regular file entry.
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(
                                        results::add,
                                        () -> errors.add(new ProcessingError(fullEntryPath, "Kon entry niet verwerken."))
                                );
                    }
                } catch (Exception e) { // Catch errors specific to processing an individual entry.
                    errors.add(new ProcessingError(fullEntryPath, "Onverwachte entry fout: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
                    System.err.println("FOUT: Onverwacht bij verwerken entry '" + fullEntryPath + "': " + e.getMessage());
                } finally {
                    // Crucial: close the current entry to allow ZipInputStream to proceed to the next one.
                    try { zis.closeEntry(); } catch (IOException ioe) {
                        // Log if closing an entry fails, but don't let it stop overall processing.
                        System.err.println("WARN: Kon ZIP entry niet correct sluiten: " + fullEntryPath + " - " + ioe.getMessage());
                    }
                }
            }
        } catch (ZipException ze) { // Handle issues like a corrupt main ZIP file.
            errors.add(new ProcessingError(containerPath, "Fout openen/lezen ZIP (mogelijk corrupt): " + ze.getMessage()));
            System.err.println("FOUT: Kan ZIP bestand niet lezen '" + containerPath + "': " + ze.getMessage());
        } catch (IOException e) { // Handle other I/O errors with the main ZIP file.
            errors.add(new ProcessingError(containerPath, "I/O Fout bij lezen ZIP: " + e.getMessage()));
            System.err.println("FOUT: I/O probleem lezen ZIP '" + containerPath + "': " + e.getMessage());
        } catch (Exception e) { // Catch-all for any other critical, unexpected errors during main ZIP processing.
            errors.add(new ProcessingError(containerPath, "Onverwachte kritieke ZIP fout: " + e.getMessage()));
            System.err.println("KRITIEKE FOUT verwerken ZIP '" + containerPath + "': " + e.getMessage());
            e.printStackTrace(System.err); // Provide full stack trace for critical errors.
        }
    }

    // Extracts a nested ZIP entry to a temporary file and then processes it recursively.
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            // Create a temporary file to hold the content of the nested ZIP.
            tempZip = Files.createTempFile("nested_zip_", ".zip");

            // Copy nested ZIP entry's content to the temporary file.
            // NonClosingInputStream ensures that reading this entry does not close the parent ZipInputStream (zis).
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath);

            // Recursively call processSource for the newly created temporary ZIP file.
            // The 'fullEntryPath' of the nested ZIP becomes the 'containerPath' for its contents.
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);

        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Fout verwerken geneste ZIP: " + e.getMessage()));
            System.err.println("FOUT: Mislukt verwerken geneste ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            // Clean up: delete the temporary file after processing.
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("WARN: Kon temp bestand niet verwijderen: " + tempZip + " - " + e.getMessage());
                }
            }
        }
    }

    // Processes a regular (non-ZIP) file entry from within the ZIP archive.
    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try {
            // Wrap the entry's stream with NonClosingInputStream before passing to metadata provider.
            // This prevents the metadata provider (e.g., Tika) from closing the entire ZipInputStream.
            InputStream entryStream = new NonClosingInputStream(zis);
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(entryStream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            return Optional.of(resource);
        } catch (Exception e) {
            // If processing an individual entry fails, log error and return empty.
            System.err.println("FOUT: Verwerken entry '" + fullEntryPath + "' mislukt: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return Optional.empty();
        }
    }

    // Determines if a ZipEntry represents a ZIP file itself, based on its name and configured extensions.
    private boolean isZipEntry(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) {
            return false;
        }
        String nameLower = entry.getName().toLowerCase();
        // Check if the entry name ends with any of the configured ZIP extensions.
        return config.getSupportedZipExtensions().stream().anyMatch(nameLower::endsWith);
    }
}
