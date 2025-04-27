import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }
    @Override public void close() throws IOException { /* No-op */ }
    @Override public synchronized void mark(int readlimit) { in.mark(readlimit); }
    @Override public synchronized void reset() throws IOException { in.reset(); }
    @Override public boolean markSupported() { return in.markSupported(); }
}

public class ZipSourceProcessor extends AbstractSourceProcessor {

    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName;

                try {
                    if (isInvalidPath(entryName)) {
                        errors.add(new ProcessingError(fullEntryPath, "Onveilig pad gedetecteerd."));
                        continue;
                    }
                    if (entry.isDirectory()) {
                        continue;
                    }
                    if (!fileFilter.isFileTypeRelevant(entryName)) {
                        ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant bestandstype/naam."));
                        continue;
                    }

                    if (isZipEntry(entry)) {
                        processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                    } else {
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(
                                        results::add,
                                        () -> errors.add(new ProcessingError(fullEntryPath, "Kon entry niet verwerken."))
                                );
                    }
                } catch (Exception e) {
                    errors.add(new ProcessingError(fullEntryPath, "Onverwachte entry fout: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
                    System.err.println("FOUT: Onverwacht bij verwerken entry '" + fullEntryPath + "': " + e.getMessage());
                } finally {
                    try { zis.closeEntry(); } catch (IOException ioe) {
                        System.err.println("WARN: Kon ZIP entry niet correct sluiten: " + fullEntryPath + " - " + ioe.getMessage());
                    }
                }
            }

        } catch (ZipException ze) {
            errors.add(new ProcessingError(containerPath, "Fout openen/lezen ZIP (mogelijk corrupt): " + ze.getMessage()));
            System.err.println("FOUT: Kan ZIP bestand niet lezen '" + containerPath + "': " + ze.getMessage());
        } catch (IOException e) {
            errors.add(new ProcessingError(containerPath, "I/O Fout bij lezen ZIP: " + e.getMessage()));
            System.err.println("FOUT: I/O probleem lezen ZIP '" + containerPath + "': " + e.getMessage());
        } catch (Exception e) {
            errors.add(new ProcessingError(containerPath, "Onverwachte kritieke ZIP fout: " + e.getMessage()));
            System.err.println("KRITIEKE FOUT verwerken ZIP '" + containerPath + "': " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");

            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath);

            this.processSource(tempZip, fullEntryPath, results, errors, ignored);

        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Fout verwerken geneste ZIP: " + e.getMessage()));
            System.err.println("FOUT: Mislukt verwerken geneste ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("WARN: Kon temp bestand niet verwijderen: " + tempZip);
                }
            }
        }
    }

    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try {
            InputStream entryStream = new NonClosingInputStream(zis);
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(entryStream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            return Optional.of(resource);
        } catch (Exception e) {
            System.err.println("FOUT: Verwerken entry '" + fullEntryPath + "' mislukt: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return Optional.empty();
        }
    }

    private boolean isZipEntry(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) {
            return false;
        }
        String nameLower = entry.getName().toLowerCase();
        return config.getSupportedZipExtensions().stream().anyMatch(nameLower::endsWith);
    }
}