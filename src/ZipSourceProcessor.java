import org.apache.tika.metadata.Metadata; // Nodig voor IMetadataProvider.ExtractionOutput

import java.io.*; // Nodig voor FilterInputStream, IOException, InputStream
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException; // Nodig voor buitenste catch
import java.util.zip.ZipInputStream;

// --- Hulpklasse ---

/**
 * InputStream wrapper die close() negeert. Essentieel voor ZipInputStream entries.
 */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }
    @Override public void close() throws IOException { /* Negeer sluiten */ }
    @Override public synchronized void mark(int readlimit) { in.mark(readlimit); }
    @Override public synchronized void reset() throws IOException { in.reset(); }
    @Override public boolean markSupported() { return in.markSupported(); }
}

// --- Zip Processor Klasse ---

/**
 * Verwerkt ZIP-archieven, inclusief geneste.
 * Erft van {@link AbstractSourceProcessor}.
 */
public class ZipSourceProcessor extends AbstractSourceProcessor {

    /** Constructor, roept super aan om dependencies te injecteren. */
    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /** Verwerkt een ZIP-archief. */
    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Gebruik try-with-resources voor automatisch sluiten streams
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            // Itereer door elke entry in de ZIP
            // getNextEntry() kan hier ZipException of IOException gooien
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName; // Pad voor rapportage

                try { // Try-block voor verwerking van één entry
                    // --- Basis checks ---
                    if (isInvalidPath(entryName)) {
                        errors.add(new ProcessingError(fullEntryPath, "Onveilig pad gedetecteerd."));
                        continue; // Sla potentieel gevaarlijke entry over
                    }
                    if (entry.isDirectory()) {
                        continue; // Sla mappen zelf over
                    }
                    if (!fileFilter.isFileTypeRelevant(entryName)) {
                        ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant bestandstype/naam."));
                        continue; // Sla irrelevante bestanden over
                    }

                    // --- Verwerk entry (geneste ZIP of regulier bestand) ---
                    if (isZipEntry(entry)) {
                        processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                    } else {
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(
                                        results::add, // Voeg resource toe bij succes
                                        () -> errors.add(new ProcessingError(fullEntryPath, "Kon entry niet verwerken.")) // Voeg fout toe bij falen
                                );
                    }
                } catch (Exception e) {
                    // Vang andere onverwachte fouten per entry op
                    errors.add(new ProcessingError(fullEntryPath, "Onverwachte entry fout: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
                    System.err.println("FOUT: Onverwacht bij verwerken entry '" + fullEntryPath + "': " + e.getMessage());
                } finally {
                    // BELANGRIJK: Sluit altijd de huidige entry om door te gaan naar de volgende
                    // closeEntry() kan ook ZipException of IOException gooien
                    try { zis.closeEntry(); } catch (IOException ioe) {
                        System.err.println("WARN: Kon ZIP entry niet correct sluiten: " + fullEntryPath + " - " + ioe.getMessage());
                        // Optioneel: voeg toe aan errors indien kritiek
                        // errors.add(new ProcessingError(fullEntryPath, "Fout bij sluiten entry: " + ioe.getMessage()));
                    }
                }
            } // Einde while loop over entries

        } catch (ZipException ze) {
            // Deze catch is nodig voor fouten bij openen/lezen hoofd ZIP structuur
            errors.add(new ProcessingError(containerPath, "Fout openen/lezen ZIP (mogelijk corrupt): " + ze.getMessage()));
            System.err.println("FOUT: Kan ZIP bestand niet lezen '" + containerPath + "': " + ze.getMessage());
        } catch (IOException e) {
            // Algemene I/O fout bij lezen hoofd ZIP
            errors.add(new ProcessingError(containerPath, "I/O Fout bij lezen ZIP: " + e.getMessage()));
            System.err.println("FOUT: I/O probleem lezen ZIP '" + containerPath + "': " + e.getMessage());
        } catch (Exception e) {
            // Vang andere onverwachte kritieke fouten op topniveau
            errors.add(new ProcessingError(containerPath, "Onverwachte kritieke ZIP fout: " + e.getMessage()));
            System.err.println("KRITIEKE FOUT verwerken ZIP '" + containerPath + "': " + e.getMessage());
            e.printStackTrace(System.err); // Print stack trace voor debuggen
        }
    }

    /** Verwerkt geneste ZIP: extract naar temp, roep processSource recursief aan, ruim temp op. */
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            // Maak tijdelijk bestand voor geneste ZIP
            tempZip = Files.createTempFile("nested_zip_", ".zip");

            // Kopieer inhoud naar temp bestand met NonClosingInputStream
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath);

            // Roep deze processor recursief aan voor het tijdelijke ZIP bestand
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);

        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Fout verwerken geneste ZIP: " + e.getMessage()));
            System.err.println("FOUT: Mislukt verwerken geneste ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            // Ruim altijd tijdelijk bestand op
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("WARN: Kon temp bestand niet verwijderen: " + tempZip);
                }
            }
        }
    }

    /** Verwerkt reguliere bestandsentry binnen ZIP. */
    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try {
            // Gebruik NonClosingInputStream om sluiten hoofdstream te voorkomen
            InputStream entryStream = new NonClosingInputStream(zis);
            // Roep metadata provider aan
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(entryStream, config.getMaxExtractedTextLength());
            // Roep formatter aan
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            // Retourneer succesvolle resource
            return Optional.of(resource);
        } catch (Exception e) {
            // Vang alle excepties op tijdens extractie/formattering voor deze entry
            System.err.println("FOUT: Verwerken entry '" + fullEntryPath + "' mislukt: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            // Geef falen aan met lege Optional
            return Optional.empty();
        }
    }

    /** Hulpmethode: controleert of ZipEntry zelf een ZIP is (o.b.v. extensie). */
    private boolean isZipEntry(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) {
            return false;
        }
        String nameLower = entry.getName().toLowerCase();
        // Check of naam eindigt op ondersteunde ZIP extensie uit config
        return config.getSupportedZipExtensions().stream().anyMatch(nameLower::endsWith);
    }
}
