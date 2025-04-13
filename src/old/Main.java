package old;

import org.apache.tika.exception.TikaException; // Behoud imports die nodig zijn
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Orkestreert het metadata-extractieproces voor een gegeven bron (bestand of ZIP-archief).
 * Gebruikt geïnjecteerde componenten (via interfaces) voor specifieke taken zoals
 * bestandsfiltering, metadata-extractie, formattering en bronverwerking.
 * Dit is de centrale "Facade" voor het extractieproces.
 */
public class MetadataExtractor {

    // Componenten worden via de constructor geïnjecteerd (Dependency Injection)
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ISourceProcessor sourceProcessor; // Voor ZIPs
    private final ExtractorConfiguration config;

    /**
     * Constructor voor old.MetadataExtractor.
     * Vereist alle componenten om het extractieproces uit te voeren.
     *
     * @param ff  De {@link IFileTypeFilter} implementatie.
     * @param mp  De {@link IMetadataProvider} implementatie.
     * @param rf  De {@link ICkanResourceFormatter} implementatie.
     * @param sp  De {@link ISourceProcessor} implementatie (bv. voor ZIPs).
     * @param cfg De {@link ExtractorConfiguration}.
     */
    public MetadataExtractor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ISourceProcessor sp, ExtractorConfiguration cfg) {
        // Gebruik Objects.requireNonNull om ervoor te zorgen dat geen null componenten worden doorgegeven
        this.fileFilter = Objects.requireNonNull(ff, "IFileTypeFilter mag niet null zijn");
        this.metadataProvider = Objects.requireNonNull(mp, "IMetadataProvider mag niet null zijn");
        this.resourceFormatter = Objects.requireNonNull(rf, "ICkanResourceFormatter mag niet null zijn");
        this.sourceProcessor = Objects.requireNonNull(sp, "ISourceProcessor mag niet null zijn");
        this.config = Objects.requireNonNull(cfg, "ExtractorConfiguration mag niet null zijn");
    }

    /**
     * Verwerkt een bronbestand. Dit kan een enkel bestand zijn of een ZIP-archief.
     * Delegeert de verwerking aan de juiste methode (processSingleFile of ISourceProcessor).
     *
     * @param sourcePathString Het pad naar het bronbestand als String.
     * @return Een {@link ProcessingReport} met de resultaten, fouten en genegeerde items.
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList<>();
        List<ProcessingError> errors = new ArrayList<>();
        List<IgnoredEntry> ignored = new ArrayList<>();

        Path sourcePath = null;
        try {
            // Probeer het pad te normaliseren en te valideren
            sourcePath = Paths.get(sourcePathString).normalize(); // Normaliseert bv. ../

            if (!Files.exists(sourcePath)) {
                errors.add(new ProcessingError(sourcePathString, "Bronbestand of -map niet gevonden."));
            } else if (Files.isDirectory(sourcePath)) {
                // Momenteel worden mappen genegeerd. Kan uitgebreid worden.
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is een map (directe mapverwerking niet ondersteund)."));
            } else if (isZipFile(sourcePath)) {
                // Delegeer ZIP-verwerking aan de gespecialiseerde SourceProcessor
                sourceProcessor.processSource(sourcePath, sourcePath.toString(), results, errors, ignored);
            } else {
                // Verwerk als een enkel, niet-ZIP bestand
                processSingleFile(sourcePath, results, errors, ignored);
            }
        } catch (InvalidPathException ipe) {
            errors.add(new ProcessingError(sourcePathString, "Ongeldig bestandspad opgegeven: " + ipe.getMessage()));
        } catch (SecurityException se) {
            errors.add(new ProcessingError(sourcePathString, "Geen leestoegang tot het bestand of de map: " + se.getMessage()));
        } catch (Exception e) {
            // Vang onverwachte fouten op tijdens de hoofdverwerking
            String pathInfo = (sourcePath != null) ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathInfo, "Onverwachte kritieke fout tijdens verwerking: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("KRITIEKE FOUT bij verwerken bron '" + pathInfo + "':");
            e.printStackTrace(System.err); // Log stacktrace voor debuggen
        }

        // Finaliseer en retourneer het rapport, ongeacht of er fouten waren
        return finishReport(results, errors, ignored);
    }

    /**
     * Verwerkt een enkel bestand (geen ZIP). Extraheert metadata en formatteert het.
     * Voegt resultaat, fout of genegeerd item toe aan de lijsten.
     *
     * @param sourcePath Het pad naar het enkele bestand.
     * @param results    Lijst om succesvolle CkanResource resultaten aan toe te voegen.
     * @param errors     Lijst om ProcessingError's aan toe te voegen.
     * @param ignored    Lijst om IgnoredEntry's aan toe te voegen.
     */
    private void processSingleFile(Path sourcePath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String source = sourcePath.toString();
        String filename = sourcePath.getFileName().toString(); // Kan null zijn bij root path? (onwaarschijnlijk)

        // 1. Controleer of bestandstype relevant is
        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(source, "Bestandstype is gemarkeerd als irrelevant door filter."));
            return;
        }

        // 2. Probeer metadata en tekst te extraheren (try-with-resources voor InputStream)
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
            // Roep de metadata provider aan
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());

            // 3. Formatteer de output naar een CkanResource
            // Gebruik record accessors: output.metadata() en output.text()
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), source);

            // 4. Voeg toe aan resultaten
            results.add(resource);

        } catch (IOException ioe) {
            errors.add(new ProcessingError(source, "IO Fout bij lezen bestand: " + ioe.getMessage()));
            System.err.println("FOUT (IO): Kon bestand '" + source + "' niet lezen: " + ioe.getMessage());
        } catch (TikaException | SAXException te) {
            errors.add(new ProcessingError(source, "Tika/Parser Fout: " + te.getMessage()));
            System.err.println("FOUT (Tika): Parseren van '" + source + "' mislukt: " + te.getMessage());
        } catch (OutOfMemoryError oom) {
            // Specifieke afhandeling voor OutOfMemoryError, kan gebeuren bij grote bestanden
            errors.add(new ProcessingError(source, "OutOfMemoryError: Bestand is mogelijk te groot of complex om te verwerken met huidig geheugen."));
            System.err.println("KRITIEKE FOUT (OOM): Onvoldoende geheugen bij verwerken '" + source + "'. Overweeg JVM heap size te verhogen (-Xmx).");
        }
        catch (Exception e) {
            // Vang andere onverwachte fouten tijdens extractie/formattering
            errors.add(new ProcessingError(source, "Onverwachte fout tijdens verwerken bestand: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("FOUT (Onverwacht): Verwerken van '" + source + "' mislukt:");
            e.printStackTrace(System.err);
        }
    }

    /**
     * Finaliseert het ProcessingReport en logt een samenvatting naar System.err.
     *
     * @param res De lijst met succesvolle resultaten.
     * @param err De lijst met fouten.
     * @param ign De lijst met genegeerde items.
     * @return Het samengestelde {@link ProcessingReport}.
     */
    private ProcessingReport finishReport(List<CkanResource> res, List<ProcessingError> err, List<IgnoredEntry> ign) {
        // Log samenvatting naar stderr (console output voor data, stderr voor logging/fouten)
        System.err.printf("--- Verwerkingssamenvatting ---%n");
        System.err.printf("Succesvolle resources: %d%n", res.size());
        System.err.printf("Genegeerde bestanden/entries: %d%n", ign.size());
        System.err.printf("Fouten opgetreden: %d%n", err.size());

        // Log details van fouten indien aanwezig
        if (!err.isEmpty()) {
            System.err.println("\nDetails van opgetreden fouten:");
            err.forEach(error -> System.err.printf("  - Bron: [%s]%n    Fout: %s%n", error.source(), error.error()));
        }
        // Optioneel: Log details van genegeerde items
        // if (!ign.isEmpty()) {
        //     System.err.println("\nDetails van genegeerde items:");
        //     ign.forEach(ignored -> System.err.printf("  - Bron: [%s]%n    Reden: %s%n", ignored.source(), ignored.reason()));
        // }
        System.err.printf("--- Einde Samenvatting ---%n");


        // Geef het onveranderlijke report object terug
        return new ProcessingReport(res, err, ign);
    }

    /**
     * Controleert of het gegeven pad verwijst naar een bestand dat lijkt op een ZIP-archief,
     * gebaseerd op de bestandsextensie uit de configuratie.
     *
     * @param p Het te controleren bestandspad.
     * @return true als het een regulier bestand is met een ondersteunde ZIP-extensie, anders false.
     */
    private boolean isZipFile(Path p) {
        // Controleer of p niet null is, een regulier bestand is (geen map/link etc.)
        // en de bestandsnaam (lowercase) eindigt op een van de ondersteunde extensies.
        return p != null &&
                Files.isRegularFile(p) &&
                config.getSupportedZipExtensions().stream()
                        .anyMatch(ext -> p.getFileName().toString().toLowerCase().endsWith(ext));
    }

    // --- Main Methode en helpers zijn verplaatst naar MainApplication ---

} // Einde old.MetadataExtractor