import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Verwerkt een enkel (niet-archief) bestand.
 * Erft van {@link AbstractSourceProcessor}.
 */
public class SingleFileProcessor extends AbstractSourceProcessor {

    /** Constructor, roept super aan om dependencies te injecteren. */
    public SingleFileProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /** Verwerkt één enkel bestand. */
    @Override
    public void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String sourceIdentifier = sourcePath.toString(); // Gebruik volledig pad als identifier
        String filename = sourcePath.getFileName().toString();

        // Controleer of bestandstype relevant is volgens filter
        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(sourceIdentifier, "Irrelevant bestandstype of naam."));
            return; // Sla verwerking over
        }

        // Probeer bestand te openen en verwerken (try-with-resources sluit stream)
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {

            // Extraheer metadata en tekst
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());

            // Formatteer data naar CkanResource
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), sourceIdentifier);

            // Voeg toe aan succesvolle resultaten
            results.add(resource);

        } catch (IOException e) {
            // Handel fouten af tijdens lezen bestand
            errors.add(new ProcessingError(sourceIdentifier, "I/O Fout bij lezen bestand: " + e.getMessage()));
            System.err.println("FOUT: I/O Probleem bestand '" + sourceIdentifier + "': " + e.getMessage());
        } catch (Exception e) {
            // Vang andere mogelijke fouten op (Tika, Formatter, etc.)
            errors.add(new ProcessingError(sourceIdentifier, "Fout bij verwerken bestand: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("FOUT: Probleem verwerken bestand '" + sourceIdentifier + "': " + e.getMessage());
        }
    }
}
