import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SingleFileProcessor extends AbstractSourceProcessor {
    public SingleFileProcessor(IFileTypeFilter ff,
                               IMetadataProvider mp,
                               ICkanResourceFormatter rf,
                               ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    @Override
    public void processSource(Path sourcePath,
                              String containerPath,
                              List<CkanResource> results,
                              List<ProcessingError> errors,
                              List<IgnoredEntry> ignored) {
        String sourceIdentifier = sourcePath.toString();
        String filename = sourcePath.getFileName().toString();

        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(sourceIdentifier, "Irrelevant bestandstype of naam."));
            return;
        }

        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
            IMetadataProvider.ExtractionOutput output =
                    metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(
                    filename,
                    output.metadata(),
                    output.text(),
                    sourceIdentifier);
            results.add(resource);
        } catch (IOException e) {
            errors.add(new ProcessingError(sourceIdentifier,
                    "I/O Fout bij lezen bestand: " + e.getMessage()));
            System.err.println("FOUT: I/O Probleem bestand '" + sourceIdentifier + "': " + e.getMessage());
        } catch (Exception e) {
            errors.add(new ProcessingError(sourceIdentifier,
                    "Fout bij verwerken bestand: "
                            + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("FOUT: Probleem verwerken bestand '" + sourceIdentifier + "': " + e.getMessage());
        }
    }
}
