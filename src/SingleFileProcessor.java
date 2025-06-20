import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SingleFileProcessor extends AbstractSourceProcessor {
    public SingleFileProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    public void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String sourceIdentifier = sourcePath.toString();
        String filename = sourcePath.getFileName().toString();
        if (!this.fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(sourceIdentifier, "Irrelevant bestandstype of naam."));
        } else {
            try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
                IMetadataProvider.ExtractionOutput output = this.metadataProvider.extract(stream, this.config.getMaxExtractedTextLength());
                CkanResource resource = this.resourceFormatter.format(filename, output.metadata(), output.text(), sourceIdentifier);
                results.add(resource);
            } catch (IOException e) {
                errors.add(new ProcessingError(sourceIdentifier, "I/O Fout bij lezen bestand: " + e.getMessage()));
                System.err.println("FOUT: I/O Probleem bestand '" + sourceIdentifier + "': " + e.getMessage());
            } catch (Exception e) {
                String var10004 = e.getClass().getSimpleName();
                errors.add(new ProcessingError(sourceIdentifier, "Fout bij verwerken bestand: " + var10004 + " - " + e.getMessage()));
                System.err.println("FOUT: Probleem verwerken bestand '" + sourceIdentifier + "': " + e.getMessage());
            }

        }
    }
}
