import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

// Processor implementation for handling single, standalone files.
public class SingleFileProcessor extends AbstractSourceProcessor {

    // Initializes the single file processor, passing dependencies to the base class.
    public SingleFileProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    // Processes a single source file.
    // 'containerPath' is part of the interface but typically same as 'sourcePath' here.
    @Override
    public void processSource(Path sourcePath, String containerPath,
                              List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String sourceIdentifier = sourcePath.toString();
        String filename = sourcePath.getFileName().toString();

        // First, check if the file is relevant based on configured filters.
        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(sourceIdentifier, "Irrelevant bestandstype of naam."));
            return; // Skip processing if not relevant.
        }

        // Use try-with-resources for automatic closing of the InputStream.
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
            // Extract metadata and text using the metadata provider.
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            // Format the extracted information into a CkanResource.
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), sourceIdentifier);
            results.add(resource); // Add successfully processed resource.

        } catch (IOException e) { // Handle I/O specific errors.
            errors.add(new ProcessingError(sourceIdentifier, "I/O Fout bij lezen bestand: " + e.getMessage()));
            System.err.println("FOUT: I/O Probleem bestand '" + sourceIdentifier + "': " + e.getMessage());
        } catch (Exception e) { // Handle other unexpected errors during processing.
            errors.add(new ProcessingError(sourceIdentifier, "Fout bij verwerken bestand: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("FOUT: Probleem verwerken bestand '" + sourceIdentifier + "': " + e.getMessage());
        }
    }
}