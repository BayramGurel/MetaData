import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Processes a single (non-archive) file.
 * Extends {@link AbstractSourceProcessor}.
 */
public class SingleFileProcessor extends AbstractSourceProcessor {

    /** Constructor, calls super to inject dependencies. */
    public SingleFileProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /** Processes one single file. */
    @Override
    public void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String sourceIdentifier = sourcePath.toString(); // Use full path as identifier
        String filename = sourcePath.getFileName().toString();

        // Check if the file type is relevant according to the filter
        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(sourceIdentifier, "Irrelevant file type or name."));
            return; // Skip processing this file
        }

        // Try opening and processing the file
        // Use try-with-resources for automatic stream closing
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {

            // Extract metadata and text using the provider
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());

            // Format the extracted data into a CkanResource
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), sourceIdentifier);

            // Add the result to the list of successful resources
            results.add(resource);

        } catch (IOException e) {
            // Handle errors during file reading
            errors.add(new ProcessingError(sourceIdentifier, "I/O Error reading file: " + e.getMessage()));
            System.err.println("ERROR: I/O problem reading file '" + sourceIdentifier + "': " + e.getMessage());
        } catch (Exception e) {
            // Catch other potential errors (from Tika, Formatter, etc.)
            errors.add(new ProcessingError(sourceIdentifier, "Error processing file: " + e.getClass().getSimpleName() + " - " + e.getMessage()));
            System.err.println("ERROR: Problem processing file '" + sourceIdentifier + "': " + e.getMessage());
            // Optionally print stack trace for more details: e.printStackTrace(System.err);
        }
    }
}
