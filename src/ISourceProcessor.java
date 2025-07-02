import java.nio.file.Path;
import java.util.List;

/**
 * Processes a source path such as a file or archive.
 */
interface ISourceProcessor {
    void processSource(Path sourcePath,
                       String containerPath,
                       List<CkanResource> results,
                       List<ProcessingError> errors,
                       List<IgnoredEntry> ignored);
}
