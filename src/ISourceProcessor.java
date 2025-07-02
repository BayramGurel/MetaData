import java.nio.file.Path;
import java.util.List;

/**
 * Processes a single source path. Implementations may handle regular files or
 * container formats (for example ZIP archives).
 */
interface ISourceProcessor {
    /**
     * @param sourcePath    the actual file on disk that is being processed
     * @param containerPath a human readable identifier for the current entry
     * @param results       list to add successfully extracted resources to
     * @param errors        list to add encountered processing errors to
     * @param ignored       list to add skipped entries to
     */
    void processSource(Path sourcePath,
                       String containerPath,
                       List<CkanResource> results,
                       List<ProcessingError> errors,
                       List<IgnoredEntry> ignored);
}
