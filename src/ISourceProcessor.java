import java.nio.file.Path;
import java.util.List;

interface ISourceProcessor {
    void processSource(Path var1, String var2, List<CkanResource> var3, List<ProcessingError> var4, List<IgnoredEntry> var5);
}
