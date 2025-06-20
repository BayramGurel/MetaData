import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class AbstractSourceProcessor implements ISourceProcessor {
    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;
    private static final Pattern WINDOWS_DRIVE_PATTERN = Pattern.compile("^[a-zA-Z]:[/\\\\].*");

    protected AbstractSourceProcessor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ExtractorConfiguration config) {
        this.fileFilter = (IFileTypeFilter)Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = (IMetadataProvider)Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = (ICkanResourceFormatter)Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = (ExtractorConfiguration)Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    public abstract void processSource(Path var1, String var2, List<CkanResource> var3, List<ProcessingError> var4, List<IgnoredEntry> var5);

    protected static String getFilenameFromEntry(String entryName) {
        if (entryName != null && !entryName.isBlank()) {
            String normalizedName = entryName.trim().replace('\\', '/');
            int lastSlash = normalizedName.lastIndexOf(47);
            return lastSlash >= 0 ? normalizedName.substring(lastSlash + 1) : normalizedName;
        } else {
            return "";
        }
    }

    protected boolean isInvalidPath(String entryName) {
        if (entryName == null) {
            return false;
        } else {
            String trimmedName = entryName.trim();
            if (trimmedName.isEmpty()) {
                return false;
            } else if (trimmedName.contains("..")) {
                return true;
            } else {
                try {
                    Path path = Paths.get(trimmedName);
                    Path normalizedPath = path.normalize();
                    if (WINDOWS_DRIVE_PATTERN.matcher(normalizedPath.toString()).matches()) {
                        return true;
                    } else {
                        return normalizedPath.isAbsolute();
                    }
                } catch (InvalidPathException var5) {
                    System.err.println("Waarschuwing: Ongeldige pad syntax gedetecteerd in entry: " + trimmedName);
                    return true;
                }
            }
        }
    }
}
