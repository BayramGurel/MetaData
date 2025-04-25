import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration settings for the metadata extractor.
 * Holds hardcoded limits, filter lists, and formatters.
 */
public final class ExtractorConfiguration {

    // Limits
    private final int maxTextSampleLength;
    private final int maxExtractedTextLength;
    private final int maxAutoDescriptionLength;
    private final int minDescMetadataLength;
    private final int maxDescMetadataLength;

    // File Filtering
    private final Set<String> supportedZipExtensions;
    private final Set<String> ignoredExtensions;
    private final List<String> ignoredPrefixes;
    private final List<String> ignoredFilenames;

    // Formatting & Misc
    private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri;
    private final Pattern titlePrefixPattern;

    /** Initializes default configuration values. */
    public ExtractorConfiguration() {
        // Limits
        this.maxTextSampleLength = 10000;
        this.maxExtractedTextLength = 5 * 1024 * 1024; // 5MB
        this.maxAutoDescriptionLength = 250;
        this.minDescMetadataLength = 10;
        this.maxDescMetadataLength = 1000;

        // File Filtering
        this.supportedZipExtensions = Set.of(".zip");
        this.ignoredExtensions = Set.of( // Includes common temp/system/GIS files
                ".ds_store", "thumbs.db", ".tmp", ".temp", ".bak", ".lock",
                ".freelist", ".gdbindexes", ".gdbtable", ".gdbtablx", ".atx", ".spx",
                ".horizon", ".cdx", ".fpt", ".ini", ".cfg", ".config", ".log",
                ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".cpg", ".qpj",
                ".gpkg", ".gpkg-journal", ".geojson", ".topojson", ".mbtiles"
        );
        this.ignoredPrefixes = List.of("~", "._"); // Common temp/Mac prefixes
        this.ignoredFilenames = List.of("gdb");    // Common filename in ESRI GDB

        // Formatting & Misc
        this.iso8601Formatter = DateTimeFormatter.ISO_INSTANT; // Use UTC Instant format
        this.placeholderUri = "urn:placeholder:replace-me";    // Default placeholder URI
        this.titlePrefixPattern = Pattern.compile(             // Regex to clean titles
                "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
                Pattern.CASE_INSENSITIVE);
    }

    // --- Getters ---
    public int getMaxTextSampleLength() { return maxTextSampleLength; }
    public int getMaxExtractedTextLength() { return maxExtractedTextLength; }
    public int getMaxAutoDescriptionLength() { return maxAutoDescriptionLength; }
    public int getMinDescMetadataLength() { return minDescMetadataLength; }
    public int getMaxDescMetadataLength() { return maxDescMetadataLength; }
    public Set<String> getSupportedZipExtensions() { return supportedZipExtensions; }
    public Set<String> getIgnoredExtensions() { return ignoredExtensions; }
    public List<String> getIgnoredPrefixes() { return ignoredPrefixes; }
    public List<String> getIgnoredFilenames() { return ignoredFilenames; }
    public DateTimeFormatter getIso8601Formatter() { return iso8601Formatter; }
    public String getPlaceholderUri() { return placeholderUri; }
    public Pattern getTitlePrefixPattern() { return titlePrefixPattern; }
}
