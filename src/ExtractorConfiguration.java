import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration for the extractor: text limits, ignored files/extensions, and formatting patterns.
 */
public final class ExtractorConfiguration {

    // --- Text length limits ---
    private static final int MAX_TEXT_SAMPLE_LENGTH     = 10_000;
    private static final int MAX_EXTRACTED_TEXT_LENGTH  = 5_242_880;  // ~5 MB
    private static final int MAX_AUTO_DESC_LENGTH       = 250;
    private static final int MIN_DESC_METADATA_LENGTH   = 10;
    private static final int MAX_DESC_METADATA_LENGTH   = 1_000;

    // --- File filtering rules ---
    private final Set<String> supportedZipExtensions = Set.of(
            ".zip"
    );

    private final Set<String> ignoredExtensions = Set.of(
            ".db", ".ds_store", "thumbs.db", ".tmp", ".temp", ".bak", ".lock", ".freelist", ".gdb", ".gdbindexes", ".gdbtable", ".gdbtablx", ".atx", ".spx", ".horizon", ".cdx", ".fpt", ".ini", ".cfg", ".config", ".log", ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".cpg", ".qpj", ".gpkg", ".gpkg-journal", ".geojson", ".topojson", ".mbtiles"
    );

    private final List<String> ignoredPrefixes = List.of(
            "~", "._"
    );

    private final List<String> ignoredFilenames = List.of(
            "gdb"
    );

    // --- Formatting helpers ---
    private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri;
    private final Pattern titlePrefixPattern;

    public ExtractorConfiguration() {
        this.iso8601Formatter   = DateTimeFormatter.ISO_INSTANT;
        this.placeholderUri     = "urn:placeholder:vervang-mij";
        this.titlePrefixPattern = Pattern.compile(
                "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
                Pattern.CASE_INSENSITIVE
        );
    }

    // --- Getters for limits ---

    public int getMaxTextSampleLength() {
        return MAX_TEXT_SAMPLE_LENGTH;
    }

    public int getMaxExtractedTextLength() {
        return MAX_EXTRACTED_TEXT_LENGTH;
    }

    public int getMaxAutoDescriptionLength() {
        return MAX_AUTO_DESC_LENGTH;
    }

    public int getMinDescMetadataLength() {
        return MIN_DESC_METADATA_LENGTH;
    }

    public int getMaxDescMetadataLength() {
        return MAX_DESC_METADATA_LENGTH;
    }

    // --- Getters for file filters ---

    /** ZIP file extensions we will recurse into. */
    public Set<String> getSupportedZipExtensions() {
        return supportedZipExtensions;
    }

    /** File extensions to skip entirely (case-insensitive). */
    public Set<String> getIgnoredExtensions() {
        return ignoredExtensions;
    }

    /** Filename prefixes to skip (e.g. temp files). */
    public List<String> getIgnoredPrefixes() {
        return ignoredPrefixes;
    }

    /** Exact filenames (without extension) to skip. */
    public List<String> getIgnoredFilenames() {
        return ignoredFilenames;
    }

    // --- Getters for formatting / placeholders ---

    public DateTimeFormatter getIso8601Formatter() {
        return iso8601Formatter;
    }

    public String getPlaceholderUri() {
        return placeholderUri;
    }

    public Pattern getTitlePrefixPattern() {
        return titlePrefixPattern;
    }
}
