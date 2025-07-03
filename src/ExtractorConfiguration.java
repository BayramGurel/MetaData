import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration for extraction limits, file‐filtering rules, and formatting helpers.
 */
public final class ExtractorConfiguration {

    // --- Text length limits ---
    private static final int MAX_TEXT_SAMPLE_LENGTH     = 10_000;
    private static final int MAX_EXTRACTED_TEXT_LENGTH  = 5_242_880; // ~5 MB
    private static final int MAX_AUTO_DESC_LENGTH       = 250;
    private static final int MIN_DESC_METADATA_LENGTH   = 10;
    private static final int MAX_DESC_METADATA_LENGTH   = 1_000;

    // --- File‐filtering rules ---
    /** ZIP extensions that will be recursed into. */
    private final Set<String> supportedZipExtensions = Set.of(
            ".zip"
    );

    /** Extensions to skip entirely (case‐insensitive). */
    private final Set<String> ignoredExtensions = Set.of(
            ".db", ".ds_store", "thumbs.db",
            ".tmp", ".temp", ".bak", ".lock",
            ".freelist", ".gdb", ".gdbindexes",
            ".gdbtable", ".gdbtablx", ".atx",
            ".spx", ".horizon", ".cdx", ".fpt",
            ".ini", ".cfg", ".config", ".log",
            ".shp", ".shx", ".dbf", ".prj",
            ".sbn", ".sbx", ".cpg", ".qpj",
            ".gpkg", ".gpkg-journal",
            ".geojson", ".topojson", ".mbtiles"
    );

    /** Filename prefixes to skip (e.g. temp files). */
    private final List<String> ignoredPrefixes = List.of(
            "~", "._"
    );

    /** Exact filenames (without extension) to skip. */
    private final List<String> ignoredFilenames = List.of(
            "gdb"
    );

    // --- Formatting helpers ---
    private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri;
    private final Pattern titlePrefixPattern;

    /**
     * Initializes with the default limits, filters, and format rules.
     */
    public ExtractorConfiguration() {
        // ISO‐8601 instant formatter
        this.iso8601Formatter = Objects.requireNonNull(
                DateTimeFormatter.ISO_INSTANT,
                "ISO‐8601 DateTimeFormatter must not be null"
        );

        // A placeholder URI for resources without a real URI yet
        this.placeholderUri = Objects.requireNonNull(
                "urn:placeholder:replace-me",
                "Placeholder URI must not be null"
        );

        // Strip common “Microsoft Word – ”, etc., prefixes from titles
        this.titlePrefixPattern = Objects.requireNonNull(
                Pattern.compile(
                        "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
                        Pattern.CASE_INSENSITIVE
                ),
                "Title‐prefix stripping Pattern must not be null"
        );
    }

    // --- Getters for text‐length limits ---

    /** Max characters to sample for language detection or snippet generation. */
    public int getMaxTextSampleLength() {
        return MAX_TEXT_SAMPLE_LENGTH;
    }

    /** Max total characters to extract from a document (~5 MB). */
    public int getMaxExtractedTextLength() {
        return MAX_EXTRACTED_TEXT_LENGTH;
    }

    /** Max length of automatically generated descriptions. */
    public int getMaxAutoDescriptionLength() {
        return MAX_AUTO_DESC_LENGTH;
    }

    /** Min length of metadata descriptions to consider “valid.” */
    public int getMinDescMetadataLength() {
        return MIN_DESC_METADATA_LENGTH;
    }

    /** Max length of metadata descriptions to consider “valid.” */
    public int getMaxDescMetadataLength() {
        return MAX_DESC_METADATA_LENGTH;
    }

    // --- Getters for file‐filtering rules ---

    /** ZIP extensions that will be recursed into. */
    public Set<String> getSupportedZipExtensions() {
        return supportedZipExtensions;
    }

    /** File extensions to skip entirely (case‐insensitive). */
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

    /** Formatter for ISO‐8601 timestamps. */
    public DateTimeFormatter getIso8601Formatter() {
        return iso8601Formatter;
    }

    /** Placeholder URI used when no real URI is available. */
    public String getPlaceholderUri() {
        return placeholderUri;
    }

    /** Pattern to strip common title prefixes (e.g. “Microsoft Word - ”). */
    public Pattern getTitlePrefixPattern() {
        return titlePrefixPattern;
    }
}
