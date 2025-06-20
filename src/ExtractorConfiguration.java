import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class ExtractorConfiguration {
    private final int maxTextSampleLength = 10000;
    private final int maxExtractedTextLength = 5242880;
    private final int maxAutoDescriptionLength = 250;
    private final int minDescMetadataLength = 10;
    private final int maxDescMetadataLength = 1000;
    private final Set<String> supportedZipExtensions = Set.of(".zip");
    private final Set<String> ignoredExtensions = Set.of(".ds_store", "thumbs.db", ".tmp", ".temp", ".bak", ".lock", ".freelist", ".gdbindexes", ".gdbtable", ".gdbtablx", ".atx", ".spx", ".horizon", ".cdx", ".fpt", ".ini", ".cfg", ".config", ".log", ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".cpg", ".qpj", ".gpkg", ".gpkg-journal", ".geojson", ".topojson", ".mbtiles");
    private final List<String> ignoredPrefixes = List.of("~", "._");
    private final List<String> ignoredFilenames = List.of("gdb");
    private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri;
    private final Pattern titlePrefixPattern;

    public ExtractorConfiguration() {
        this.iso8601Formatter = DateTimeFormatter.ISO_INSTANT;
        this.placeholderUri = "urn:placeholder:vervang-mij";
        this.titlePrefixPattern = Pattern.compile("^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )", 2);
    }

    public int getMaxTextSampleLength() {
        return this.maxTextSampleLength;
    }

    public int getMaxExtractedTextLength() {
        return this.maxExtractedTextLength;
    }

    public int getMaxAutoDescriptionLength() {
        return this.maxAutoDescriptionLength;
    }

    public int getMinDescMetadataLength() {
        return this.minDescMetadataLength;
    }

    public int getMaxDescMetadataLength() {
        return this.maxDescMetadataLength;
    }

    public Set<String> getSupportedZipExtensions() {
        return this.supportedZipExtensions;
    }

    public Set<String> getIgnoredExtensions() {
        return this.ignoredExtensions;
    }

    public List<String> getIgnoredPrefixes() {
        return this.ignoredPrefixes;
    }

    public List<String> getIgnoredFilenames() {
        return this.ignoredFilenames;
    }

    public DateTimeFormatter getIso8601Formatter() {
        return this.iso8601Formatter;
    }

    public String getPlaceholderUri() {
        return this.placeholderUri;
    }

    public Pattern getTitlePrefixPattern() {
        return this.titlePrefixPattern;
    }
}
