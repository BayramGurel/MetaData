import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

// --- Data Transfer Objects / Result Classes ---

/**
 * Represents the successfully extracted and formatted metadata for a single resource.
 * Uses a Map internally for flexibility but provides a dedicated class structure.
 */
class CkanResource {
    private final Map<String, Object> data;

    public CkanResource(Map<String, Object> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data)); // Make immutable copy
    }

    public Map<String, Object> getData() {
        return data;
    }
    // Add getters for specific fields if needed
}

/**
 * Represents an error encountered during processing a specific source entry.
 */
class ProcessingError {
    private final String source;
    private final String error;

    public ProcessingError(String source, String error) {
        this.source = source;
        this.error = error;
    }

    public String getSource() { return source; }
    public String getError() { return error; }

    // Convert to Map for easy serialization if needed, though ObjectMapper handles POJOs
    public Map<String, String> toMap() {
        return Map.of("source", source, "error", error);
    }
}

/**
 * Represents a file/entry that was ignored based on filtering rules.
 */
class IgnoredEntry {
    private final String source;
    private final String reason;

    public IgnoredEntry(String source, String reason) {
        this.source = source;
        this.reason = reason;
    }

    public String getSource() { return source; }
    public String getReason() { return reason; }

    public Map<String, String> toMap() {
        return Map.of("source", source, "reason", reason);
    }
}

/**
 * Encapsulates the overall result of a processing operation.
 */
class ProcessingReport {
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Use unmodifiable lists to ensure the report itself is immutable after creation
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }

    public List<CkanResource> getResults() { return results; }
    public List<ProcessingError> getErrors() { return errors; }
    public List<IgnoredEntry> getIgnored() { return ignored; }
}


// --- Interfaces for Components ---

/**
 * Interface for determining if a file/entry is relevant for processing.
 */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/**
 * Interface for extracting metadata and text content from an input stream.
 */
interface IMetadataProvider {
    /** Represents combined metadata and text */
    class ExtractionOutput {
        final Metadata metadata;
        final String text;
        ExtractionOutput(Metadata m, String t) { this.metadata = m; this.text = t; }
    }
    ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException;
}

/**
 * Interface for formatting extracted metadata into a CKAN resource structure.
 */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/**
 * Interface for processing a source (file or archive).
 */
interface ISourceProcessor {
    void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) throws IOException;
}


// --- Default Implementations ---

/**
 * Default implementation using file extensions, prefixes, and names.
 */
class DefaultFileTypeFilter implements IFileTypeFilter {
    private static final Set<String> IGNORED_EXTENSIONS = Set.of(
            ".ds_store", "thumbs.db", ".tmp", ".bak", ".lock",
            ".freelist", ".gdbindexes", ".gdbtablx", ".atx", ".spx", ".horizon", // ESRI GDB internals
            ".cdx", ".fpt" // dBase/FoxPro/Turboveg index/memo files (keep .dbf)
    );
    private static final List<String> IGNORED_PREFIXES = List.of("~", "._");
    private static final List<String> IGNORED_FILENAMES = List.of("gdb");

    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isEmpty()) {
            return false;
        }
        String filename = getFilenameFromEntry(entryName).toLowerCase();
        if (filename.isEmpty()) return false; // Ignore entries that resolve to empty filename

        for (String prefix : IGNORED_PREFIXES) {
            if (filename.startsWith(prefix)) return false;
        }
        if (IGNORED_FILENAMES.contains(filename)) return false;

        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            String extension = filename.substring(lastDot);
            if (IGNORED_EXTENSIONS.contains(extension)) return false;
        } else if (lastDot == -1 && IGNORED_FILENAMES.contains(filename)) {
            return false; // Handle case like 'gdb' with no extension
        }

        return true; // Relevant if no ignore rule matched
    }

    private String getFilenameFromEntry(String entryName) {
        if (entryName == null) return "";
        String normalizedName = entryName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }
}

/**
 * Default implementation using Apache Tika.
 */
class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser;

    public TikaMetadataProvider() {
        this.tikaParser = new AutoDetectParser(); // Initialize Tika parser
    }

    public TikaMetadataProvider(Parser parser) {
        this.tikaParser = Objects.requireNonNull(parser, "Tika Parser cannot be null");
    }

    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException {
        BodyContentHandler handler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata();
        ParseContext context = new ParseContext();
        try {
            tikaParser.parse(inputStream, handler, metadata, context);
        } catch (Exception e) {
            // Consider logging here if a logging framework was used
            System.err.println("Error during Tika parsing: " + e.getMessage());
            if (e instanceof TikaException) throw (TikaException) e;
            if (e instanceof SAXException) throw (SAXException) e;
            if (e instanceof IOException) throw (IOException) e;
            throw new TikaException("Tika parsing failed unexpectedly", e);
        }
        return new ExtractionOutput(metadata, handler.toString());
    }
}

/**
 * Default implementation formatting data for CKAN.
 */
class DefaultCkanResourceFormatter implements ICkanResourceFormatter {

    private static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final String PLACEHOLDER_URI = "urn:placeholder:vervang-mij";
    private static final Pattern TITLE_PREFIX_PATTERN = Pattern.compile(
            "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
            Pattern.CASE_INSENSITIVE);
    private final LanguageDetector languageDetector; // Inject or initialize

    // Constructor allowing injection or default initialization
    public DefaultCkanResourceFormatter(LanguageDetector languageDetector) {
        this.languageDetector = languageDetector; // Can be null if detection disabled/failed
    }

    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        Map<String, Object> resourceData = new HashMap<>();
        List<Map<String, String>> extras = new ArrayList<>();

        // --- CKAN Resource Core Fields ---
        resourceData.put("__comment_mandatory__", "package_id and url MUST be provided externally.");
        resourceData.put("package_id", "PLACEHOLDER_-_NEEDS_PARENT_DATASET_ID_OR_NAME_FROM_CKAN");
        resourceData.put("url", "PLACEHOLDER_-_NEEDS_PUBLIC_URL_AFTER_UPLOAD_TO_CKAN_OR_SERVER");

        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle);
        String resourceName = Optional.ofNullable(cleanedTitle)
                .orElse(Optional.ofNullable(originalTitle)
                        .orElse(getFilenameFromEntry(entryName)));
        resourceData.put("name", resourceName);
        resourceData.put("description", "PLACEHOLDER_-_Add_a_meaningful_summary_here.");

        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.CREATED))
                .ifPresent(d -> resourceData.put("created", d));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.MODIFIED))
                .ifPresent(d -> resourceData.put("last_modified", d));

        // --- CKAN Extras ---
        addExtra(extras, "source_identifier", sourceIdentifier);
        if (originalTitle != null && !originalTitle.equals(cleanedTitle)) {
            addExtra(extras, "original_extracted_title", originalTitle);
        }
        String originalEntryFilename = getFilenameFromEntry(entryName);
        if (!originalEntryFilename.equals(resourceName)) {
            addExtra(extras, "original_entry_name", originalEntryFilename);
        }
        addExtra(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));
        String creatorTool = getMetadataValue(metadata, TikaCoreProperties.CREATOR_TOOL);
        if (creatorTool != null) {
            addExtra(extras, "creator_tool", creatorTool);
        }
        addExtra(extras, "producer", getMetadataValue(metadata, "producer"));
        addExtra(extras, "mime_type", contentType);
        addExtra(extras, "media_type_iana_uri", mapContentTypeToIanaUri(contentType));
        addExtra(extras, "file_type_eu_uri", mapContentTypeToEuFileTypeUri(contentType));
        addExtra(extras, "pdf_version", getMetadataValue(metadata, "pdf:PDFVersion"));
        addExtra(extras, "page_count", getMetadataValue(metadata, "xmpTPg:NPages"));

        detectLanguage(text, 10000) // Use constant or config for sample length
                .filter(LanguageResult::isReasonablyCertain)
                .ifPresentOrElse(
                        langResult -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri(langResult.getLanguage())),
                        () -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri("und"))
                );

        resourceData.put("extras", extras);

        // --- Dataset Hints ---
        Map<String, Object> datasetHints = new HashMap<>();
        datasetHints.put("potential_dataset_title_suggestion", resourceName);
        String[] subjects = metadata.getValues(DublinCore.SUBJECT);
        if (subjects != null && subjects.length > 0) {
            datasetHints.put("potential_dataset_tags", Arrays.stream(subjects)
                    .filter(s -> s != null && !s.trim().isEmpty())
                    .map(String::trim)
                    .distinct()
                    .collect(Collectors.toList()));
        }
        resourceData.put("__comment_dataset_hints__", "These fields might help populate the parent Dataset");
        resourceData.put("dataset_hints", datasetHints);

        return new CkanResource(resourceData);
    }

    // --- Helper methods specific to formatting ---
    private String getFilenameFromEntry(String entryName) {
        if (entryName == null) return "";
        String normalizedName = entryName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }
    private String cleanTitle(String title) {
        if (title == null || title.trim().isEmpty()) return null;
        String cleaned = TITLE_PREFIX_PATTERN.matcher(title).replaceFirst("");
        cleaned = cleaned.replace('_', ' ');
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }
    private void addExtra(List<Map<String, String>> extras, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            extras.add(Map.of("key", key, "value", value.trim()));
        }
    }
    private String getMetadataValue(Metadata metadata, org.apache.tika.metadata.Property property) {
        String value = metadata.get(property);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }
    private String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }
    private Optional<String> formatIso8601DateTime(String dateTime) {
        return parseToInstant(dateTime).flatMap(this::formatInstantToIso8601);
    }
    private Optional<Instant> parseToInstant(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) return Optional.empty();
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_ZONED_DATE_TIME,
                DateTimeFormatter.ISO_INSTANT, DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                DateTimeFormatter.ISO_LOCAL_DATE);
        for (DateTimeFormatter formatter : formatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME) return Optional.of(java.time.LocalDateTime.parse(dateTime, formatter).atZone(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE) return Optional.of(java.time.LocalDate.parse(dateTime, formatter).atStartOfDay(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_INSTANT) return Optional.of(Instant.parse(dateTime));
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME) return Optional.of(ZonedDateTime.parse(dateTime, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) return Optional.of(OffsetDateTime.parse(dateTime, formatter).toInstant());
            } catch (DateTimeParseException ignored) {}
        }
        System.err.println("Warning: Could not parse date/time: " + dateTime); return Optional.empty();
    }
    private Optional<String> formatInstantToIso8601(Instant instant) {
        return Optional.ofNullable(instant).map(ISO8601_FORMATTER::format);
    }
    private String mapContentTypeToSimpleFormat(String contentType) {
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf" -> "PDF";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "DOCX";
                    case "application/msword" -> "DOC";
                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation" -> "PPTX";
                    case "application/vnd.ms-powerpoint" -> "PPT";
                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> "XLSX";
                    case "application/vnd.ms-excel" -> "XLS";
                    case "text/plain" -> "TXT"; case "text/csv" -> "CSV"; case "text/html" -> "HTML";
                    case "application/rtf" -> "RTF";
                    case "image/jpeg", "image/jpg" -> "JPEG"; case "image/png" -> "PNG"; case "image/gif" -> "GIF";
                    case "image/tiff" -> "TIFF"; case "image/bmp" -> "BMP"; case "image/svg+xml" -> "SVG";
                    case "application/zip" -> "ZIP"; case "application/gzip" -> "GZIP";
                    case "application/x-rar-compressed" -> "RAR"; case "application/x-7z-compressed" -> "7Z";
                    case "application/json" -> "JSON"; case "application/xml", "text/xml" -> "XML";
                    case "application/shp", "application/x-shapefile" -> "SHP"; case "application/x-dbf" -> "DBF";
                    case "application/vnd.google-earth.kml+xml" -> "KML";
                    case "application/geopackage+sqlite3" -> "GPKG"; case "application/geo+json" -> "GEOJSON";
                    default -> { int i = lowerType.lastIndexOf('/'); yield (i!=-1&&i<lowerType.length()-1)?lowerType.substring(i+1).toUpperCase().replaceAll("[^A-Z0-9]",""):lowerType.toUpperCase().replaceAll("[^A-Z0-9]",""); }
                }).orElse("Unknown");
    }
    private String mapContentTypeToIanaUri(String contentType) {
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf" -> "https://www.iana.org/assignments/media-types/application/pdf";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                    case "application/msword" -> "https://www.iana.org/assignments/media-types/application/msword";
                    case "text/plain" -> "https://www.iana.org/assignments/media-types/text/plain";
                    case "text/csv" -> "https://www.iana.org/assignments/media-types/text/csv";
                    case "image/jpeg" -> "https://www.iana.org/assignments/media-types/image/jpeg";
                    case "image/png" -> "https://www.iana.org/assignments/media-types/image/png";
                    case "image/gif" -> "https://www.iana.org/assignments/media-types/image/gif";
                    case "application/zip" -> "https://www.iana.org/assignments/media-types/application/zip";
                    case "application/json" -> "https://www.iana.org/assignments/media-types/application/json";
                    case "application/xml" -> "https://www.iana.org/assignments/media-types/application/xml";
                    case "text/xml" -> "https://www.iana.org/assignments/media-types/text/xml";
                    case "application/shp", "application/x-shapefile" -> "https://www.iana.org/assignments/media-types/application/vnd.shp";
                    case "application/x-dbf" -> "https://www.iana.org/assignments/media-types/application/vnd.dbf";
                    default -> PLACEHOLDER_URI + "-iana-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                }).orElse(PLACEHOLDER_URI + "-iana-unknown");
    }
    private String mapContentTypeToEuFileTypeUri(String contentType) {
        final String EU_FT_BASE = "http://publications.europa.eu/resource/authority/file-type/";
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf" -> EU_FT_BASE + "PDF"; case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> EU_FT_BASE + "DOCX";
                    case "application/msword" -> EU_FT_BASE + "DOC"; case "application/vnd.openxmlformats-officedocument.presentationml.presentation" -> EU_FT_BASE + "PPTX";
                    case "application/vnd.ms-powerpoint" -> EU_FT_BASE + "PPT"; case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> EU_FT_BASE + "XLSX";
                    case "application/vnd.ms-excel" -> EU_FT_BASE + "XLS"; case "text/plain" -> EU_FT_BASE + "TXT"; case "text/csv" -> EU_FT_BASE + "CSV";
                    case "image/jpeg", "image/jpg" -> EU_FT_BASE + "JPEG"; case "image/png" -> EU_FT_BASE + "PNG"; case "image/gif" -> EU_FT_BASE + "GIF";
                    case "image/tiff" -> EU_FT_BASE + "TIFF"; case "application/zip" -> EU_FT_BASE + "ZIP"; case "application/xml", "text/xml" -> EU_FT_BASE + "XML";
                    case "application/shp", "application/x-shapefile" -> EU_FT_BASE + "SHP"; case "application/x-dbf" -> EU_FT_BASE + "DBF";
                    default -> PLACEHOLDER_URI + "-eu-filetype-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                }).orElse(PLACEHOLDER_URI + "-eu-filetype-unknown");
    }
    private Optional<LanguageResult> detectLanguage(String text, int maxSampleLength) {
        if (this.languageDetector == null || text == null || text.trim().isEmpty()) return Optional.empty();
        try {
            String textSample = text.substring(0, Math.min(maxSampleLength, text.length()));
            if (textSample.isBlank()) return Optional.empty();
            return Optional.of(this.languageDetector.detect(textSample));
        } catch (Exception e) {
            System.err.println("Warning: Error during language detection: " + e.getMessage()); return Optional.empty();
        }
    }
    private String mapLanguageCodeToNalUri(String langCode) {
        final String EU_LANG_BASE = "http://publications.europa.eu/resource/authority/language/";
        if (langCode == null || langCode.isBlank()) return EU_LANG_BASE + "UND";
        String normCode = langCode.toLowerCase().split("-")[0];
        return switch (normCode) {
            case "nl"->"NLD"; case "en"->"ENG"; case "de"->"DEU"; case "fr"->"FRA";
            case "es"->"SPA"; case "it"->"ITA"; case "und"->"UND"; default->"MUL";
        }; // Simplified: assumes base URI prefix is handled elsewhere or concatenated
        // Corrected version:
        // return EU_LANG_BASE + switch (normCode) {
        //     case "nl" -> "NLD"; case "en" -> "ENG"; case "de" -> "DEU"; case "fr" -> "FRA";
        //     case "es" -> "SPA"; case "it" -> "ITA"; case "und" -> "UND"; default -> "MUL";
        // };
        // Re-correction - Need full URI
        return EU_LANG_BASE + switch (normCode) {
            case "nl" -> "NLD"; case "en" -> "ENG"; case "de" -> "DEU"; case "fr" -> "FRA";
            case "es" -> "SPA"; case "it" -> "ITA"; case "und" -> "UND"; default -> "MUL";
        };
    }
}

/**
 * Processes ZIP archives, handling nesting and delegating file processing.
 */
class ZipSourceProcessor implements ISourceProcessor {
    private static final Set<String> SUPPORTED_ZIP_EXTENSIONS = Set.of(".zip"); // Keep zip check local?

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final int maxTextLength; // Configurable text limit

    public ZipSourceProcessor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, int maxTextLength) {
        this.fileFilter = Objects.requireNonNull(fileFilter);
        this.metadataProvider = Objects.requireNonNull(metadataProvider);
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter);
        this.maxTextLength = maxTextLength;
    }

    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) throws IOException {
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName;

                if (entry.isDirectory()) {
                    zis.closeEntry(); continue;
                }
                if (!fileFilter.isFileTypeRelevant(entryName)) {
                    ignored.add(new IgnoredEntry(fullEntryPath, "File type ignored by filter"));
                    System.err.println("Ignoring entry based on filter: " + fullEntryPath);
                    zis.closeEntry(); continue;
                }

                if (isZipEntryZip(entry)) {
                    processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                } else {
                    processRegularEntry(entryName, zis, fullEntryPath, results, errors);
                }
                zis.closeEntry(); // Ensure entry is closed before next iteration
            }
        } catch (IOException e) {
            System.err.println("Error reading ZIP file '" + zipPath + "': " + e.getMessage());
            // Add error for the container itself? Or rethrow? Rethrowing for now.
            throw e;
        }
    }

    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("Processing nested ZIP: " + fullEntryPath);
            // Create a new instance of this processor or reuse? Reuse for now.
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Failed to process nested zip: " + e.getMessage()));
            System.err.println("Error processing nested ZIP entry '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("Warning: Failed to delete temporary file '" + tempZip + "': " + e.getMessage());
                }
            }
        }
    }

    private void processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors) {
        try {
            InputStream nonClosingStream = new NonClosingInputStream(zis);
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(nonClosingStream, maxTextLength);
            CkanResource resource = resourceFormatter.format(entryName, output.metadata, output.text, fullEntryPath);
            results.add(resource);
        } catch (Exception e) {
            errors.add(new ProcessingError(fullEntryPath, e.getClass().getSimpleName() + ": " + e.getMessage()));
            System.err.println("Error processing entry '" + entryName + "': " + e.getMessage());
        }
    }

    private boolean isZipEntryZip(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) return false;
        String lowerCaseName = entry.getName().toLowerCase();
        return SUPPORTED_ZIP_EXTENSIONS.stream().anyMatch(lowerCaseName::endsWith);
    }
}

// --- Facade Class ---

/**
 * Main entry point for metadata extraction, orchestrating the different components.
 */
public class MetadataExtractorFacade {

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ISourceProcessor zipProcessor; // Handles ZIPs
    private final int maxTextLength;

    /**
     * Constructor for dependency injection.
     */
    public MetadataExtractorFacade(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ISourceProcessor zipProcessor, int maxTextLength) {
        this.fileFilter = Objects.requireNonNull(fileFilter);
        this.metadataProvider = Objects.requireNonNull(metadataProvider);
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter);
        this.zipProcessor = Objects.requireNonNull(zipProcessor);
        this.maxTextLength = maxTextLength;
    }

    /**
     * Processes a given source path (file or ZIP) and returns a report.
     * @param sourcePathString Path to the source file or ZIP archive.
     * @return A ProcessingReport containing results, errors, and ignored entries.
     * @throws IOException If the source path is invalid or major I/O errors occur.
     */
    public ProcessingReport processSource(String sourcePathString) throws IOException {
        Path sourcePath = Paths.get(sourcePathString);
        if (!Files.exists(sourcePath)) {
            throw new IOException("Source not found: " + sourcePathString);
        }

        List<CkanResource> successfulResults = new ArrayList<>();
        List<ProcessingError> processingErrors = new ArrayList<>();
        List<IgnoredEntry> ignoredEntries = new ArrayList<>();

        if (Files.isDirectory(sourcePath)) {
            // Decide how to handle directories - ignore, process recursively? Ignoring for now.
            ignoredEntries.add(new IgnoredEntry(sourcePathString, "Source is a directory (not processed)"));
            System.err.println("Source is a directory, skipping: " + sourcePathString);

        } else if (isZipFile(sourcePath)) {
            // Delegate ZIP processing
            zipProcessor.processSource(sourcePath, sourcePath.toString(), successfulResults, processingErrors, ignoredEntries);

        } else {
            // Process single file
            String filename = sourcePath.getFileName().toString();
            if (fileFilter.isFileTypeRelevant(filename)) {
                try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
                    IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, maxTextLength);
                    CkanResource resource = resourceFormatter.format(filename, output.metadata, output.text, sourcePath.toString());
                    successfulResults.add(resource);
                } catch (Exception e) {
                    processingErrors.add(new ProcessingError(sourcePathString, e.getClass().getSimpleName() + ": " + e.getMessage()));
                    System.err.println("Error processing single file '" + sourcePath + "': " + e.getMessage());
                }
            } else {
                ignoredEntries.add(new IgnoredEntry(sourcePathString, "File type ignored by filter"));
                System.err.println("Ignoring file based on filter: " + filename);
            }
        }

        return new ProcessingReport(successfulResults, processingErrors, ignoredEntries);
    }

    // Helper to check if a Path is a zip file (could be static utility)
    private boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) { // Check if it's a regular file
            return false;
        }
        String fileName = path.getFileName().toString().toLowerCase();
        // Use the same constant as ZipSourceProcessor or define centrally
        final Set<String> SUPPORTED_ZIP_EXTENSIONS = Set.of(".zip");
        return SUPPORTED_ZIP_EXTENSIONS.stream().anyMatch(fileName::endsWith);
    }


    // --- Main Method for Demonstration ---
    public static void main(String[] args) {
        String filePath = "C:\\Users\\gurelb\\Downloads\\Veg kartering - habitatkaart 2021-2023.zip"; // Example path

        if (args.length > 0) {
            filePath = args[0];
        } else {
            System.err.println("Warning: No file path provided, using default: " + filePath);
        }

        Path sourcePath = Paths.get(filePath);
        if (!Files.exists(sourcePath)) {
            System.err.println("Error: File path does not exist: " + filePath);
            return;
        }

        // --- Dependency Setup ---
        LanguageDetector languageDetector = null;
        try {
            languageDetector = OptimaizeLangDetector.getDefaultLanguageDetector().loadModels();
        } catch (IOException e) {
            System.err.println("Warning: Failed to load language models: " + e.getMessage());
        }

        IFileTypeFilter filter = new DefaultFileTypeFilter();
        IMetadataProvider provider = new TikaMetadataProvider(); // Uses default AutoDetectParser
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormatter(languageDetector);
        ISourceProcessor zipProcessor = new ZipSourceProcessor(filter, provider, formatter, 5 * 1024 * 1024); // Use constant
        int maxTextLengthForSingleFiles = 5 * 1024 * 1024; // Use constant

        // Create the main facade instance with dependencies
        MetadataExtractorFacade extractor = new MetadataExtractorFacade(filter, provider, formatter, zipProcessor, maxTextLengthForSingleFiles);

        // --- Execution & Output ---
        try {
            ProcessingReport report = extractor.processSource(filePath);

            // Serialize the report object to JSON
            ObjectMapper jsonMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            // Configure mapper to handle the CkanResource data map correctly if needed
            // For example, if CkanResource had specific serialization needs.
            // By default, it should serialize the internal map of CkanResource fine.
            String jsonOutput = jsonMapper.writeValueAsString(report);

            System.out.println(jsonOutput);

        } catch (Exception e) {
            System.err.println("Critical Error during processing source '" + filePath + "': " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    // --- Utility Inner Class ---
    private static class NonClosingInputStream extends FilterInputStream {
        protected NonClosingInputStream(InputStream in) { super(in); }
        @Override public void close() {} // Prevents closing the underlying stream
    }

} // End of class MetadataExtractorFacade (Main Entry Point)
