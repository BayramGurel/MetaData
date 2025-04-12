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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Extracts metadata from files and formats it for CKAN resource creation.
 * This class is designed to be reusable and extensible, following object-oriented principles.
 */
public class MetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MetadataExtractor.class);

    // Constants for frequently used values
    private static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final String PLACEHOLDER_URI = "urn:placeholder:vervang-mij";
    private static final Pattern TITLE_PREFIX_PATTERN = Pattern.compile(
            "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
            Pattern.CASE_INSENSITIVE);
    private static final int MAX_TEXT_SAMPLE_LENGTH = 10000; // Define a constant for max text sample

    private final LanguageDetector languageDetector;
    private final ObjectMapper objectMapper; // Use a single ObjectMapper instance

    /**
     * Constructor for MetadataExtractor.  Initializes the language detector and ObjectMapper.
     */
    public MetadataExtractor() {
        this.languageDetector = initializeLanguageDetector();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // Configure ObjectMapper once
    }

    /**
     * Initializes the language detector.  Handles potential errors during initialization.
     * @return The initialized LanguageDetector, or null if initialization fails.
     */
    private LanguageDetector initializeLanguageDetector() {
        try {
            return OptimaizeLangDetector.getDefaultLanguageDetector().loadModels();
        } catch (IOException e) {
            logger.warn("Language detection models not found or could not be loaded. Language detection is disabled. Error: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Main method for the MetadataExtractor.  Demonstrates usage by extracting metadata
     * from a sample file and printing the JSON output.
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        String filePath = "C:\\Users\\gurelb\\Downloads\\datawarehouse\\Veg kartering - habitatkaart 2021-2023\\Veg kartering - habitatkaart 2021-2023\\PZH_WestduinparkEnWapendal_2023_HabitatRapport.pdf"; // Replace with a valid file path
        MetadataExtractor extractor = new MetadataExtractor();
        try {
            String jsonOutput = extractor.extractResourceMetadataToJson(filePath);
            System.out.println(jsonOutput);
        } catch (Exception e) {
            logger.error("Error extracting metadata: {}", e.getMessage(), e); // Use SLF4J for logging
        }
    }

    /**
     * Extracts metadata from a file and returns it as a JSON string, formatted for CKAN.
     * This is the primary entry point for using the class.
     * @param filePath The path to the file to process.
     * @return A JSON string representing the extracted metadata in CKAN format.
     * @throws IOException If an error occurs while reading the file.
     * @throws TikaException If Tika fails to parse the file.
     * @throws SAXException If an error occurs during XML processing (unlikely with AutoDetectParser).
     */
    public String extractResourceMetadataToJson(String filePath) throws IOException, TikaException, SAXException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new IOException("File not found: " + filePath);
        }
        Metadata metadata = extractMetadata(path);
        String text = extractText(path);
        Map<String, Object> ckanResourceData = createCkanResourceData(path, metadata, text);
        return objectMapper.writeValueAsString(ckanResourceData);
    }

    /**
     * Extracts metadata from a file using Apache Tika.
     * @param path The path to the file.
     * @return A Metadata object containing the extracted metadata.
     * @throws IOException If an error occurs while reading the file.
     * @throws TikaException If Tika fails to parse the file.
     * @throws SAXException If an error occurs during XML processing.
     */
    private Metadata extractMetadata(Path path) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        try (InputStream stream = Files.newInputStream(path)) {
            parser.parse(stream, new BodyContentHandler(-1), metadata, new ParseContext());
        }
        return metadata;
    }

    /**
     * Extracts text content from a file using Apache Tika.
     * @param path The path to the file.
     * @return The extracted text content.
     * @throws IOException If an error occurs while reading the file.
     * @throws TikaException If Tika fails to parse the file.
     * @throws SAXException If an error occurs during XML processing.
     */
    private String extractText(Path path) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata textMetadata = new Metadata(); // Not strictly needed, but good practice
        try (InputStream stream = Files.newInputStream(path)) {
            parser.parse(stream, handler, textMetadata, new ParseContext());
        }
        return handler.toString();
    }

    /**
     * Creates a Map representing the CKAN Resource structure based on extracted metadata.
     * Includes title cleaning.
     * @param path The path object of the file being processed.
     * @param metadata The extracted Tika metadata object.
     * @param text The extracted text content (primarily used for language detection).
     * @return A Map structured for a CKAN Resource API call (includes placeholders).
     */
    private Map<String, Object> createCkanResourceData(Path path, Metadata metadata, String text) {
        Map<String, Object> resourceData = new HashMap<>();
        List<Map<String, String>> extras = new ArrayList<>();

        // --- CKAN Resource Core Fields ---
        resourceData.put("__comment_mandatory__", "package_id (CKAN Dataset ID) and url (Public File URL) MUST be provided externally.");
        resourceData.put("package_id", "PLACEHOLDER_-_NEEDS_PARENT_DATASET_ID_OR_NAME_FROM_CKAN");
        resourceData.put("url", "PLACEHOLDER_-_NEEDS_PUBLIC_URL_AFTER_UPLOAD_TO_CKAN_OR_SERVER");

        // Resource Name: Extract title, clean it, then use filename as fallback.
        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle);
        resourceData.put("name", (cleanedTitle != null ? cleanedTitle : path.getFileName().toString()));

        // Resource Description: Placeholder explaining manual input is usually needed.
        resourceData.put("description", "PLACEHOLDER_-_Add_a_meaningful_summary_here._Automatic_extraction_is_not_feasible.");

        // Format, created, last_modified
        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.CREATED))
                .ifPresent(d -> resourceData.put("created", d));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.MODIFIED))
                .ifPresent(d -> resourceData.put("last_modified", d));

        // --- CKAN Extras (Key-Value Pairs) ---
        // Add original (uncleaned) title to extras if it was present and cleaned
        if (originalTitle != null && !originalTitle.equals(cleanedTitle)) {
            addExtra(extras, "original_extracted_title", originalTitle);
        }

        addExtra(extras, "original_filename", path.toString());
        addExtra(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));
        // Add creator_tool (TikaCoreProperties) if different from the cleaned title derived value
        String creatorTool = getMetadataValue(metadata, TikaCoreProperties.CREATOR_TOOL);
        if (creatorTool != null && !creatorTool.equalsIgnoreCase(originalTitle)) {
            addExtra(extras, "creator_tool", creatorTool);
        }
        addExtra(extras, "producer", getMetadataValue(metadata, "producer"));
        addExtra(extras, "mime_type", contentType);
        addExtra(extras, "media_type_iana_uri", mapContentTypeToIanaUri(contentType));
        addExtra(extras, "file_type_eu_uri", mapContentTypeToEuFileTypeUri(contentType));
        addExtra(extras, "pdf_version", getMetadataValue(metadata, "pdf:PDFVersion"));
        addExtra(extras, "page_count", getMetadataValue(metadata, "xmpTPg:NPages"));

        detectLanguage(text)
                .filter(LanguageResult::isReasonablyCertain)
                .ifPresentOrElse(
                        langResult -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri(langResult.getLanguage())),
                        () -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri("und"))
                );

        resourceData.put("extras", extras);

        // --- Hints for Parent Dataset ---
        Map<String, Object> datasetHints = new HashMap<>();
        // Use the CLEANED title as the suggestion for the dataset title
        if (cleanedTitle != null) {
            datasetHints.put("potential_dataset_title_suggestion", cleanedTitle);
        }
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

        return resourceData;
    }

    /**
     * Cleans a potential title string extracted from metadata.
     * Removes common application prefixes and replaces underscores with spaces.
     * @param title The raw title string.
     * @return The cleaned title string, or null if the input was null/empty.
     */
    private String cleanTitle(String title) {
        if (title == null || title.trim().isEmpty()) {
            return null;
        }
        String cleaned = title;
        // Remove known prefixes using regex (case-insensitive)
        cleaned = TITLE_PREFIX_PATTERN.matcher(cleaned).replaceFirst("");
        // Replace underscores with spaces
        cleaned = cleaned.replace('_', ' ');
        // Replace multiple spaces with a single space
        cleaned = cleaned.replaceAll("\\s+", " ").trim();

        return cleaned.isEmpty() ? null : cleaned;
    }

    /**
     * Adds a key-value pair to the 'extras' list, if the value is not null or empty.
     * @param extras The list of extra metadata key-value pairs.
     * @param key The key of the extra metadata.
     * @param value The value of the extra metadata.
     */
    private void addExtra(List<Map<String, String>> extras, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            Map<String, String> extra = new HashMap<>();
            extra.put("key", key);
            extra.put("value", value.trim());
            extras.add(extra);
        }
    }

    /**
     * Retrieves a metadata value from the Metadata object, handling null and empty values.
     * @param metadata The Metadata object.
     * @param property The Tika Property to retrieve.
     * @return The trimmed metadata value, or null if the property is not set or empty.
     */
    private String getMetadataValue(Metadata metadata, org.apache.tika.metadata.Property property) {
        String value = metadata.get(property);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }

    /**
     * Retrieves a metadata value from the Metadata object, handling null and empty values.
     * @param metadata The Metadata object.
     * @param key The key of the metadata to retrieve.
     * @return The trimmed metadata value, or null if the key is not set or empty.
     */
    private String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }

    /**
     * Maps a content type string to a simplified, uppercase format string.
     * @param contentType The content type string (e.g., "application/pdf").
     * @return A simplified format string (e.g., "PDF"), or "Unknown" if the mapping fails.
     */
    private String mapContentTypeToSimpleFormat(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> {
                    // Use a switch expression for more concise mapping
                    return switch (lowerType) {
                        case "application/pdf" -> "PDF";
                        case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "DOCX";
                        case "application/msword" -> "DOC";
                        case "text/plain" -> "TXT";
                        case "text/csv" -> "CSV";
                        case "application/vnd.ms-excel" -> "XLS";
                        case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> "XLSX";
                        case "application/zip" -> "ZIP";
                        case "image/jpeg", "image/jpg" -> "JPEG"; // Combine cases
                        case "image/png" -> "PNG";
                        case "image/gif" -> "GIF";
                        case "application/json" -> "JSON";
                        case "application/xml", "text/xml" -> "XML"; // Combine cases
                        case "application/shp", "application/x-shapefile" -> "SHP"; // Combine cases
                        case "application/vnd.google-earth.kml+xml" -> "KML";
                        case "application/geopackage+sqlite3" -> "GPKG";
                        default -> lowerType.toUpperCase();
                    };
                })
                .orElse("Unknown");
    }

    /**
     * Parses a date/time string into an Instant object, trying multiple formats.
     * Uses Optional to handle parsing failures more cleanly.
     * @param dateTime The date/time string to parse.
     * @return An Optional containing the parsed Instant, or empty if parsing fails.
     */
    private Optional<Instant> parseToInstant(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) {
            return Optional.empty();
        }
        // Try parsing with different formats
        try { return Optional.of(OffsetDateTime.parse(dateTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant()); } catch (DateTimeParseException ignored) {}
        try { return Optional.of(ZonedDateTime.parse(dateTime, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant()); } catch (DateTimeParseException ignored) {}
        try { return Optional.of(Instant.parse(dateTime)); } catch (DateTimeParseException ignored) {}
        try { return Optional.of(java.time.LocalDateTime.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME).atZone(ZoneId.systemDefault()).toInstant()); } catch (DateTimeParseException ignored) {}
        try { return Optional.of(java.time.LocalDate.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay(ZoneId.systemDefault()).toInstant()); } catch (DateTimeParseException ignored) {}

        logger.warn("Could not parse date/time string to Instant: {}", dateTime);
        return Optional.empty();
    }

    /**
     * Formats an Instant object as an ISO 8601 date/time string.
     * Uses Optional to handle null instants.
     * @param instant The Instant object to format.
     * @return An Optional containing the formatted ISO 8601 string, or empty if the input is null.
     */
    private Optional<String> formatInstantToIso8601(Instant instant) {
        return Optional.ofNullable(instant).map(ISO8601_FORMATTER::format);
    }

    /**
     * Formats a date/time string (in any parsable format) as an ISO 8601 string.
     * Uses Optional chaining for a more functional style.
     * @param dateTime The date/time string to format.
     * @return An Optional containing the formatted ISO 8601 string, or empty if parsing or formatting fails.
     */
    private Optional<String> formatIso8601DateTime(String dateTime) {
        return parseToInstant(dateTime).flatMap(this::formatInstantToIso8601);
    }

    /**
     * Maps a content type to its corresponding IANA URI.
     * @param contentType The content type string.
     * @return The IANA URI, or a placeholder URI if no mapping is found.
     */
    private String mapContentTypeToIanaUri(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> {
                    // Use a switch expression for more concise mapping
                    return switch (lowerType) {
                        case "application/pdf" -> "https://www.iana.org/assignments/media-types/application/pdf";
                        case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ->
                                "https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                        case "application/msword" -> "https://www.iana.org/assignments/media-types/application/msword";
                        case "text/plain" -> "https://www.iana.org/assignments/media-types/text/plain";
                        case "text/csv" -> "https://www.iana.org/assignments/media-types/text/csv";
                        case "image/jpeg", "image/jpg" -> "https://www.iana.org/assignments/media-types/image/jpeg";
                        case "image/png" -> "https://www.iana.org/assignments/media-types/image/png";
                        case "image/gif" -> "https://www.iana.org/assignments/media-types/image/gif";
                        default ->
                                PLACEHOLDER_URI + "-iana-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-"); // Sanitize
                    };
                })
                .orElse(PLACEHOLDER_URI + "-iana-unknown");
    }

    /**
     * Maps a content type to its corresponding EU file type URI.
     * @param contentType The content type string.
     * @return The EU file type URI, or a placeholder URI if no mapping is found.
     */
    private String mapContentTypeToEuFileTypeUri(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> {
                    // Use a switch expression for more concise mapping
                    return switch (lowerType) {
                        case "application/pdf" -> "http://publications.europa.eu/resource/authority/file-type/PDF";
                        case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ->
                                "http://publications.europa.eu/resource/authority/file-type/DOCX";
                        case "application/msword" -> "http://publications.europa.eu/resource/authority/file-type/DOC";
                        case "text/plain" -> "http://publications.europa.eu/resource/authority/file-type/TXT";
                        case "text/csv" -> "http://publications.europa.eu/resource/authority/file-type/CSV";
                        case "image/jpeg", "image/jpg" -> "http://publications.europa.eu/resource/authority/file-type/JPEG";
                        case "image/png" -> "http://publications.europa.eu/resource/authority/file-type/PNG";
                        case "image/gif" -> "http://publications.europa.eu/resource/authority/file-type/GIF";
                        default ->
                                PLACEHOLDER_URI + "-eu-filetype-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-"); // Sanitize
                    };
                })
                .orElse(PLACEHOLDER_URI + "-eu-filetype-unknown");
    }

    /**
     * Maps a language code to its corresponding NAL (Named Authority List) URI.
     * @param langCode The language code (e.g., "en", "nl").
     * @return The NAL URI, or a default URI if no mapping is found.
     */
    private String mapLanguageCodeToNalUri(String langCode) {
        if (langCode == null || langCode.isBlank()) {
            return "http://publications.europa.eu/resource/authority/language/UND";
        }
        // Use a switch expression for more concise mapping
        return switch (langCode.toLowerCase()) {
            case "nl" -> "http://publications.europa.eu/resource/authority/language/NLD";
            case "en" -> "http://publications.europa.eu/resource/authority/language/ENG";
            case "de" -> "http://publications.europa.eu/resource/authority/language/DEU";
            case "fr" -> "http://publications.europa.eu/resource/authority/language/FRA";
            case "und" -> "http://publications.europa.eu/resource/authority/language/UND";
            default -> "http://publications.europa.eu/resource/authority/language/MUL";
        };
    }

    /**
     * Detects the language of a text sample.
     * @param text The text to analyze.
     * @return An Optional containing the LanguageResult, or empty if language detection is disabled or fails.
     */
    private Optional<LanguageResult> detectLanguage(String text) {
        if (this.languageDetector == null || text == null || text.trim().isEmpty()) {
            return Optional.empty(); // Return empty Optional for null/empty text or disabled detector
        }
        try {
            // Use the constant for max text sample length
            String textSample = text.substring(0, Math.min(MAX_TEXT_SAMPLE_LENGTH, text.length()));
            if (textSample.isBlank()) return Optional.empty();
            LanguageResult result = this.languageDetector.detect(textSample);
            return Optional.of(result);
        } catch (Exception e) {
            logger.warn("Error during language detection: {}", e.getMessage()); // Log the error
            return Optional.empty();
        }
    }
}
