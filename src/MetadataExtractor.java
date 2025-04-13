import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.TikaCoreProperties; // Nodig voor TikaCoreProperties.DESCRIPTION
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

// --- Configuratie Constanten ---
/**
 * Bevat configuratieconstanten voor de metadata extractor.
 */
final class ExtractorConfig {
    private ExtractorConfig() {} // Voorkom instantiatie

    public static final int MAX_TEXT_SAMPLE_LENGTH = 10000; // Voor taaldetectie
    public static final int MAX_EXTRACTED_TEXT_LENGTH = 5 * 1024 * 1024; // Limiteer tekstextractie tot 5MB
    public static final int MAX_AUTO_DESCRIPTION_LENGTH = 250; // Max lengte voor auto-gegenereerde beschrijving uit tekst
    public static final int MIN_DESC_METADATA_LENGTH = 10; // Minimale lengte voor bruikbare metadata beschrijving
    public static final int MAX_DESC_METADATA_LENGTH = 1000; // Maximale lengte (voor check, niet voor inkorten)
    public static final Set<String> SUPPORTED_ZIP_EXTENSIONS = Set.of(".zip");

    // Definieer bestandstypen/patronen om te negeren
    public static final Set<String> IGNORED_EXTENSIONS = Set.of(
            ".ds_store", "thumbs.db", ".tmp", ".bak", ".lock",
            ".freelist", ".gdbindexes", ".gdbtablx", ".atx", ".spx", ".horizon", // ESRI GDB internals
            ".cdx", ".fpt" // dBase/FoxPro/Turboveg index/memo bestanden (behoud .dbf)
    );
    public static final List<String> IGNORED_PREFIXES = List.of("~", "._");
    public static final List<String> IGNORED_FILENAMES = List.of("gdb");

    public static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    public static final String PLACEHOLDER_URI = "urn:placeholder:vervang-mij";
    public static final Pattern TITLE_PREFIX_PATTERN = Pattern.compile(
            "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
            Pattern.CASE_INSENSITIVE);
}


// --- Data Transfer Objects / Resultaat Klassen ---

/**
 * Representeert de succesvol geëxtraheerde en geformatteerde metadata voor één bron.
 */
class CkanResource {
    private final Map<String, Object> data;

    public CkanResource(Map<String, Object> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }
    // Maakt directe toegang tot de data mogelijk voor serialisatie
    public Map<String, Object> getData() { return data; }
}

/**
 * Representeert een fout die is opgetreden tijdens het verwerken van een specifieke bron-entry.
 */
class ProcessingError {
    private final String source;
    private final String error;

    public ProcessingError(String source, String error) {
        this.source = source; this.error = error;
    }
    public String getSource() { return source; }
    public String getError() { return error; }
}

/**
 * Representeert een bestand/entry dat is genegeerd op basis van filterregels.
 */
class IgnoredEntry {
    private final String source;
    private final String reason;

    public IgnoredEntry(String source, String reason) {
        this.source = source; this.reason = reason;
    }
    public String getSource() { return source; }
    public String getReason() { return reason; }
}

/**
 * Omvat het algehele resultaat van een verwerkingsoperatie.
 */
class ProcessingReport {
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }
    public List<CkanResource> getResults() { return results; }
    public List<ProcessingError> getErrors() { return errors; }
    public List<IgnoredEntry> getIgnored() { return ignored; }
}


// --- Interfaces voor Componenten ---

/**
 * Interface om te bepalen of een bestand/entry relevant is voor verwerking.
 */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/**
 * Interface voor het extraheren van metadata en tekstinhoud uit een input stream.
 */
interface IMetadataProvider {
    /** Representeert gecombineerde metadata en tekst */
    class ExtractionOutput {
        final Metadata metadata;
        final String text;
        ExtractionOutput(Metadata m, String t) { this.metadata = m; this.text = t; }
    }
    ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException;
}

/**
 * Interface voor het formatteren van geëxtraheerde metadata naar een CKAN resource structuur.
 */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/**
 * Interface voor het verwerken van een bron (bestand of archief).
 */
interface ISourceProcessor {
    /** Verwerkt de gegeven bron en voegt resultaten/fouten/genegeerde items toe aan de lijsten. */
    void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}

// --- Utility Inner Class (Moved Here) ---
/**
 * Helper InputStream wrapper die voorkomt dat de close() methode de onderliggende stream sluit.
 */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }
    @Override public void close() {} // Voorkomt sluiten onderliggende stream
}


// --- Standaard Implementaties ---

/**
 * Standaard implementatie die filtert op basis van bestandsextensies, prefixes en namen.
 */
class DefaultFileTypeFilter implements IFileTypeFilter {
    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isEmpty()) return false;
        String filename = getFilenameFromEntry(entryName).toLowerCase();
        if (filename.isEmpty()) return false;

        for (String prefix : ExtractorConfig.IGNORED_PREFIXES) {
            if (filename.startsWith(prefix)) return false;
        }
        if (ExtractorConfig.IGNORED_FILENAMES.contains(filename)) return false;

        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            String extension = filename.substring(lastDot);
            if (ExtractorConfig.IGNORED_EXTENSIONS.contains(extension)) return false;
        } else if (lastDot == -1 && ExtractorConfig.IGNORED_FILENAMES.contains(filename)) {
            return false;
        }
        return true;
    }

    public static String getFilenameFromEntry(String entryName) {
        if (entryName == null) return "";
        String normalizedName = entryName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }
}

/**
 * Standaard implementatie die Apache Tika gebruikt voor metadata extractie.
 */
class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser;

    public TikaMetadataProvider() { this(new AutoDetectParser()); }
    public TikaMetadataProvider(Parser parser) { this.tikaParser = Objects.requireNonNull(parser); }

    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException {
        BodyContentHandler handler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata();
        ParseContext context = new ParseContext();
        try {
            tikaParser.parse(inputStream, handler, metadata, context);
        } catch (Exception e) {
            System.err.println("Fout tijdens Tika parsing: " + e.getMessage());
            if (e instanceof TikaException) throw (TikaException) e;
            if (e instanceof SAXException) throw (SAXException) e;
            if (e instanceof IOException) throw (IOException) e;
            throw new TikaException("Tika parsing onverwacht mislukt", e);
        }
        return new ExtractionOutput(metadata, handler.toString());
    }
}

/**
 * Standaard implementatie die data formatteert voor CKAN.
 * Probeert nu een betere automatische beschrijving te genereren.
 */
class DefaultCkanResourceFormatter implements ICkanResourceFormatter {
    private final LanguageDetector languageDetector;

    public DefaultCkanResourceFormatter(LanguageDetector languageDetector) {
        this.languageDetector = languageDetector;
    }

    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        Map<String, Object> resourceData = new HashMap<>();
        List<Map<String, String>> extras = new ArrayList<>();
        String filename = DefaultFileTypeFilter.getFilenameFromEntry(entryName);

        // --- Kernvelden ---
        resourceData.put("__comment_mandatory__", "package_id en url MOETEN extern worden aangeleverd.");
        resourceData.put("package_id", "PLACEHOLDER_-_NEEDS_PARENT_DATASET_ID_OR_NAME_FROM_CKAN");
        resourceData.put("url", "PLACEHOLDER_-_NEEDS_PUBLIC_URL_AFTER_UPLOAD_TO_CKAN_OR_SERVER");

        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle);
        String resourceName = Optional.ofNullable(cleanedTitle)
                .orElse(Optional.ofNullable(originalTitle)
                        .orElse(filename));
        resourceData.put("name", resourceName);

        // --- Verbeterde Beschrijving ---
        String description = generateDescription(metadata, text, filename);
        resourceData.put("description", description);

        // --- Overige Velden ---
        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.CREATED))
                .ifPresent(d -> resourceData.put("created", d));
        formatIso8601DateTime(metadata.get(TikaCoreProperties.MODIFIED))
                .ifPresent(d -> resourceData.put("last_modified", d));

        // --- Extras ---
        addExtra(extras, "source_identifier", sourceIdentifier);
        if (originalTitle != null && !originalTitle.equals(cleanedTitle)) {
            addExtra(extras, "original_extracted_title", originalTitle);
        }
        if (!filename.equals(resourceName)) {
            addExtra(extras, "original_entry_name", filename);
        }
        addExtra(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));
        String creatorTool = getMetadataValue(metadata, TikaCoreProperties.CREATOR_TOOL);
        if (creatorTool != null) addExtra(extras, "creator_tool", creatorTool);
        addExtra(extras, "producer", getMetadataValue(metadata, "producer"));
        addExtra(extras, "mime_type", contentType);
        addExtra(extras, "media_type_iana_uri", mapContentTypeToIanaUri(contentType));
        addExtra(extras, "file_type_eu_uri", mapContentTypeToEuFileTypeUri(contentType));
        addExtra(extras, "pdf_version", getMetadataValue(metadata, "pdf:PDFVersion"));
        addExtra(extras, "page_count", getMetadataValue(metadata, "xmpTPg:NPages"));

        detectLanguage(text, ExtractorConfig.MAX_TEXT_SAMPLE_LENGTH)
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
                    .filter(s -> s != null && !s.trim().isEmpty()).map(String::trim).distinct().collect(Collectors.toList()));
        }
        resourceData.put("__comment_dataset_hints__", "Deze velden kunnen helpen bij het vullen van de bovenliggende Dataset");
        resourceData.put("dataset_hints", datasetHints);

        return new CkanResource(resourceData);
    }

    /**
     * Genereert een beschrijving op basis van beschikbare metadata of tekstinhoud.
     * Past kwaliteitschecks toe en verwijdert prefixen voor productie.
     */
    private String generateDescription(Metadata metadata, String text, String filename) {
        // 1. Probeer expliciete beschrijvingsvelden met kwaliteitscheck
        String desc = getMetadataValue(metadata, DublinCore.DESCRIPTION);
        if (!isDescriptionValid(desc)) { // Check dc:description
            desc = getMetadataValue(metadata, TikaCoreProperties.DESCRIPTION);
            if (!isDescriptionValid(desc)) { // Check cp:description
                desc = null; // Beide ongeldig
            }
        }

        if (desc != null) {
            // Trim, kort in, voeg ellipsis toe indien nodig
            desc = desc.trim();
            if (desc.length() > ExtractorConfig.MAX_AUTO_DESCRIPTION_LENGTH) {
                desc = desc.substring(0, ExtractorConfig.MAX_AUTO_DESCRIPTION_LENGTH) + "...";
            }
            // Verwijder potentieel leidende/volgende non-alfanumerieke tekens
            desc = desc.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!desc.isBlank()) {
                return desc; // Return de schone metadata beschrijving
            }
        }

        // 2. Probeer eerste deel van de tekstinhoud
        if (text != null && !text.isBlank()) {
            String cleanedText = text.trim().replaceAll("\\s+", " "); // Vervang witruimte
            if (cleanedText.length() > 10) { // Eis minimale lengte
                String snippet = cleanedText.substring(0, Math.min(cleanedText.length(), ExtractorConfig.MAX_AUTO_DESCRIPTION_LENGTH));
                // Verwijder leidende/volgende non-alfanumerieke tekens
                snippet = snippet.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
                String suffix = cleanedText.length() > snippet.length() ? "..." : ""; // Ellipsis alleen als echt ingekort

                if (!snippet.isBlank()) {
                    return snippet + suffix; // Return het schone tekstfragment
                }
            }
        }

        // 3. Terugval placeholder (verbeterd)
        return "Geen beschrijving automatisch beschikbaar voor: " + filename;
    }

    /**
     * Helper methode om te valideren of een metadata beschrijving bruikbaar is.
     */
    private static boolean isDescriptionValid(String description) {
        if (description == null || description.isBlank()) return false;
        String trimmed = description.trim();
        // Check op minimale/maximale lengte en vreemde codes
        return trimmed.length() >= ExtractorConfig.MIN_DESC_METADATA_LENGTH &&
                trimmed.length() <= ExtractorConfig.MAX_DESC_METADATA_LENGTH && // Voorkom extreem lange velden
                !trimmed.contains("_x000d_"); // Check op vreemde codes
    }


    // --- Statische Helper methoden voor formattering ---
    private static String cleanTitle(String title) {
        if (title == null || title.trim().isEmpty()) return null;
        String cleaned = ExtractorConfig.TITLE_PREFIX_PATTERN.matcher(title).replaceFirst("");
        cleaned = cleaned.replace('_', ' ');
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }
    private static void addExtra(List<Map<String, String>> extras, String key, String value) {
        if (value != null && !value.trim().isEmpty()) extras.add(Map.of("key", key, "value", value.trim()));
    }
    private static String getMetadataValue(Metadata metadata, org.apache.tika.metadata.Property property) {
        String value = metadata.get(property); return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }
    private static String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key); return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }
    private static Optional<String> formatIso8601DateTime(String dateTime) {
        return parseToInstant(dateTime).flatMap(DefaultCkanResourceFormatter::formatInstantToIso8601);
    }
    private static Optional<Instant> parseToInstant(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) return Optional.empty();
        List<DateTimeFormatter> formatters = Arrays.asList(DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_ZONED_DATE_TIME, DateTimeFormatter.ISO_INSTANT, DateTimeFormatter.ISO_LOCAL_DATE_TIME, DateTimeFormatter.ISO_LOCAL_DATE);
        for (DateTimeFormatter formatter : formatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME) return Optional.of(java.time.LocalDateTime.parse(dateTime, formatter).atZone(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE) return Optional.of(java.time.LocalDate.parse(dateTime, formatter).atStartOfDay(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_INSTANT) return Optional.of(Instant.parse(dateTime));
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME) return Optional.of(ZonedDateTime.parse(dateTime, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) return Optional.of(OffsetDateTime.parse(dateTime, formatter).toInstant());
            } catch (DateTimeParseException ignored) {}
        }
        System.err.println("Waarschuwing: Kon datum/tijd niet parsen: " + dateTime); return Optional.empty();
    }
    private static Optional<String> formatInstantToIso8601(Instant instant) {
        return Optional.ofNullable(instant).map(ExtractorConfig.ISO8601_FORMATTER::format);
    }
    private static String mapContentTypeToSimpleFormat(String contentType) {
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf"->"PDF"; case "application/vnd.openxmlformats-officedocument.wordprocessingml.document"->"DOCX";
                    case "application/msword"->"DOC"; case "application/vnd.openxmlformats-officedocument.presentationml.presentation"->"PPTX";
                    case "application/vnd.ms-powerpoint"->"PPT"; case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"->"XLSX";
                    case "application/vnd.ms-excel"->"XLS"; case "text/plain"->"TXT"; case "text/csv"->"CSV"; case "text/html"->"HTML";
                    case "application/rtf"->"RTF"; case "image/jpeg", "image/jpg"->"JPEG"; case "image/png"->"PNG"; case "image/gif"->"GIF";
                    case "image/tiff"->"TIFF"; case "image/bmp"->"BMP"; case "image/svg+xml"->"SVG"; case "application/zip"->"ZIP";
                    case "application/gzip"->"GZIP"; case "application/x-rar-compressed"->"RAR"; case "application/x-7z-compressed"->"7Z";
                    case "application/json"->"JSON"; case "application/xml", "text/xml"->"XML"; case "application/shp", "application/x-shapefile"->"SHP";
                    case "application/x-dbf"->"DBF"; case "application/vnd.google-earth.kml+xml"->"KML"; case "application/geopackage+sqlite3"->"GPKG";
                    case "application/geo+json"->"GEOJSON";
                    default->{int i=lowerType.lastIndexOf('/'); yield (i!=-1&&i<lowerType.length()-1)?lowerType.substring(i+1).toUpperCase().replaceAll("[^A-Z0-9]",""):lowerType.toUpperCase().replaceAll("[^A-Z0-9]","");}
                }).orElse("Unknown");
    }
    private static String mapContentTypeToIanaUri(String contentType) {
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf"->"https://www.iana.org/assignments/media-types/application/pdf";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document"->"https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                    case "application/msword"->"https://www.iana.org/assignments/media-types/application/msword";
                    case "text/plain"->"https://www.iana.org/assignments/media-types/text/plain"; case "text/csv"->"https://www.iana.org/assignments/media-types/text/csv";
                    case "image/jpeg"->"https://www.iana.org/assignments/media-types/image/jpeg"; case "image/png"->"https://www.iana.org/assignments/media-types/image/png";
                    case "image/gif"->"https://www.iana.org/assignments/media-types/image/gif"; case "application/zip"->"https://www.iana.org/assignments/media-types/application/zip";
                    case "application/json"->"https://www.iana.org/assignments/media-types/application/json"; case "application/xml"->"https://www.iana.org/assignments/media-types/application/xml";
                    case "text/xml"->"https://www.iana.org/assignments/media-types/text/xml"; case "application/shp", "application/x-shapefile"->"https://www.iana.org/assignments/media-types/application/vnd.shp";
                    case "application/x-dbf"->"https://www.iana.org/assignments/media-types/application/vnd.dbf";
                    default->ExtractorConfig.PLACEHOLDER_URI + "-iana-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                }).orElse(ExtractorConfig.PLACEHOLDER_URI + "-iana-unknown");
    }
    private static String mapContentTypeToEuFileTypeUri(String contentType) {
        final String EU_FT_BASE = "http://publications.europa.eu/resource/authority/file-type/";
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank()).map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf"->EU_FT_BASE+"PDF"; case "application/vnd.openxmlformats-officedocument.wordprocessingml.document"->EU_FT_BASE+"DOCX";
                    case "application/msword"->EU_FT_BASE+"DOC"; case "application/vnd.openxmlformats-officedocument.presentationml.presentation"->EU_FT_BASE+"PPTX";
                    case "application/vnd.ms-powerpoint"->EU_FT_BASE+"PPT"; case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"->EU_FT_BASE+"XLSX";
                    case "application/vnd.ms-excel"->EU_FT_BASE+"XLS"; case "text/plain"->EU_FT_BASE+"TXT"; case "text/csv"->EU_FT_BASE+"CSV";
                    case "image/jpeg","image/jpg"->EU_FT_BASE+"JPEG"; case "image/png"->EU_FT_BASE+"PNG"; case "image/gif"->EU_FT_BASE+"GIF";
                    case "image/tiff"->EU_FT_BASE+"TIFF"; case "application/zip"->EU_FT_BASE+"ZIP"; case "application/xml","text/xml"->EU_FT_BASE+"XML";
                    case "application/shp","application/x-shapefile"->EU_FT_BASE+"SHP"; case "application/x-dbf"->EU_FT_BASE+"DBF";
                    default->ExtractorConfig.PLACEHOLDER_URI + "-eu-filetype-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                }).orElse(ExtractorConfig.PLACEHOLDER_URI + "-eu-filetype-unknown");
    }
    // Instance method as it uses the languageDetector instance variable
    private Optional<LanguageResult> detectLanguage(String text, int maxSampleLength) {
        if (this.languageDetector == null || text == null || text.trim().isEmpty()) return Optional.empty();
        try {
            String textSample = text.substring(0, Math.min(maxSampleLength, text.length()));
            if (textSample.isBlank()) return Optional.empty();
            return Optional.of(this.languageDetector.detect(textSample));
        } catch (Exception e) {
            System.err.println("Waarschuwing: Fout tijdens taaldetectie: " + e.getMessage()); return Optional.empty();
        }
    }
    private static String mapLanguageCodeToNalUri(String langCode) {
        final String EU_LANG_BASE = "http://publications.europa.eu/resource/authority/language/";
        if (langCode == null || langCode.isBlank()) return EU_LANG_BASE + "UND";
        String normCode = langCode.toLowerCase().split("-")[0];
        return EU_LANG_BASE + switch (normCode) {
            case "nl" -> "NLD"; case "en" -> "ENG"; case "de" -> "DEU"; case "fr" -> "FRA";
            case "es" -> "SPA"; case "it" -> "ITA"; case "und" -> "UND"; default -> "MUL";
        };
    }
}

/**
 * Verwerkt ZIP-archieven, inclusief geneste archieven, en delegeert bestandsverwerking.
 */
class ZipSourceProcessor implements ISourceProcessor {
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final int maxTextLength;

    public ZipSourceProcessor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, int maxTextLength) {
        this.fileFilter = Objects.requireNonNull(fileFilter);
        this.metadataProvider = Objects.requireNonNull(metadataProvider);
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter);
        this.maxTextLength = maxTextLength;
    }

    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis);
             ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                // Voorkom Zip Slip vulnerability door entry naam te normaliseren en te checken
                Path currentPath = Paths.get(entryName).normalize();
                if (currentPath.startsWith("..") || currentPath.isAbsolute()) {
                    String errorMsg = "Ongeldige/onveilige entry naam overgeslagen: " + entryName;
                    errors.add(new ProcessingError(containerPath + "!/" + entryName, errorMsg));
                    System.err.println(errorMsg);
                    zis.closeEntry();
                    continue;
                }
                String fullEntryPath = containerPath + "!/" + entryName;

                if (entry.isDirectory()) {
                    zis.closeEntry(); continue;
                }
                if (!fileFilter.isFileTypeRelevant(entryName)) {
                    ignored.add(new IgnoredEntry(fullEntryPath, "Bestandstype genegeerd door filter"));
                    System.err.println("Negeer entry gebaseerd op filter: " + fullEntryPath);
                    zis.closeEntry(); continue;
                }

                if (isZipEntryZip(entry)) {
                    processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                } else {
                    processRegularEntry(entryName, zis, fullEntryPath, results, errors);
                }
                zis.closeEntry();
            }
        } catch (IOException e) {
            String errorMsg = "Fout bij lezen ZIP-bestand '" + zipPath + "': " + e.getMessage();
            errors.add(new ProcessingError(containerPath, errorMsg));
            System.err.println(errorMsg);
        }
    }

    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("Verwerk geneste ZIP: " + fullEntryPath);
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) {
            String errorMsg = "Kon geneste zip niet verwerken: " + e.getMessage();
            errors.add(new ProcessingError(fullEntryPath, errorMsg));
            System.err.println("Fout bij verwerken geneste ZIP entry '" + fullEntryPath + "': " + e.getMessage());
        } finally {
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) {
                    System.err.println("Waarschuwing: Kon tijdelijk bestand niet verwijderen '" + tempZip + "': " + e.getMessage());
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
            System.err.println("Fout bij verwerken entry '" + entryName + "': " + e.getMessage());
        }
    }

    private boolean isZipEntryZip(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) return false;
        String lowerCaseName = entry.getName().toLowerCase();
        return ExtractorConfig.SUPPORTED_ZIP_EXTENSIONS.stream().anyMatch(lowerCaseName::endsWith);
    }
}

// --- Facade Klasse ---

/**
 * Hoofdingang voor metadata-extractie, orkestreert de verschillende componenten.
 */
public class MetadataExtractor { // Hernoemd van MetadataExtractorFacade voor eenvoud

    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ISourceProcessor sourceProcessor;
    private final int maxTextLength;

    /** Constructor voor dependency injection. */
    public MetadataExtractor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ISourceProcessor sourceProcessor, int maxTextLength) {
        this.fileFilter = Objects.requireNonNull(fileFilter);
        this.metadataProvider = Objects.requireNonNull(metadataProvider);
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter);
        this.sourceProcessor = Objects.requireNonNull(sourceProcessor);
        this.maxTextLength = maxTextLength;
    }

    /**
     * Verwerkt een gegeven bronpad (bestand of ZIP) en retourneert een rapport.
     * Vangt de meeste IOExceptions intern af en rapporteert ze.
     * @param sourcePathString Pad naar het bronbestand of ZIP-archief.
     * @return Een ProcessingReport met resultaten, fouten en genegeerde entries.
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> successfulResults = new ArrayList<>();
        List<ProcessingError> processingErrors = new ArrayList<>();
        List<IgnoredEntry> ignoredEntries = new ArrayList<>();
        Path sourcePath = null;

        try {
            sourcePath = Paths.get(sourcePathString).normalize();

            if (!Files.exists(sourcePath)) {
                processingErrors.add(new ProcessingError(sourcePathString, "Bron niet gevonden"));
                System.err.println("Fout: Bron niet gevonden: " + sourcePathString);
                return new ProcessingReport(successfulResults, processingErrors, ignoredEntries);
            }

            if (Files.isDirectory(sourcePath)) {
                ignoredEntries.add(new IgnoredEntry(sourcePathString, "Bron is een map (niet verwerkt)"));
                System.err.println("Bron is een map, overgeslagen: " + sourcePathString);
            } else if (isZipFile(sourcePath)) {
                sourceProcessor.processSource(sourcePath, sourcePath.toString(), successfulResults, processingErrors, ignoredEntries);
            } else {
                String filename = sourcePath.getFileName().toString();
                if (fileFilter.isFileTypeRelevant(filename)) {
                    try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
                        IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, maxTextLength);
                        CkanResource resource = resourceFormatter.format(filename, output.metadata, output.text, sourcePath.toString());
                        successfulResults.add(resource);
                    } catch (Exception e) {
                        processingErrors.add(new ProcessingError(sourcePathString, e.getClass().getSimpleName() + ": " + e.getMessage()));
                        System.err.println("Fout bij verwerken enkel bestand '" + sourcePath + "': " + e.getMessage());
                    }
                } else {
                    ignoredEntries.add(new IgnoredEntry(sourcePathString, "Bestandstype genegeerd door filter"));
                    System.err.println("Negeer bestand gebaseerd op filter: " + filename);
                }
            }
        } catch (Exception e) {
            String errorMsg = "Onverwachte kritieke fout tijdens verwerking '" + sourcePathString + "': " + e.getMessage();
            processingErrors.add(new ProcessingError(sourcePathString, errorMsg));
            System.err.println(errorMsg);
            e.printStackTrace(System.err);
        }

        return new ProcessingReport(successfulResults, processingErrors, ignoredEntries);
    }

    private static boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) return false;
        String fileName = path.getFileName().toString().toLowerCase();
        return ExtractorConfig.SUPPORTED_ZIP_EXTENSIONS.stream().anyMatch(fileName::endsWith);
    }

    // --- Main Methode voor Demonstratie ---
    public static void main(String[] args) {
        String filePath = "C:\\Users\\gurelb\\Downloads\\Veg kartering - habitatkaart 2021-2023.zip"; // Voorbeeld pad

        if (args.length > 0) filePath = args[0];
        else System.err.println("Waarschuwing: Geen bestandspad opgegeven, gebruik standaard: " + filePath);

        // --- Dependency Setup ---
        LanguageDetector languageDetector = null;
        try {
            languageDetector = OptimaizeLangDetector.getDefaultLanguageDetector().loadModels();
        } catch (IOException e) { System.err.println("Waarschuwing: Kon taalmodellen niet laden: " + e.getMessage()); }

        int maxTextLength = ExtractorConfig.MAX_EXTRACTED_TEXT_LENGTH;

        IFileTypeFilter filter = new DefaultFileTypeFilter();
        IMetadataProvider provider = new TikaMetadataProvider();
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormatter(languageDetector);
        ISourceProcessor zipProcessor = new ZipSourceProcessor(filter, provider, formatter, maxTextLength);

        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, zipProcessor, maxTextLength);

        // --- Uitvoering & Output ---
        try {
            ProcessingReport report = extractor.processSource(filePath);

            ObjectMapper jsonMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            String jsonOutput = jsonMapper.writeValueAsString(report);
            System.out.println(jsonOutput);

            if (!report.getErrors().isEmpty()) {
                System.err.println("\nVerwerking voltooid met " + report.getErrors().size() + " fout(en).");
            }
            if (!report.getIgnored().isEmpty()) {
                System.err.println(report.getIgnored().size() + " bestand(en) genegeerd.");
            }

        } catch (Exception e) {
            System.err.println("Kritieke fout in main: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

} // Einde van MetadataExtractor
