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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MetadataExtractor {

    private final DateTimeFormatter iso8601Formatter = DateTimeFormatter.ISO_INSTANT;
    private final String placeholderUri = "urn:placeholder:vervang-mij";
    private final LanguageDetector languageDetector;

    public MetadataExtractor() {
        this.languageDetector = initializeLanguageDetector();
    }

    private LanguageDetector initializeLanguageDetector() {
        try {
            return OptimaizeLangDetector.getDefaultLanguageDetector().loadModels();
        } catch (IOException e) {
            System.err.println("Waarschuwing: Taaldetectie niet geïnitialiseerd. " + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) {
        String filePath = "C:\\Users\\gurelb\\Downloads\\datawarehouse\\Veg kartering - habitatkaart 2021-2023\\Veg kartering - habitatkaart 2021-2023\\PZH_WestduinparkEnWapendal_2023_HabitatRapport.pdf";
        MetadataExtractor extractor = new MetadataExtractor();
        try {
            String jsonOutput = extractor.extractMetadataToJson(filePath);
            System.out.println(jsonOutput);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String extractMetadataToJson(String filePath) throws IOException, TikaException, SAXException {
        Metadata metadata = extractMetadata(filePath);
        String text = extractText(filePath);
        Map<String, Object> jsonData = createJsonData(filePath, metadata, text);
        return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(jsonData);
    }

    private Metadata extractMetadata(String filePath) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        try (InputStream stream = Files.newInputStream(Paths.get(filePath))) {
            parser.parse(stream, new BodyContentHandler(-1), metadata, new ParseContext());
        }
        return metadata;
    }

    private String extractText(String filePath) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler(-1);
        try (InputStream stream = Files.newInputStream(Paths.get(filePath))) {
            parser.parse(stream, handler, new Metadata(), new ParseContext());
        }
        return handler.toString();
    }

    private Map<String, Object> createJsonData(String filePath, Metadata metadata, String text) {
        Map<String, Object> data = new HashMap<>();
        data.put("analyse_bronBestand", filePath);
        addMetadataToMap(data, metadata, DublinCore.TITLE, "dct:title");
        addMetadataToMap(data, metadata, DublinCore.CREATOR, "dct:creator");
        addMetadataToMap(data, metadata, TikaCoreProperties.CREATOR_TOOL, "tika:creatorTool");
        addMetadataToMap(data, metadata, "producer", "tika:producer");
        addMetadataToMap(data, metadata, Metadata.CONTENT_TYPE, "dct:format");
        data.put("dct:issued", formatIso8601DateTime(metadata.get(TikaCoreProperties.CREATED)).orElse(null));
        data.put("dct:modified", formatIso8601DateTime(metadata.get(TikaCoreProperties.MODIFIED)).orElse(null));
        data.put("dcat:mediaType", mapContentTypeToIanaUri(metadata.get(Metadata.CONTENT_TYPE)));
        data.put("dct:format_uri", mapContentTypeToEuFileTypeUri(metadata.get(Metadata.CONTENT_TYPE)));

        Optional.ofNullable(detectLanguage(text))
                .filter(LanguageResult::isReasonablyCertain)
                .map(LanguageResult::getLanguage)
                .ifPresentOrElse(
                        lang -> data.put("dct:language", mapLanguageCodeToNalUri(lang)),
                        () -> data.put("dct:language", mapLanguageCodeToNalUri("und"))
                );

        String[] subjects = metadata.getValues(DublinCore.SUBJECT);
        data.put("dcat:keyword", subjects != null ? Arrays.asList(subjects) : null);

        addMetadataToMap(data, metadata, "pdf:PDFVersion", "pdf:PDFVersion");
        addMetadataToMap(data, metadata, "xmpTPg:NPages", "xmpTPg:NPages");
        return data;
    }

    private void addMetadataToMap(Map<String, Object> data, Metadata metadata, Object key, String mapKey) {
        String value = (key instanceof org.apache.tika.metadata.Property)
                ? getMetadataValue(metadata, (org.apache.tika.metadata.Property) key)
                : getMetadataValue(metadata, (String) key);
        if (value != null) {
            data.put(mapKey, value);
        }
    }

    private String getMetadataValue(Metadata metadata, org.apache.tika.metadata.Property property) {
        String value = metadata.get(property);
        return value != null && !value.trim().isEmpty() ? value.trim() : null;
    }

    private String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key);
        return value != null && !value.trim().isEmpty() ? value.trim() : null;
    }

    private Optional<String> formatIso8601DateTime(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) return Optional.empty();
        try {
            TemporalAccessor temporal = parseDateTime(dateTime);
            Instant instant = (temporal instanceof Instant) ? (Instant) temporal : ZonedDateTime.of(java.time.LocalDateTime.from(temporal), ZoneId.systemDefault()).toInstant();
            return Optional.of(iso8601Formatter.format(instant));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private TemporalAccessor parseDateTime(String dateTime) throws DateTimeParseException {
        try {
            return OffsetDateTime.parse(dateTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException e1) {
            try {
                return ZonedDateTime.parse(dateTime, DateTimeFormatter.ISO_ZONED_DATE_TIME);
            } catch (DateTimeParseException e2) {
                try {
                    return Instant.parse(dateTime);
                } catch (DateTimeParseException e3) {
                    try {
                        return java.time.LocalDateTime.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    } catch (DateTimeParseException e4) {
                        return java.time.LocalDate.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE);
                    }
                }
            }
        }
    }

    private String mapContentTypeToIanaUri(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf" -> "https://www.iana.org/assignments/media-types/application/pdf";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                    case "application/msword" -> "https://www.iana.org/assignments/media-types/application/msword";
                    case "text/plain" -> "https://www.iana.org/assignments/media-types/text/plain";
                    case "text/csv" -> "https://www.iana.org/assignments/media-types/text/csv";
                    case "image/jpeg", "image/jpg" -> "https://www.iana.org/assignments/media-types/image/jpeg";
                    case "image/png" -> "https://www.iana.org/assignments/media-types/image/png";
                    case "image/gif" -> "https://www.iana.org/assignments/media-types/image/gif";
                    default -> placeholderUri + "-iana-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                })
                .orElse(placeholderUri);
    }

    private String mapContentTypeToEuFileTypeUri(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    case "application/pdf" -> "http://publications.europa.eu/resource/authority/file-type/PDF";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "http://publications.europa.eu/resource/authority/file-type/DOCX";
                    case "application/msword" -> "http://publications.europa.eu/resource/authority/file-type/DOC";
                    case "text/plain" -> "http://publications.europa.eu/resource/authority/file-type/TXT";
                    case "text/csv" -> "http://publications.europa.eu/resource/authority/file-type/CSV";
                    case "image/jpeg", "image/jpg" -> "http://publications.europa.eu/resource/authority/file-type/JPEG";
                    case "image/png" -> "http://publications.europa.eu/resource/authority/file-type/PNG";
                    case "image/gif" -> "http://publications.europa.eu/resource/authority/file-type/GIF";
                    default -> placeholderUri + "-eu-filetype-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
                })
                .orElse(placeholderUri);
    }

    private String mapLanguageCodeToNalUri(String langCode) {
        return Optional.ofNullable(langCode)
                .filter(l -> !l.isBlank())
                .map(String::toLowerCase)
                .map(lowerLangCode -> switch (lowerLangCode) {
                    case "nl" -> "http://publications.europa.eu/resource/authority/language/NLD";
                    case "en" -> "http://publications.europa.eu/resource/authority/language/ENG";
                    case "de" -> "http://publications.europa.eu/resource/authority/language/DEU";
                    case "fr" -> "http://publications.europa.eu/resource/authority/language/FRA";
                    default -> "http://publications.europa.eu/resource/authority/language/MUL";
                })
                .orElse("http://publications.europa.eu/resource/authority/language/UND");
    }

    private LanguageResult detectLanguage(String text) {
        return Optional.ofNullable(languageDetector)
                .filter(detector -> text != null && !text.trim().isEmpty())
                .map(detector -> {
                    try {
                        return detector.detect(text.substring(0, Math.min(10000, text.length())));
                    } catch (Exception e) {
                        System.err.println("Waarschuwing: Fout bij taaldetectie: " + e.getMessage());
                        return null;
                    }
                })
                .orElse(null);
    }
}