import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts extracted metadata and text into a CKAN resource.
 */
public class DefaultCkanResourceFormat implements ICkanResourceFormatter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCkanResourceFormat.class);

    private static final Map<String, String> FORMAT_MAP = Map.ofEntries(
            Map.entry("application/pdf", "PDF"),
            Map.entry("application/msword", "DOC"),
            Map.entry("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "DOCX"),
            Map.entry("application/vnd.ms-excel", "XLS"),
            Map.entry("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "XLSX"),
            Map.entry("application/vnd.ms-powerpoint", "PPT"),
            Map.entry("application/vnd.openxmlformats-officedocument.presentationml.presentation", "PPTX"),
            Map.entry("text/plain", "TXT"),
            Map.entry("text/csv", "CSV"),
            Map.entry("application/xml", "XML"),
            Map.entry("text/xml", "XML"),
            Map.entry("text/html", "HTML"),
            Map.entry("image/jpeg", "JPEG"),
            Map.entry("image/png", "PNG"),
            Map.entry("image/gif", "GIF"),
            Map.entry("image/tiff", "TIFF"),
            Map.entry("application/zip", "ZIP"),
            Map.entry("application/geo+json", "GEOJSON"),
            Map.entry("application/vnd.geo+json", "GEOJSON"),
            Map.entry("application/geopackage+sqlite3", "GPKG"),
            Map.entry("application/x-sqlite3", "GPKG")
    );

    private static final Map<String, String> LANG_MAP = Map.of(
            "nl", "NLD",
            "en", "ENG",
            "de", "DEU",
            "fr", "FRA",
            "es", "SPA",
            "it", "ITA"
    );

    private final LanguageDetector detector;
    private final ExtractorConfiguration cfg;
    private final String packageId;

    public DefaultCkanResourceFormat(
            LanguageDetector detector,
            ExtractorConfiguration cfg,
            String packageId
    ) {
        this.detector = detector;
        this.cfg = Objects.requireNonNull(cfg, "ExtractorConfiguration must not be null");
        this.packageId = Objects.requireNonNull(packageId, "packageId must not be null");
    }

    @Override
    public CkanResource format(
            String entryName,
            Metadata metadata,
            String text,
            String srcId
    ) {
        Map<String, Object> data = new LinkedHashMap<>();
        Map<String, String> extras = new LinkedHashMap<>();

        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);

        // Core fields
        data.put("package_id", packageId);
        data.put("upload", toRelative(srcId));

        // Name: cleaned title > raw title > filename
        String rawTitle = getValue(metadata, TikaCoreProperties.TITLE);
        String cleaned  = cleanTitle(rawTitle, cfg.getTitlePrefixPattern());
        String name     = Optional.ofNullable(cleaned)
                .or(() -> Optional.ofNullable(rawTitle))
                .orElse(filename);
        data.put("name", name);

        // Description
        data.put("description", makeDescription(metadata, text, filename));

        // Format & mimetype
        String contentType = getValue(metadata, "Content-Type");
        data.put("format", mapFormat(contentType));
        data.put("mimetype",
                Optional.ofNullable(contentType)
                        .map(s -> s.split(";")[0].trim())
                        .filter(s -> !s.isBlank())
                        .orElse(null)
        );

        // Created timestamp
        parseInstant(metadata.get(TikaCoreProperties.CREATED))
                .flatMap(this::formatInstant)
                .ifPresent(ts -> data.put("created", ts));

        // Last-modified timestamp
        String modifiedRaw = Optional.ofNullable(metadata.get(TikaCoreProperties.MODIFIED))
                .orElse(metadata.get(DublinCore.MODIFIED));
        parseInstant(modifiedRaw)
                .flatMap(this::formatInstant)
                .ifPresent(ts -> data.put("last_modified", ts));

        // Extras
        putIfPresent(extras, "source_identifier",  srcId);
        putIfPresent(extras, "original_entry_name", entryName);
        putIfPresent(extras, "original_filename",   filename);
        putIfPresent(extras, "creator",             getValue(metadata, TikaCoreProperties.CREATOR));

        // Language detection
        Optional.ofNullable(detector)
                .flatMap(d -> detectLanguage(d, text, cfg.getMaxTextSampleLength()))
                .filter(LanguageResult::isReasonablyCertain)
                .map(LanguageResult::getLanguage)
                .map(this::toLanguageUri)
                .ifPresent(uri -> extras.put("language_uri", uri));

        if (!extras.isEmpty()) {
            data.put("extras", extras);
        }

        return new CkanResource(data);
    }

    // —— Helper methods —— //

    /**
     * Converts an absolute source path into a relative one based on "MetaData\" marker.
     */
    private String toRelative(String fullPath) {
        final String marker = "MetaData\\";
        int idx = fullPath.indexOf(marker);
        if (idx >= 0) {
            String remainder = fullPath.substring(idx + marker.length());
            return ".\\" + remainder;
        }
        return fullPath;
    }

    /** Safely retrieve and trim a metadata value by Property key */
    private static String getValue(Metadata m, Property p) {
        return trim(m.get(p));
    }

    /** Safely retrieve and trim a metadata value by String key */
    private static String getValue(Metadata m, String key) {
        return trim(m.get(key));
    }

    private static String trim(String s) {
        return (s != null && !s.isBlank()) ? s.trim() : null;
    }

    /** Put only non-blank values into extras */
    private static void putIfPresent(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value.trim());
        }
    }

    /** Remove unwanted prefix/suffix from a title */
    private static String cleanTitle(String title, Pattern prefixPattern) {
        if (title == null || title.isBlank()) {
            return null;
        }
        String cleaned = prefixPattern.matcher(title)
                .replaceFirst("")
                .replace('_', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        return cleaned.isBlank() ? null : cleaned;
    }

    /** Try several ISO date/time formats to parse into Instant */
    private static Optional<Instant> parseInstant(String dt) {
        if (dt == null || dt.isBlank()) {
            return Optional.empty();
        }
        List<Supplier<Optional<Instant>>> strategies = List.of(
                () -> tryParse(() -> OffsetDateTime.parse(dt).toInstant()),
                () -> tryParse(() -> ZonedDateTime.parse(dt).toInstant()),
                () -> tryParse(() -> Instant.parse(dt)),
                () -> tryParse(() -> LocalDateTime.parse(dt).atZone(ZoneId.systemDefault()).toInstant()),
                () -> tryParse(() -> LocalDate.parse(dt).atStartOfDay(ZoneId.systemDefault()).toInstant())
        );
        return strategies.stream()
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private static Optional<Instant> tryParse(Supplier<Instant> supplier) {
        try {
            return Optional.of(supplier.get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Format an Instant with the configured ISO‐8601 formatter */
    private Optional<String> formatInstant(Instant instant) {
        DateTimeFormatter fmt = Objects.requireNonNull(cfg.getIso8601Formatter(),
                "ISO8601 formatter must not be null");
        return Optional.ofNullable(instant)
                .map(fmt::format);
    }

    /** Map MIME type to CKAN format code, defaulting to file‐extension fallback */
    private static String mapFormat(String contentType) {
        if (contentType == null || contentType.isBlank()) {
            return "Unknown";
        }
        String key = contentType.toLowerCase()
                .split(";")[0]
                .trim();
        return FORMAT_MAP.getOrDefault(
                key,
                key.contains("/")
                        ? key.substring(key.lastIndexOf('/') + 1)
                        .toUpperCase()
                        .replaceAll("[^A-Z0-9]", "")
                        : key.toUpperCase()
        );
    }

    /** Build language URI from ISO code */
    private String toLanguageUri(String code) {
        String tag = Optional.ofNullable(code)
                .map(String::toLowerCase)
                .map(c -> c.split("-")[0])
                .filter(c -> !c.equals("und"))
                .flatMap(c -> Optional.ofNullable(LANG_MAP.get(c)))
                .orElse("UND");
        return "http://publications.europa.eu/resource/authority/language/" + tag;
    }

    /** Detect language with a text sample, logging any failures */
    private Optional<LanguageResult> detectLanguage(
            LanguageDetector detector,
            String text,
            int maxLen
    ) {
        try {
            if (text != null && !text.isBlank()) {
                String sample = text.substring(0, Math.min(text.length(), maxLen)).trim();
                if (!sample.isEmpty()) {
                    return Optional.of(detector.detect(sample));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Language detection failed: {}", e.getMessage(), e);
        }
        return Optional.empty();
    }

    /**
     * Build a short description:
     * 1) use metadata if valid, cropping to max length
     * 2) otherwise generate from text sample
     * 3) fallback to a default message
     */
    private String makeDescription(Metadata m, String text, String filename) {
        String desc = getValue(m, DublinCore.DESCRIPTION);
        if (!isValid(desc)) {
            desc = getValue(m, TikaCoreProperties.DESCRIPTION);
        }

        if (isValid(desc)) {
            String d = desc.trim().replaceAll("\\s+", " ");
            int max = cfg.getMaxAutoDescriptionLength();
            if (d.length() > max) {
                int cut = Math.max(d.lastIndexOf(' ', max - 3), max / 2);
                d = d.substring(0, cut > 0 ? cut : max - 3).trim() + "...";
            }
            d = d.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!d.isEmpty()) {
                return d;
            }
        }

        if (text != null && text.strip().length() > 10) {
            String t = text.trim().replaceAll("\\s+", " ");
            int end = Math.min(t.length(), cfg.getMaxAutoDescriptionLength());
            String snippet = t.substring(0, end)
                    .replaceAll("^[\\W_]+|[\\W_]+$", "")
                    .trim();
            if (!snippet.isEmpty()) {
                return snippet + (t.length() > end && snippet.length() <= cfg.getMaxAutoDescriptionLength() - 3
                        ? "..."
                        : "");
            }
        }

        return "No description available for: " + filename;
    }

    private boolean isValid(String s) {
        if (s == null || s.isBlank()) {
            return false;
        }
        String t = s.trim();
        return t.length() >= cfg.getMinDescMetadataLength()
                && t.length() <= cfg.getMaxDescMetadataLength()
                && !t.contains("_x000d_")
                && !t.equalsIgnoreCase("untitled")
                && !t.equalsIgnoreCase("no title")
                && !t.matches("(?i)^\\s*template\\s*$");
    }
}
