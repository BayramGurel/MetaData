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

public class DefaultCkanResourceFormat implements ICkanResourceFormatter {
    private final String packageId;
    private static final Map<String, String> FORMAT_MAP = Map.ofEntries(Map.entry("application/pdf", "PDF"), Map.entry("application/msword", "DOC"), Map.entry("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "DOCX"), Map.entry("application/vnd.ms-excel", "XLS"), Map.entry("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "XLSX"), Map.entry("application/vnd.ms-powerpoint", "PPT"), Map.entry("application/vnd.openxmlformats-officedocument.presentationml.presentation", "PPTX"), Map.entry("text/plain", "TXT"), Map.entry("text/csv", "CSV"), Map.entry("application/xml", "XML"), Map.entry("text/xml", "XML"), Map.entry("text/html", "HTML"), Map.entry("image/jpeg", "JPEG"), Map.entry("image/png", "PNG"), Map.entry("image/gif", "GIF"), Map.entry("image/tiff", "TIFF"), Map.entry("application/zip", "ZIP"), Map.entry("application/geo+json", "GEOJSON"), Map.entry("application/vnd.geo+json", "GEOJSON"), Map.entry("application/geopackage+sqlite3", "GPKG"), Map.entry("application/x-sqlite3", "GPKG"));
    private static final Map<String, String> LANG_MAP = Map.of("nl", "NLD", "en", "ENG", "de", "DEU", "fr", "FRA", "es", "SPA", "it", "ITA");
    private final LanguageDetector detector;
    private final ExtractorConfiguration cfg;

    public DefaultCkanResourceFormat(LanguageDetector detector, ExtractorConfiguration cfg, String packageId) {
        this.detector = detector;
        this.cfg = (ExtractorConfiguration)Objects.requireNonNull(cfg, "Configuratie mag niet null zijn");
        this.packageId = (String)Objects.requireNonNull(packageId, "packageId mag niet null zijn");
    }

    public CkanResource format(String entryName, Metadata m, String text, String srcId) {
        LinkedHashMap<String, Object> data = new LinkedHashMap();
        LinkedHashMap<String, String> extras = new LinkedHashMap();
        String fn = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        data.put("package_id", this.packageId);
        data.put("upload", this.toRelative(srcId));
        String title = val(m, TikaCoreProperties.TITLE);
        String clean = cleanTitle(title, this.cfg.getTitlePrefixPattern());
        data.put("name", Optional.ofNullable(clean).or(() -> Optional.ofNullable(title)).orElse(fn));
        data.put("description", this.makeDescription(m, text, fn));
        String ct = val(m, "Content-Type");
        data.put("format", mapFormat(ct));
        data.put("mimetype", Optional.ofNullable(ct).map(s -> s.split(";")[0].trim()).filter(s -> !s.isBlank()).orElse(null));        parse(m.get(TikaCoreProperties.CREATED)).flatMap(this::fmt).ifPresent((d) -> data.put("created", d));
        String modRaw = (String)Optional.ofNullable(m.get(TikaCoreProperties.MODIFIED)).orElse(m.get(DublinCore.MODIFIED));
        parse(modRaw).flatMap(this::fmt).ifPresent((d) -> data.put("last_modified", d));
        put(extras, "source_identifier", srcId);
        put(extras, "original_entry_name", entryName);
        put(extras, "original_filename", fn);
        put(extras, "creator", val(m, TikaCoreProperties.CREATOR));
        Optional.ofNullable(this.detector).flatMap((d) -> this.detect(d, text, this.cfg.getMaxTextSampleLength())).filter(LanguageResult::isReasonablyCertain).map(LanguageResult::getLanguage).or(() -> Optional.of("und")).map(this::toLangUri).ifPresent((u) -> extras.put("language_uri", u));
        if (!extras.isEmpty()) {
            data.put("extras", extras);
        }

        return new CkanResource(data);
    }

    private String toRelative(String fullPath) {
        String marker = "MetaData\\";
        int idx = fullPath.indexOf(marker);
        if (idx >= 0) {
            String var10000 = fullPath.substring(idx + marker.length());
            return ".\\" + var10000;
        } else {
            return fullPath;
        }
    }

    private static String val(Metadata m, Property p) {
        return trim(m.get(p));
    }

    private static String val(Metadata m, String key) {
        return trim(m.get(key));
    }

    private static String trim(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }

    private static void put(Map<String, String> x, String k, String v) {
        if (v != null && !v.isBlank()) {
            x.put(k, v.trim());
        }

    }

    private static String cleanTitle(String t, Pattern p) {
        if (t != null && !t.isBlank()) {
            String s = p.matcher(t).replaceFirst("").replace('_', ' ').replaceAll("\\s+", " ").trim();
            return s.isBlank() ? null : s;
        } else {
            return null;
        }
    }

    private static Optional<Instant> parse(String dt) {
        if (dt == null || dt.isBlank()) {
            return Optional.empty();
        }

        try {
            return Optional.of(OffsetDateTime.parse(dt).toInstant());
        } catch (Exception ignored) { }

        try {
            return Optional.of(ZonedDateTime.parse(dt).toInstant());
        } catch (Exception ignored) { }

        try {
            return Optional.of(Instant.parse(dt));
        } catch (Exception ignored) { }

        try {
            return Optional.of(LocalDateTime.parse(dt)
                    .atZone(ZoneId.systemDefault())
                    .toInstant());
        } catch (Exception ignored) { }

        try {
            return Optional.of(LocalDate.parse(dt)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant());
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private Optional<String> fmt(Instant instant) {
        DateTimeFormatter formatter = Objects.requireNonNull(cfg.getIso8601Formatter());
        return Optional.ofNullable(instant)
                .map(formatter::format);
    }

    private static String mapFormat(String ct) {
        if (ct != null && !ct.isBlank()) {
            String key = ct.toLowerCase().split(";")[0].trim();
            return (String)FORMAT_MAP.getOrDefault(key, key.contains("/") ? key.substring(key.lastIndexOf(47) + 1).toUpperCase().replaceAll("[^A-Z0-9]", "") : key.toUpperCase());
        } else {
            return "Unknown";
        }
    }

    private String toLangUri(String code) {
        String tag = code != null && !code.isBlank() && !code.equalsIgnoreCase("und") ? (String)LANG_MAP.getOrDefault(code.toLowerCase().split("-")[0], "MUL") : "UND";
        return "http://publications.europa.eu/resource/authority/language/" + tag;
    }

    private Optional<LanguageResult> detect(LanguageDetector d, String txt, int maxLen) {
        try {
            if (txt != null && !txt.isBlank()) {
                String s = txt.substring(0, Math.min(txt.length(), maxLen)).trim();
                return s.isEmpty() ? Optional.empty() : Optional.of(d.detect(s));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            System.err.println("Waarschuwing: Taaldetectie mislukt: " + e.getMessage());
            return Optional.empty();
        }
    }

    private String makeDescription(Metadata m, String txt, String fn) {
        String desc = val(m, DublinCore.DESCRIPTION);
        if (!this.valid(desc)) {
            desc = val(m, TikaCoreProperties.DESCRIPTION);
        }

        if (this.valid(desc)) {
            String d = desc.trim().replaceAll("\\s+", " ");
            if (d.length() > this.cfg.getMaxAutoDescriptionLength()) {
                int end = this.cfg.getMaxAutoDescriptionLength();
                int sp = d.lastIndexOf(32, end - 3);
                String var10000 = sp > end / 2 ? d.substring(0, sp) : d.substring(0, end - 3);
                d = var10000.trim() + "...";
            }

            d = d.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!d.isEmpty()) {
                return d;
            }
        }

        if (txt != null && txt.strip().length() > 10) {
            String t = txt.trim().replaceAll("\\s+", " ");
            int end = Math.min(t.length(), this.cfg.getMaxAutoDescriptionLength());
            String sn = t.substring(0, end).replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!sn.isEmpty()) {
                return sn + (t.length() > end && sn.length() <= this.cfg.getMaxAutoDescriptionLength() - 3 ? "..." : "");
            }
        }

        return "Geen beschrijving beschikbaar voor: " + fn;
    }

    private boolean valid(String s) {
        if (s != null && !s.isBlank()) {
            String t = s.trim();
            return t.length() >= this.cfg.getMinDescMetadataLength() && t.length() <= this.cfg.getMaxDescMetadataLength() && !t.contains("_x000d_") && !t.equalsIgnoreCase("untitled") && !t.equalsIgnoreCase("no title") && !t.matches("(?i)^\\s*template\\s*$");
        } else {
            return false;
        }
    }
}
