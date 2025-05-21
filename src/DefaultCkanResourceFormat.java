import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.*;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class DefaultCkanResourceFormat implements ICkanResourceFormatter {
    private static final Map<String, String> FORMAT_MAP = Map.ofEntries(
            Map.entry("application/pdf","PDF"),
            Map.entry("application/msword","DOC"),
            Map.entry("application/vnd.openxmlformats-officedocument.wordprocessingml.document","DOCX"),
            Map.entry("application/vnd.ms-excel","XLS"),
            Map.entry("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet","XLSX"),
            Map.entry("application/vnd.ms-powerpoint","PPT"),
            Map.entry("application/vnd.openxmlformats-officedocument.presentationml.presentation","PPTX"),
            Map.entry("text/plain","TXT"),
            Map.entry("text/csv","CSV"),
            Map.entry("application/xml","XML"),
            Map.entry("text/xml","XML"),
            Map.entry("text/html","HTML"),
            Map.entry("image/jpeg","JPEG"),
            Map.entry("image/png","PNG"),
            Map.entry("image/gif","GIF"),
            Map.entry("image/tiff","TIFF"),
            Map.entry("application/zip","ZIP"),
            Map.entry("application/geo+json","GEOJSON"),
            Map.entry("application/vnd.geo+json","GEOJSON"),
            Map.entry("application/geopackage+sqlite3","GPKG"),
            Map.entry("application/x-sqlite3","GPKG")
    );
    private static final Map<String, String> LANG_MAP = Map.of(
            "nl","NLD","en","ENG","de","DEU","fr","FRA","es","SPA","it","ITA"
    );

    private final LanguageDetector detector;
    private final ExtractorConfiguration cfg;

    public DefaultCkanResourceFormat(LanguageDetector detector, ExtractorConfiguration cfg) {
        this.detector = detector;
        this.cfg      = Objects.requireNonNull(cfg, "Configuratie mag niet null zijn");
    }

    @Override
    public CkanResource format(String entryName, Metadata m, String text, String srcId) {
        var data   = new LinkedHashMap<String, Object>();
        var extras = new LinkedHashMap<String, String>();
        String fn  = AbstractSourceProcessor.getFilenameFromEntry(entryName);

        // Core fields
        data.put("package_id", "PLACEHOLDER_PACKAGE_ID");
        // ipv URL: geef hier de lokale bestands-identifier mee; bij upload map je dit naar multipart-field 'upload'
        data.put("upload", toRelative(srcId));

        String title = val(m, TikaCoreProperties.TITLE);
        String clean = cleanTitle(title, cfg.getTitlePrefixPattern());
        data.put("name", Optional.ofNullable(clean)
                .or(() -> Optional.ofNullable(title))
                .orElse(fn));
        data.put("description", makeDescription(m, text, fn));

        String ct = val(m, Metadata.CONTENT_TYPE);
        data.put("format",   mapFormat(ct));
        data.put("mimetype", Optional.ofNullable(ct)
                .map(s -> s.split(";")[0].trim())
                .filter(s -> !s.isBlank())
                .orElse(null));

        // Datums
        parse(m.get(TikaCoreProperties.CREATED))
                .flatMap(this::fmt)
                .ifPresent(d -> data.put("created", d));

        String modRaw = Optional.ofNullable(m.get(TikaCoreProperties.MODIFIED))
                .orElse(m.get(DublinCore.MODIFIED));
        parse(modRaw)
                .flatMap(this::fmt)
                .ifPresent(d -> data.put("last_modified", d));

        // Extras
        put(extras, "source_identifier",   srcId);
        put(extras, "original_entry_name", entryName);
        put(extras, "original_filename",   fn);
        put(extras, "creator",             val(m, TikaCoreProperties.CREATOR));

        Optional.ofNullable(detector)
                .flatMap(d -> detect(d, text, cfg.getMaxTextSampleLength()))
                .filter(LanguageResult::isReasonablyCertain)
                .map(LanguageResult::getLanguage)
                .or(() -> Optional.of("und"))
                .map(this::toLangUri)
                .ifPresent(u -> extras.put("language_uri", u));

        if (!extras.isEmpty()) data.put("extras", extras);
        return new CkanResource(data);
    }

    private String toRelative(String fullPath) {
        // Zoek het deel vanaf 'MetaData\' en vervang dat door '.\'
        String marker = "MetaData\\";
        int idx = fullPath.indexOf(marker);
        if (idx >= 0) {
            return ".\\" + fullPath.substring(idx + marker.length());
        }
        return fullPath;
    }

    // --- helpers (ongewijzigd) ---

    private static String val(Metadata m, Property p)    { return trim(m.get(p)); }
    private static String val(Metadata m, String key)    { return trim(m.get(key)); }
    private static String trim(String s)                 { return (s==null||s.isBlank())?null:s.trim(); }
    private static void put(Map<String,String> x, String k, String v) {
        if (v!=null && !v.isBlank()) x.put(k, v.trim());
    }

    private static String cleanTitle(String t, Pattern p) {
        if (t==null||t.isBlank()) return null;
        String s = p.matcher(t).replaceFirst("")
                .replace('_',' ')
                .replaceAll("\\s+"," ")
                .trim();
        return s.isBlank()?null:s;
    }

    private static Optional<Instant> parse(String dt) {
        if (dt==null||dt.isBlank()) return Optional.empty();
        List<Supplier<Optional<Instant>>> ps = List.of(
                ()->Optional.of(OffsetDateTime.parse(dt).toInstant()),
                ()->Optional.of(ZonedDateTime.parse(dt).toInstant()),
                ()->Optional.of(Instant.parse(dt)),
                ()->Optional.of(LocalDateTime.parse(dt).atZone(ZoneId.systemDefault()).toInstant()),
                ()->Optional.of(LocalDate.parse(dt).atStartOfDay(ZoneId.systemDefault()).toInstant())
        );
        return ps.stream().map(Supplier::get).filter(Optional::isPresent).map(Optional::get).findFirst();
    }

    private Optional<String> fmt(Instant i) {
        return Optional.ofNullable(i).map(cfg.getIso8601Formatter()::format);
    }

    private static String mapFormat(String ct) {
        if (ct==null||ct.isBlank()) return "Unknown";
        String key = ct.toLowerCase().split(";")[0].trim();
        return FORMAT_MAP.getOrDefault(key,
                key.contains("/")? key.substring(key.lastIndexOf('/')+1)
                        .toUpperCase()
                        .replaceAll("[^A-Z0-9]","")
                        : key.toUpperCase()
        );
    }

    private String toLangUri(String code) {
        String tag = (code==null||code.isBlank()||code.equalsIgnoreCase("und"))
                ? "UND"
                : LANG_MAP.getOrDefault(code.toLowerCase().split("-")[0], "MUL");
        return "http://publications.europa.eu/resource/authority/language/" + tag;
    }

    private Optional<LanguageResult> detect(LanguageDetector d, String txt, int maxLen) {
        try {
            if (txt==null||txt.isBlank()) return Optional.empty();
            String s = txt.substring(0, Math.min(txt.length(), maxLen)).trim();
            return s.isEmpty() ? Optional.empty() : Optional.of(d.detect(s));
        } catch (Exception e) {
            System.err.println("Waarschuwing: Taaldetectie mislukt: " + e.getMessage());
            return Optional.empty();
        }
    }

    private String makeDescription(Metadata m, String txt, String fn) {
        String desc = val(m, DublinCore.DESCRIPTION);
        if (!valid(desc)) desc = val(m, TikaCoreProperties.DESCRIPTION);
        if (valid(desc)) {
            String d = desc.trim().replaceAll("\\s+"," ");
            if (d.length() > cfg.getMaxAutoDescriptionLength()) {
                int end = cfg.getMaxAutoDescriptionLength();
                int sp  = d.lastIndexOf(' ', end-3);
                d = (sp> end/2 ? d.substring(0, sp) : d.substring(0, end-3)).trim() + "...";
            }
            d = d.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!d.isEmpty()) return d;
        }
        if (txt!=null && txt.strip().length()>10) {
            String t   = txt.trim().replaceAll("\\s+"," ");
            int end    = Math.min(t.length(), cfg.getMaxAutoDescriptionLength());
            String sn  = t.substring(0, end).replaceAll("^[\\W_]+|[\\W_]+$","").trim();
            if (!sn.isEmpty()) {
                return sn + (t.length()>end && sn.length()<=cfg.getMaxAutoDescriptionLength()-3 ? "..." : "");
            }
        }
        return "Geen beschrijving beschikbaar voor: " + fn;
    }

    private boolean valid(String s) {
        if (s==null||s.isBlank()) return false;
        String t = s.trim();
        return t.length() >= cfg.getMinDescMetadataLength()
                && t.length() <= cfg.getMaxDescMetadataLength()
                && !t.contains("_x000d_")
                && !t.equalsIgnoreCase("untitled")
                && !t.equalsIgnoreCase("no title")
                && !t.matches("(?i)^\\s*template\\s*$");
    }
}
