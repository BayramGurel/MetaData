import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
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

// --- Configuratie Klasse (onveranderd) ---
final class ExtractorConfiguration {
    private final int maxTextSampleLength; private final int maxExtractedTextLength;
    private final int maxAutoDescriptionLength; private final int minDescMetadataLength;
    private final int maxDescMetadataLength; private final Set<String> supportedZipExtensions;
    private final Set<String> ignoredExtensions; private final List<String> ignoredPrefixes;
    private final List<String> ignoredFilenames; private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri; private final Pattern titlePrefixPattern;

    public ExtractorConfiguration() {
        this.maxTextSampleLength = 10000; this.maxExtractedTextLength = 5 * 1024 * 1024;
        this.maxAutoDescriptionLength = 250; this.minDescMetadataLength = 10;
        this.maxDescMetadataLength = 1000; this.supportedZipExtensions = Set.of(".zip");
        this.ignoredExtensions = Set.of(".ds_store", "thumbs.db", ".tmp", ".bak", ".lock", ".freelist", ".gdbindexes", ".gdbtablx", ".atx", ".spx", ".horizon", ".cdx", ".fpt");
        this.ignoredPrefixes = List.of("~", "._"); this.ignoredFilenames = List.of("gdb");
        this.iso8601Formatter = DateTimeFormatter.ISO_INSTANT; this.placeholderUri = "urn:placeholder:vervang-mij";
        this.titlePrefixPattern = Pattern.compile("^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )", Pattern.CASE_INSENSITIVE);
    }
    // Getters...
    public int getMaxTextSampleLength() { return maxTextSampleLength; } public int getMaxExtractedTextLength() { return maxExtractedTextLength; }
    public int getMaxAutoDescriptionLength() { return maxAutoDescriptionLength; } public int getMinDescMetadataLength() { return minDescMetadataLength; }
    public int getMaxDescMetadataLength() { return maxDescMetadataLength; } public Set<String> getSupportedZipExtensions() { return supportedZipExtensions; }
    public Set<String> getIgnoredExtensions() { return ignoredExtensions; } public List<String> getIgnoredPrefixes() { return ignoredPrefixes; }
    public List<String> getIgnoredFilenames() { return ignoredFilenames; } public DateTimeFormatter getIso8601Formatter() { return iso8601Formatter; }
    public String getPlaceholderUri() { return placeholderUri; } public Pattern getTitlePrefixPattern() { return titlePrefixPattern; }
}

// --- Data Objecten (Records) ---
class CkanResource {
    private final Map<String, Object> data;
    public CkanResource(Map<String, Object> data) { this.data = Collections.unmodifiableMap(new HashMap<>(data)); }
    public Map<String, Object> getData() { return new HashMap<>(data); }
}
record ProcessingError(String source, String error) {}
record IgnoredEntry(String source, String reason) {}
class ProcessingReport {
    private final List<CkanResource> results; private final List<ProcessingError> errors; private final List<IgnoredEntry> ignored;
    public ProcessingReport(List<CkanResource> r, List<ProcessingError> e, List<IgnoredEntry> i) {
        this.results = Collections.unmodifiableList(new ArrayList<>(r)); this.errors = Collections.unmodifiableList(new ArrayList<>(e)); this.ignored = Collections.unmodifiableList(new ArrayList<>(i));
    }
    public List<CkanResource> getResults() { return results; } public List<ProcessingError> getErrors() { return errors; } public List<IgnoredEntry> getIgnored() { return ignored; }
}

// --- Interfaces ---
interface IFileTypeFilter { boolean isFileTypeRelevant(String entryName); }
interface IMetadataProvider { record ExtractionOutput(Metadata metadata, String text) {} ExtractionOutput extract(InputStream is, int maxLen) throws IOException, TikaException, SAXException; }
interface ICkanResourceFormatter { CkanResource format(String eName, Metadata md, String txt, String sId); }
interface ISourceProcessor { void processSource(Path sPath, String cPath, List<CkanResource> res, List<ProcessingError> err, List<IgnoredEntry> ign); }

// --- Utility Inner Class ---
class NonClosingInputStream extends FilterInputStream { protected NonClosingInputStream(InputStream in) { super(in); } @Override public void close() {} }

// --- Implementaties ---
class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config;
    public DefaultFileTypeFilter(ExtractorConfiguration config) { this.config = Objects.requireNonNull(config); }
    @Override public boolean isFileTypeRelevant(String entryName) { if (entryName == null || entryName.isEmpty()) return false; String filename = getFilenameFromEntry(entryName); if (filename.isEmpty()) return false; String lowerFilename = filename.toLowerCase(); if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) return false; if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) return false; int lastDot = lowerFilename.lastIndexOf('.'); if (lastDot > 0 && lastDot < lowerFilename.length() - 1) { String extension = lowerFilename.substring(lastDot); if (config.getIgnoredExtensions().contains(extension)) return false; } return true; }
    private static String getFilenameFromEntry(String eN){if(eN==null)return"";String nN=eN.replace('\\','/');int lS=nN.lastIndexOf('/');return(lS>=0)?nN.substring(lS+1):nN;}
}

class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser;
    public TikaMetadataProvider() { this(new AutoDetectParser()); }
    public TikaMetadataProvider(Parser parser) { this.tikaParser = Objects.requireNonNull(parser); }
    @Override public ExtractionOutput extract(InputStream is, int maxLen) throws IOException, TikaException, SAXException { BodyContentHandler h = new BodyContentHandler(maxLen); Metadata md = new Metadata(); ParseContext ctx = new ParseContext(); try { tikaParser.parse(is, h, md, ctx); return new ExtractionOutput(md, h.toString()); } catch (Exception e) { System.err.println("Fout Tika parsing: " + e.getMessage()); if (e instanceof TikaException) throw (TikaException) e; if (e instanceof SAXException) throw (SAXException) e; if (e instanceof IOException) throw (IOException) e; throw new TikaException("Onverwachte Tika parsing fout", e); } }
}

/**
 * Implementatie van ICkanResourceFormatter (CKAN-Compatible Output).
 */
class DefaultCkanResourceFormat implements ICkanResourceFormatter {
    private final LanguageDetector languageDetector;
    private final ExtractorConfiguration config;

    public DefaultCkanResourceFormat(LanguageDetector loadedLanguageDetector, ExtractorConfiguration config) {
        if (loadedLanguageDetector == null) { System.err.println("Waarschuwing: Geen LanguageDetector."); }
        this.languageDetector = loadedLanguageDetector;
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        // Gebruik LinkedHashMap voor voorspelbare volgorde in JSON (optioneel)
        Map<String, Object> resourceData = new LinkedHashMap<>();
        Map<String, String> extras = new LinkedHashMap<>(); // Gebruik Map voor extras
        String filename = getFilenameFromEntry(entryName);

        // Vul de resource data map direct (geen comments, geen hints)
        populateCoreFields(resourceData, metadata, text, filename);
        populateDates(resourceData, metadata);
        populateExtrasMap(extras, metadata, text, sourceIdentifier, filename); // Vul extras Map

        // Voeg extras map toe *alleen* als deze niet leeg is
        if (!extras.isEmpty()) {
            resourceData.put("extras", extras);
        }

        return new CkanResource(resourceData);
    }

    // --- Private Helper Methoden ---

    private void populateCoreFields(Map<String, Object> resourceData, Metadata metadata, String text, String filename) {
        // Plaats placeholders - DEZE MOETEN LATER VERVANGEN WORDEN
        resourceData.put("package_id", "PLACEHOLDER_PACKAGE_ID");
        resourceData.put("url", "PLACEHOLDER_URL");

        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle, config.getTitlePrefixPattern());
        String resourceName = Optional.ofNullable(cleanedTitle)
                .or(() -> Optional.ofNullable(originalTitle))
                .orElse(filename);
        resourceData.put("name", resourceName);

        String description = generateDescription(metadata, text, filename);
        resourceData.put("description", description); // Kan null zijn als niet gegenereerd

        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType)); // Simpele format string

        // Voeg mimetype als top-level veld toe
        if (contentType != null && !contentType.isBlank()) {
            resourceData.put("mimetype", contentType.split(";")[0].trim()); // Primaire mimetype
        } else {
            resourceData.put("mimetype", null); // Expliciet null als onbekend
        }
    }

    private void populateDates(Map<String, Object> resourceData, Metadata metadata) {
        parseToInstant(metadata.get(TikaCoreProperties.CREATED)).ifPresent(instant ->
                formatInstantToIso8601(instant, config.getIso8601Formatter()).ifPresent(formattedDate ->
                        resourceData.put("created", formattedDate)
                )
        );
        parseToInstant(metadata.get(TikaCoreProperties.MODIFIED)).ifPresent(instant ->
                formatInstantToIso8601(instant, config.getIso8601Formatter()).ifPresent(formattedDate ->
                        resourceData.put("last_modified", formattedDate)
                )
        );
        // Overweeg size toe te voegen indien beschikbaar in metadata (TikaCoreProperties.CONTENT_LENGTH)
        // String sizeStr = getMetadataValue(metadata, Metadata.CONTENT_LENGTH);
        // if (sizeStr != null) { try { resourceData.put("size", Long.parseLong(sizeStr)); } catch (NumberFormatException e) { /* ignore */ } }
    }

    /** Vult de CKAN extras *Map*. */
    private void populateExtrasMap(Map<String, String> extras, Metadata metadata, String text, String sourceIdentifier, String filename) {
        addExtraMap(extras, "source_identifier", sourceIdentifier);
        addExtraMap(extras, "original_entry_name", filename);

        // Voeg creator toe als extra
        addExtraMap(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));

        // Voeg taal URI toe als extra
        detectLanguage(text, config.getMaxTextSampleLength())
                .filter(LanguageResult::isReasonablyCertain)
                .ifPresentOrElse(
                        langResult -> addExtraMap(extras, "language_uri", mapLanguageCodeToNalUri(langResult.getLanguage())),
                        () -> addExtraMap(extras, "language_uri", mapLanguageCodeToNalUri("und"))
                );

        // Voeg eventueel de volledige Content-Type van Tika toe als extra voor debug/info
        // addExtraMap(extras, "tika_content_type_full", getMetadataValue(metadata, Metadata.CONTENT_TYPE));
    }

    // --- Statische Helper Methoden ---
    /** Voegt key-value toe aan de extras map indien value niet leeg is. */
    private static void addExtraMap(Map<String, String> extrasMap, String key, String value) {
        if (value != null && !value.trim().isEmpty()) { extrasMap.put(key, value.trim()); }
    }
    private static String getFilenameFromEntry(String eN){if(eN==null)return"";String nN=eN.replace('\\','/');int lS=nN.lastIndexOf('/');return(lS>=0)?nN.substring(lS+1):nN;}
    private static String cleanTitle(String t,Pattern p){if(t==null||t.trim().isEmpty())return null;String c=p.matcher(t).replaceFirst("").replace('_',' ');return c.replaceAll("\\s+"," ").trim().isEmpty()?null:c.replaceAll("\\s+"," ").trim();}
    private static String getMetadataValue(Metadata m, org.apache.tika.metadata.Property prop){String v=m.get(prop);return(v!=null&&!v.trim().isEmpty())?v.trim():null;}
    private static String getMetadataValue(Metadata m, String k){String v=m.get(k);return(v!=null&&!v.trim().isEmpty())?v.trim():null;}
    private static Optional<String> formatInstantToIso8601(Instant i,DateTimeFormatter f){return Optional.ofNullable(i).map(f::format);}
    private static Optional<Instant> parseToInstant(String dTS){if(dTS==null||dTS.trim().isEmpty())return Optional.empty();List<DateTimeFormatter> fs=Arrays.asList(DateTimeFormatter.ISO_OFFSET_DATE_TIME,DateTimeFormatter.ISO_ZONED_DATE_TIME,DateTimeFormatter.ISO_INSTANT,DateTimeFormatter.ISO_LOCAL_DATE_TIME,DateTimeFormatter.ISO_LOCAL_DATE);for(DateTimeFormatter f:fs){try{if(f==DateTimeFormatter.ISO_OFFSET_DATE_TIME)return Optional.of(OffsetDateTime.parse(dTS,f).toInstant());if(f==DateTimeFormatter.ISO_ZONED_DATE_TIME)return Optional.of(ZonedDateTime.parse(dTS,f).toInstant());if(f==DateTimeFormatter.ISO_INSTANT)return Optional.of(Instant.parse(dTS));if(f==DateTimeFormatter.ISO_LOCAL_DATE_TIME)return Optional.of(java.time.LocalDateTime.parse(dTS,f).atZone(ZoneId.systemDefault()).toInstant());if(f==DateTimeFormatter.ISO_LOCAL_DATE)return Optional.of(java.time.LocalDate.parse(dTS,f).atStartOfDay(ZoneId.systemDefault()).toInstant());}catch(DateTimeParseException ignored){}} System.err.println("Warn: Datum parse failed: "+dTS);return Optional.empty();}
    private static String mapContentTypeToSimpleFormat(String cT){return Optional.ofNullable(cT).filter(c->!c.isBlank()).map(c->c.toLowerCase().split(";")[0].trim()).map(lT->switch(lT){case"application/pdf"->"PDF";case"application/vnd.openxmlformats-officedocument.wordprocessingml.document"->"DOCX";case"application/msword"->"DOC";case"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"->"XLSX";case"application/vnd.ms-excel"->"XLS";case"text/plain"->"TXT";case"text/csv"->"CSV";case"image/jpeg","image/jpg"->"JPEG";case"image/png"->"PNG";case"application/zip"->"ZIP";case"application/xml","text/xml"->"XML";case"application/shp","application/x-shapefile","application/vnd.shp"->"SHP";case"application/x-dbf","application/vnd.dbf"->"DBF";default->{int i=lT.lastIndexOf('/');yield(i!=-1&&i<lT.length()-1)?lT.substring(i+1).toUpperCase().replaceAll("[^A-Z0-9]",""):lT.toUpperCase().replaceAll("[^A-Z0-9]","");}}).orElse("Unknown");}
    private static String mapLanguageCodeToNalUri(String lC){final String BASE="http://publications.europa.eu/resource/authority/language/";if(lC==null||lC.isBlank()||lC.equalsIgnoreCase("und"))return BASE+"UND";String nC=lC.toLowerCase().split("-")[0];return BASE+switch(nC){case"nl"->"NLD";case"en"->"ENG";case"de"->"DEU";case"fr"->"FRA";default->"MUL";};}

    // --- Instance Helper Methoden ---
    private String generateDescription(Metadata m,String t,String f){String d=getMetadataValue(m,DublinCore.DESCRIPTION);if(!isDescriptionValid(d)){d=getMetadataValue(m,TikaCoreProperties.DESCRIPTION);if(!isDescriptionValid(d))d=null;}if(d!=null){String cD=d.trim().replaceAll("\\s+"," ");if(cD.length()>config.getMaxAutoDescriptionLength())cD=cD.substring(0,config.getMaxAutoDescriptionLength()).trim()+"...";cD=cD.replaceAll("^[\\W_]+|[\\W_]+$","").trim();if(!cD.isEmpty())return cD;}if(t!=null&&!t.isBlank()){String cT=t.trim().replaceAll("\\s+"," ");if(cT.length()>10){int end=Math.min(cT.length(),config.getMaxAutoDescriptionLength());String s=cT.substring(0,end).trim().replaceAll("^[\\W_]+|[\\W_]+$","").trim();String suffix=(cT.length()>end&&s.length()<config.getMaxAutoDescriptionLength())?"...":"";if(!s.isEmpty())return s+suffix;}}return"Geen beschrijving beschikbaar voor: "+f;}
    private boolean isDescriptionValid(String d){if(d==null||d.isBlank())return false;String t=d.trim();return t.length()>=config.getMinDescMetadataLength()&&t.length()<=config.getMaxDescMetadataLength()&&!t.contains("_x000d_");}
    private Optional<LanguageResult> detectLanguage(String t,int max){if(this.languageDetector==null||t==null||t.trim().isEmpty())return Optional.empty();try{String s=t.substring(0,Math.min(t.length(),max)).trim();if(s.isEmpty())return Optional.empty();return Optional.of(this.languageDetector.detect(s));}catch(Exception e){System.err.println("Warn: Fout taaldetectie: "+e.getMessage());return Optional.empty();}}
}

/** Implementatie van ISourceProcessor voor ZIPs (met correcte error handling). */
class ZipSourceProcessor implements ISourceProcessor {
    private final IFileTypeFilter fileFilter; private final IMetadataProvider metadataProvider; private final ICkanResourceFormatter resourceFormatter; private final ExtractorConfiguration config;
    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) { this.fileFilter = ff; this.metadataProvider = mp; this.resourceFormatter = rf; this.config = cfg; }

    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath)))) { // try-with-resources
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName(); String fullEntryPath = containerPath + "!/" + entryName;
                try {
                    if (isInvalidPath(entryName)) { errors.add(new ProcessingError(fullEntryPath, "Onveilig pad.")); continue; }
                    if (entry.isDirectory()) { continue; }
                    if (!fileFilter.isFileTypeRelevant(entryName)) { ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant type.")); continue; }

                    if (isZipEntryZip(entry)) { processNestedZip(entry, zis, fullEntryPath, results, errors, ignored); }
                    else {
                        // Roep aan en handel Optional af
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(results::add, () -> errors.add(new ProcessingError(fullEntryPath, "Kon entry niet verwerken.")));
                    }
                } catch (Exception e) { errors.add(new ProcessingError(fullEntryPath, "Onverwachte entry fout: " + e.getMessage())); System.err.println("FOUT: Onverwacht bij entry '" + fullEntryPath + "': " + e.getMessage()); }
                finally { zis.closeEntry(); } // Altijd sluiten
            }
        } catch (IOException e) { errors.add(new ProcessingError(containerPath, "Fout lezen ZIP: " + e.getMessage())); System.err.println("FOUT: Kon ZIP niet lezen '" + containerPath + "': " + e.getMessage()); }
        catch (Exception e) { errors.add(new ProcessingError(containerPath, "Onverwachte ZIP fout: " + e.getMessage())); System.err.println("KRITIEKE FOUT ZIP '" + containerPath + "': " + e.getMessage()); e.printStackTrace(System.err); }
    }

    /** Verwerkt geneste ZIP. */
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try { tempZip = Files.createTempFile("nested_zip_", ".zip"); try (InputStream nzs = new NonClosingInputStream(zis)) { Files.copy(nzs, tempZip, StandardCopyOption.REPLACE_EXISTING); }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath); this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) { errors.add(new ProcessingError(fullEntryPath, "Fout geneste ZIP: " + e.getMessage())); System.err.println("FOUT: Geneste ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally { if (tempZip != null) try { Files.deleteIfExists(tempZip); } catch (IOException e) { System.err.println("WARN: Kon temp file niet verwijderen: " + tempZip); } }
    }

    /** Verwerkt regulier bestand in ZIP, retourneert Optional<CkanResource>. */
    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try (InputStream stream = new NonClosingInputStream(zis)) {
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            // Gebruik record accessors: output.metadata() en output.text()
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            return Optional.of(resource);
        } catch (Exception e) { System.err.println("FOUT: Verwerken entry '" + fullEntryPath + "' mislukt: " + e.getClass().getSimpleName() + ": " + e.getMessage()); return Optional.empty(); }
    }
    private boolean isZipEntryZip(ZipEntry e) { return e != null && !e.isDirectory() && config.getSupportedZipExtensions().stream().anyMatch(e.getName().toLowerCase()::endsWith); }
    private boolean isInvalidPath(String eN) { return eN.contains("..") || Paths.get(eN).normalize().isAbsolute(); }
}

// --- Hoofd Klasse: De Facade ---
/** Orkestreert het extractie proces. */
public class MetadataExtractor {
    private final IFileTypeFilter fileFilter; private final IMetadataProvider metadataProvider; private final ICkanResourceFormatter resourceFormatter; private final ISourceProcessor sourceProcessor; private final ExtractorConfiguration config;

    public MetadataExtractor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ISourceProcessor sp, ExtractorConfiguration cfg) {
        this.fileFilter = ff; this.metadataProvider = mp; this.resourceFormatter = rf; this.sourceProcessor = sp; this.config = cfg;
    }

    /** Verwerkt bronbestand of ZIP. */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results=new ArrayList<>(); List<ProcessingError> errors=new ArrayList<>(); List<IgnoredEntry> ignored=new ArrayList<>();
        try {
            Path sourcePath = Paths.get(sourcePathString).normalize();
            if (!Files.exists(sourcePath)) { errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden.")); }
            else if (Files.isDirectory(sourcePath)) { ignored.add(new IgnoredEntry(sourcePathString, "Bron is map.")); }
            else if (isZipFile(sourcePath)) { sourceProcessor.processSource(sourcePath, sourcePath.toString(), results, errors, ignored); }
            else { processSingleFile(sourcePath, results, errors, ignored); }
        } catch (Exception e) { errors.add(new ProcessingError(sourcePathString, "Kritieke fout: " + e.getMessage())); e.printStackTrace(System.err); }
        return finishReport(results, errors, ignored); // Altijd rapport teruggeven
    }

    /** Verwerkt een enkel bestand. */
    private void processSingleFile(Path sourcePath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String source = sourcePath.toString(); String filename = sourcePath.getFileName().toString();
        if (!fileFilter.isFileTypeRelevant(filename)) { ignored.add(new IgnoredEntry(source, "Irrelevant type.")); return; }
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            // Gebruik record accessors: output.metadata() en output.text()
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), source);
            results.add(resource);
        } catch (Exception e) { errors.add(new ProcessingError(source, e.getClass().getSimpleName() + ": " + e.getMessage())); System.err.println("FOUT bij bestand '" + source + "': " + e.getMessage()); }
    }

    /** Finaliseert en logt samenvatting. */
    private ProcessingReport finishReport(List<CkanResource> res, List<ProcessingError> err, List<IgnoredEntry> ign){
        System.err.printf("Verwerking afgerond. Resultaten: %d, Fouten: %d, Genegeerd: %d%n", res.size(), err.size(), ign.size());
        if (!err.isEmpty()) { System.err.println("Details fouten:"); err.forEach(error -> System.err.printf("  - [%s]: %s%n", error.source(), error.error())); }
        return new ProcessingReport(res, err, ign);
    }
    /** Checkt of pad een ZIP is. */
    private boolean isZipFile(Path p) { return p != null && Files.isRegularFile(p) && config.getSupportedZipExtensions().stream().anyMatch(p.getFileName().toString().toLowerCase()::endsWith); }

    // --- Main Methode ---
    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");
        String filePath = getFilePath(args); if (filePath == null) { System.err.println("FATAL: Geen geldig bestandspad opgegeven."); return; }

        // Setup
        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector languageDetector = loadLanguageDetector();
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider();
        // Belangrijk: Gebruik de *correcte* formatter implementatie
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormat(languageDetector, config);
        ISourceProcessor sourceProcessor = new ZipSourceProcessor(filter, provider, formatter, config);
        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, sourceProcessor, config);

        // Uitvoering
        System.out.println("INFO: Start verwerking voor: " + filePath);
        ProcessingReport report = extractor.processSource(filePath); // Samenvatting wordt al gelogd in finishReport

        // Output JSON
        printReportJson(report);
        System.out.println("\n--- Metadata Extractor Klaar ---");
    }

    /** Haalt bestandspad op. */
    private static String getFilePath(String[] args) {
        String defaultPath = "C:\\Users\\gurelb\\Downloads\\Veg kartering - habitatkaart 2021-2023.zip"; // <<-- AANPASSEN!
        String pathToCheck;
        if (args.length > 0) { pathToCheck = args[0]; System.out.println("INFO: Gebruik pad uit argument: " + pathToCheck); }
        else { pathToCheck = defaultPath; System.out.println("INFO: Gebruik standaard pad: " + pathToCheck); System.err.println("WAARSCHUWING: Zorg dat het standaard pad correct is!"); }
        if (!Files.exists(Paths.get(pathToCheck))) { System.err.println("FOUT: Pad '" + pathToCheck + "' bestaat niet!"); return null; }
        return pathToCheck;
    }
    /** Laadt taaldetector. */
    private static LanguageDetector loadLanguageDetector() { /* ... onveranderd ... */ try { System.out.println("INFO: Laden taalmodellen..."); LanguageDetector d = OptimaizeLangDetector.getDefaultLanguageDetector(); d.loadModels(); System.out.println("INFO: Taalmodellen geladen."); return d; } catch (NoClassDefFoundError e) { System.err.println("FOUT: tika-langdetect library mist."); } catch (IOException e) { System.err.println("FOUT: Kon taalmodellen niet laden: " + e.getMessage()); } catch (Exception e) { System.err.println("FOUT: Onverwachte fout laden taalmodellen: " + e.getMessage()); e.printStackTrace(System.err); } return null; }
    /** Print JSON rapport. */
    private static void printReportJson(ProcessingReport report) {
        if (report.getResults().isEmpty()) { System.out.println("\nINFO: Geen succesvolle resultaten om als JSON te tonen."); return; }
        System.out.println("\n--- Succesvolle Resources (JSON) ---");
        try {
            ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            Map<String, Object> root = new LinkedHashMap<>();
            // Verplaats summary naar stderr, focus stdout op data
            // root.put("summary", Map.of("successful", report.getResults().size(), "errors", report.getErrors().size(), "ignored", report.getIgnored().size()));
            root.put("resources", report.getResults().stream().map(CkanResource::getData).collect(Collectors.toList())); // Hernoemd naar "resources"
            System.out.println(mapper.writeValueAsString(root));
        } catch (JsonProcessingException e) { System.err.println("FOUT: Kon JSON niet maken: " + e.getMessage()); }
    }
} // Einde MetadataExtractor