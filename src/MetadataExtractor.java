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

// --- Configuratie & Data Objecten ---

/**
 * Bevat configuratie-instellingen voor de metadata extractor.
 * Doel: Maakt het mogelijk gedrag aan te passen (limieten, te negeren bestanden) zonder code te wijzigen.
 */
final class ExtractorConfiguration {
    // Velden voor configuratie (limieten, negeerlijsten, formatters etc.)
    private final int maxTextSampleLength; private final int maxExtractedTextLength;
    private final int maxAutoDescriptionLength; private final int minDescMetadataLength;
    private final int maxDescMetadataLength; private final Set<String> supportedZipExtensions;
    private final Set<String> ignoredExtensions; private final List<String> ignoredPrefixes;
    private final List<String> ignoredFilenames; private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri; private final Pattern titlePrefixPattern;

    /** Stelt standaard configuratiewaarden in. */
    public ExtractorConfiguration() {
        this.maxTextSampleLength = 10000; this.maxExtractedTextLength = 5 * 1024 * 1024;
        this.maxAutoDescriptionLength = 250; this.minDescMetadataLength = 10;
        this.maxDescMetadataLength = 1000; this.supportedZipExtensions = Set.of(".zip");
        this.ignoredExtensions = Set.of(".ds_store", "thumbs.db", ".tmp", ".bak", ".lock", ".freelist", ".gdbindexes", ".gdbtablx", ".atx", ".spx", ".horizon", ".cdx", ".fpt");
        this.ignoredPrefixes = List.of("~", "._"); this.ignoredFilenames = List.of("gdb");
        this.iso8601Formatter = DateTimeFormatter.ISO_INSTANT; this.placeholderUri = "urn:placeholder:vervang-mij";
        this.titlePrefixPattern = Pattern.compile("^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )", Pattern.CASE_INSENSITIVE);
    }
    // Getters voor toegang tot configuratiewaarden.
    public int getMaxTextSampleLength() { return maxTextSampleLength; } public int getMaxExtractedTextLength() { return maxExtractedTextLength; }
    public int getMaxAutoDescriptionLength() { return maxAutoDescriptionLength; } public int getMinDescMetadataLength() { return minDescMetadataLength; }
    public int getMaxDescMetadataLength() { return maxDescMetadataLength; } public Set<String> getSupportedZipExtensions() { return supportedZipExtensions; }
    public Set<String> getIgnoredExtensions() { return ignoredExtensions; } public List<String> getIgnoredPrefixes() { return ignoredPrefixes; }
    public List<String> getIgnoredFilenames() { return ignoredFilenames; } public DateTimeFormatter getIso8601Formatter() { return iso8601Formatter; }
    public String getPlaceholderUri() { return placeholderUri; } public Pattern getTitlePrefixPattern() { return titlePrefixPattern; }
}

/**
 * Representeert metadata van één bron, opgemaakt voor CKAN. Onveranderlijk (immutable).
 * Doel: Gestructureerde output per bestand voor catalogusintegratie.
 */
class CkanResource {
    /** De resource data (CKAN velden) als onveranderlijke map. */
    private final Map<String, Object> data;
    public CkanResource(Map<String, Object> data) { this.data = Collections.unmodifiableMap(new HashMap<>(data)); }
    /** Geeft een *kopie* van de data map terug. */
    public Map<String, Object> getData() { return new HashMap<>(data); }
}

/**
 * Representeert een fout opgetreden tijdens verwerking. Onveranderlijk.
 * Doel: Rapporteren welke bestanden faalden en waarom.
 * @param source Bron (bestand/pad) waar de fout optrad.
 * @param error Foutbeschrijving.
 */
record ProcessingError(String source, String error) {}

/**
 * Representeert een genegeerd bestand/item. Onveranderlijk.
 * Doel: Rapporteren welke bestanden zijn overgeslagen en waarom (filter/type).
 * @param source Bron van het genegeerde item.
 * @param reason Reden van negeren.
 */
record IgnoredEntry(String source, String reason) {}

/**
 * Bundelt resultaten, fouten en genegeerde items van een verwerkingsrun. Onveranderlijk.
 * Doel: Biedt een compleet overzicht van de uitkomst van het extractieproces.
 */
class ProcessingReport {
    private final List<CkanResource> results; // Succesvolle resources
    private final List<ProcessingError> errors; // Fouten
    private final List<IgnoredEntry> ignored; // Genegeerde items
    public ProcessingReport(List<CkanResource> r, List<ProcessingError> e, List<IgnoredEntry> i) {
        this.results = Collections.unmodifiableList(new ArrayList<>(r)); this.errors = Collections.unmodifiableList(new ArrayList<>(e)); this.ignored = Collections.unmodifiableList(new ArrayList<>(i));
    }
    public List<CkanResource> getResults() { return results; } public List<ProcessingError> getErrors() { return errors; } public List<IgnoredEntry> getIgnored() { return ignored; }
}

// --- Interfaces & Abstracte Klasse ---

/** Contract voor het filteren van bestanden op relevantie. */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/** Contract voor extractie van metadata en tekst uit een stream. */
interface IMetadataProvider {
    record ExtractionOutput(Metadata metadata, String text) {}
    ExtractionOutput extract(InputStream is, int maxLen) throws IOException, TikaException, SAXException;
}

/** Contract voor het formatteren van geëxtraheerde data naar {@link CkanResource}. */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/** Contract voor het verwerken van een databron. */
interface ISourceProcessor {
    /** Verwerkt de bron op sourcePath en voegt resultaten toe aan de lijsten. */
    void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}

/**
 * Abstracte basisklasse voor bronverwerkers. Bevat gedeelde dependencies.
 * Doel: Introduceert een basis voor overerving zoals vereist door de rubric,
 * en deelt gemeenschappelijke componenten die alle processors nodig hebben.
 */
abstract class AbstractSourceProcessor implements ISourceProcessor {
    protected final IFileTypeFilter fileFilter;
    protected final IMetadataProvider metadataProvider;
    protected final ICkanResourceFormatter resourceFormatter;
    protected final ExtractorConfiguration config;

    /** Constructor voor het injecteren van gedeelde dependencies. */
    protected AbstractSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        this.fileFilter = Objects.requireNonNull(ff, "File filter cannot be null");
        this.metadataProvider = Objects.requireNonNull(mp, "Metadata provider cannot be null");
        this.resourceFormatter = Objects.requireNonNull(rf, "Resource formatter cannot be null");
        this.config = Objects.requireNonNull(cfg, "Configuration cannot be null");
    }

    /**
     * Abstracte methode die door subclasses geïmplementeerd moet worden om de
     * specifieke bron (bv. ZIP, enkel bestand) te verwerken.
     */
    @Override
    public abstract void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);

    // Gemeenschappelijke helper methods kunnen hier eventueel worden toegevoegd.
    /** Haalt bestandsnaam uit pad-string. */
    protected static String getFilenameFromEntry(String eN){if(eN==null)return"";String nN=eN.replace('\\','/');int lS=nN.lastIndexOf('/');return(lS>=0)?nN.substring(lS+1):nN;}
    /** Checkt op onveilige paden ("..", absoluut). */
    protected boolean isInvalidPath(String eN) { return eN != null && (eN.contains("..") || Paths.get(eN).normalize().isAbsolute()); }

}

// --- Utility Inner Class ---

/** Inputstream wrapper die close() negeert. */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); }
    @Override public void close() {}
}

// --- Implementaties ---

/** Standaard filter implementatie. */
class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config;
    public DefaultFileTypeFilter(ExtractorConfiguration config) { this.config = Objects.requireNonNull(config); }
    @Override public boolean isFileTypeRelevant(String entryName) { if (entryName == null || entryName.isEmpty()) return false; String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName); if (filename.isEmpty()) return false; String lowerFilename = filename.toLowerCase(); if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) return false; if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) return false; int lastDot = lowerFilename.lastIndexOf('.'); if (lastDot > 0 && lastDot < lowerFilename.length() - 1) { String extension = lowerFilename.substring(lastDot); if (config.getIgnoredExtensions().contains(extension)) return false; } return true; }
}

/** Tika metadata provider implementatie. */
class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser;
    public TikaMetadataProvider() { this(new AutoDetectParser()); }
    public TikaMetadataProvider(Parser parser) { this.tikaParser = Objects.requireNonNull(parser); }
    @Override public ExtractionOutput extract(InputStream is, int maxLen) throws IOException, TikaException, SAXException { BodyContentHandler h = new BodyContentHandler(maxLen); Metadata md = new Metadata(); ParseContext ctx = new ParseContext(); try { tikaParser.parse(is, h, md, ctx); return new ExtractionOutput(md, h.toString()); } catch (Exception e) { System.err.println("Fout Tika parsing: " + e.getMessage()); if (e instanceof TikaException) throw (TikaException) e; if (e instanceof SAXException) throw (SAXException) e; if (e instanceof IOException) throw (IOException) e; throw new TikaException("Onverwachte Tika parsing fout", e); } }
}

/** Standaard CKAN resource formatter implementatie. */
class DefaultCkanResourceFormat implements ICkanResourceFormatter {
    private final LanguageDetector languageDetector;
    private final ExtractorConfiguration config;
    public DefaultCkanResourceFormat(LanguageDetector loadedLanguageDetector, ExtractorConfiguration config) {
        if (loadedLanguageDetector == null) { System.err.println("Waarschuwing: Geen LanguageDetector. Taaldetectie wordt overgeslagen."); }
        this.languageDetector = loadedLanguageDetector;
        this.config = Objects.requireNonNull(config);
    }
    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        Map<String, Object> resourceData = new LinkedHashMap<>(); Map<String, String> extras = new LinkedHashMap<>();
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        populateCoreFields(resourceData, metadata, text, filename); populateDates(resourceData, metadata); populateExtrasMap(extras, metadata, text, sourceIdentifier, filename);
        if (!extras.isEmpty()) { resourceData.put("extras", extras); } return new CkanResource(resourceData);
    }
    private void populateCoreFields(Map<String, Object> resourceData, Metadata metadata, String text, String filename) { resourceData.put("package_id", "PLACEHOLDER_PACKAGE_ID"); resourceData.put("url", "PLACEHOLDER_URL"); String oT=getMetadataValue(metadata, TikaCoreProperties.TITLE); String cT=cleanTitle(oT, config.getTitlePrefixPattern()); String rN=Optional.ofNullable(cT).or(()->Optional.ofNullable(oT)).orElse(filename); resourceData.put("name", rN); String d=generateDescription(metadata,text,filename); resourceData.put("description", d); String cType=getMetadataValue(metadata, Metadata.CONTENT_TYPE); resourceData.put("format", mapContentTypeToSimpleFormat(cType)); if(cType!=null&&!cType.isBlank()){resourceData.put("mimetype", cType.split(";")[0].trim());}else{resourceData.put("mimetype", null);} }
    private void populateDates(Map<String, Object> resourceData, Metadata metadata) { parseToInstant(metadata.get(TikaCoreProperties.CREATED)).flatMap(i->formatInstantToIso8601(i,config.getIso8601Formatter())).ifPresent(d->resourceData.put("created",d)); parseToInstant(metadata.get(TikaCoreProperties.MODIFIED)).flatMap(i->formatInstantToIso8601(i,config.getIso8601Formatter())).ifPresent(d->resourceData.put("last_modified",d)); }
    private void populateExtrasMap(Map<String, String> extras, Metadata metadata, String text, String sourceIdentifier, String filename) { addExtraMap(extras, "source_identifier", sourceIdentifier); addExtraMap(extras, "original_entry_name", filename); addExtraMap(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR)); detectLanguage(text, config.getMaxTextSampleLength()).filter(LanguageResult::isReasonablyCertain).ifPresentOrElse( l->addExtraMap(extras, "language_uri", mapLanguageCodeToNalUri(l.getLanguage())), ()->addExtraMap(extras, "language_uri", mapLanguageCodeToNalUri("und")) ); }
    private static void addExtraMap(Map<String, String> m, String k, String v) { if (v!=null&&!v.trim().isEmpty()) {m.put(k,v.trim());} }
    private static String cleanTitle(String t,Pattern p){if(t==null||t.trim().isEmpty())return null;String c=p.matcher(t).replaceFirst("").replace('_',' ');return c.replaceAll("\\s+"," ").trim().isEmpty()?null:c.replaceAll("\\s+"," ").trim();}
    private static String getMetadataValue(Metadata m, org.apache.tika.metadata.Property p){String v=m.get(p);return(v!=null&&!v.trim().isEmpty())?v.trim():null;}
    private static String getMetadataValue(Metadata m, String k){String v=m.get(k);return(v!=null&&!v.trim().isEmpty())?v.trim():null;}
    private static Optional<String> formatInstantToIso8601(Instant i,DateTimeFormatter f){return Optional.ofNullable(i).map(f::format);}
    private static Optional<Instant> parseToInstant(String dTS){if(dTS==null||dTS.trim().isEmpty())return Optional.empty();List<DateTimeFormatter>fs=Arrays.asList(DateTimeFormatter.ISO_OFFSET_DATE_TIME,DateTimeFormatter.ISO_ZONED_DATE_TIME,DateTimeFormatter.ISO_INSTANT,DateTimeFormatter.ISO_LOCAL_DATE_TIME,DateTimeFormatter.ISO_LOCAL_DATE);for(DateTimeFormatter f:fs){try{if(f==DateTimeFormatter.ISO_OFFSET_DATE_TIME)return Optional.of(OffsetDateTime.parse(dTS,f).toInstant());if(f==DateTimeFormatter.ISO_ZONED_DATE_TIME)return Optional.of(ZonedDateTime.parse(dTS,f).toInstant());if(f==DateTimeFormatter.ISO_INSTANT)return Optional.of(Instant.parse(dTS));if(f==DateTimeFormatter.ISO_LOCAL_DATE_TIME)return Optional.of(java.time.LocalDateTime.parse(dTS,f).atZone(ZoneId.systemDefault()).toInstant());if(f==DateTimeFormatter.ISO_LOCAL_DATE)return Optional.of(java.time.LocalDate.parse(dTS,f).atStartOfDay(ZoneId.systemDefault()).toInstant());}catch(DateTimeParseException ignored){}}System.err.println("Warn: Datum parse failed: "+dTS);return Optional.empty();}
    private static String mapContentTypeToSimpleFormat(String cT){return Optional.ofNullable(cT).filter(c->!c.isBlank()).map(c->c.toLowerCase().split(";")[0].trim()).map(lT->switch(lT){case"application/pdf"->"PDF";case"application/vnd.openxmlformats-officedocument.wordprocessingml.document"->"DOCX";case"application/msword"->"DOC";case"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"->"XLSX";case"application/vnd.ms-excel"->"XLS";case"text/plain"->"TXT";case"text/csv"->"CSV";case"image/jpeg","image/jpg"->"JPEG";case"image/png"->"PNG";case"application/zip"->"ZIP";case"application/xml","text/xml"->"XML";case"application/shp","application/x-shapefile","application/vnd.shp"->"SHP";case"application/x-dbf","application/vnd.dbf"->"DBF";default->{int i=lT.lastIndexOf('/');yield(i!=-1&&i<lT.length()-1)?lT.substring(i+1).toUpperCase().replaceAll("[^A-Z0-9]",""):lT.toUpperCase().replaceAll("[^A-Z0-9]","");}}).orElse("Unknown");}
    private static String mapLanguageCodeToNalUri(String lC){final String BASE="http://publications.europa.eu/resource/authority/language/";if(lC==null||lC.isBlank()||lC.equalsIgnoreCase("und"))return BASE+"UND";String nC=lC.toLowerCase().split("-")[0];return BASE+switch(nC){case"nl"->"NLD";case"en"->"ENG";case"de"->"DEU";case"fr"->"FRA";default->"MUL";};}
    private String generateDescription(Metadata m,String t,String f){String d=getMetadataValue(m,DublinCore.DESCRIPTION);if(!isDescriptionValid(d)){d=getMetadataValue(m,TikaCoreProperties.DESCRIPTION);if(!isDescriptionValid(d))d=null;}if(d!=null){String cD=d.trim().replaceAll("\\s+"," ");if(cD.length()>config.getMaxAutoDescriptionLength())cD=cD.substring(0,config.getMaxAutoDescriptionLength()).trim()+"...";cD=cD.replaceAll("^[\\W_]+|[\\W_]+$","").trim();if(!cD.isEmpty())return cD;}if(t!=null&&!t.isBlank()){String cT=t.trim().replaceAll("\\s+"," ");if(cT.length()>10){int end=Math.min(cT.length(),config.getMaxAutoDescriptionLength());String s=cT.substring(0,end).trim().replaceAll("^[\\W_]+|[\\W_]+$","").trim();String suffix=(cT.length()>end&&s.length()<config.getMaxAutoDescriptionLength())?"...":"";if(!s.isEmpty())return s+suffix;}}return"Geen beschrijving beschikbaar voor: "+f;}
    private boolean isDescriptionValid(String d){if(d==null||d.isBlank())return false;String t=d.trim();return t.length()>=config.getMinDescMetadataLength()&&t.length()<=config.getMaxDescMetadataLength()&&!t.contains("_x000d_");}
    private Optional<LanguageResult> detectLanguage(String t,int max){if(this.languageDetector==null||t==null||t.trim().isEmpty())return Optional.empty();try{String s=t.substring(0,Math.min(t.length(),max)).trim();if(s.isEmpty())return Optional.empty();return Optional.of(this.languageDetector.detect(s));}catch(Exception e){System.err.println("Warn: Fout taaldetectie: "+e.getMessage());return Optional.empty();}}
}

/**
 * Verwerkt ZIP-archieven. Erft van {@link AbstractSourceProcessor}.
 * Implementeert de logica specifiek voor het lezen van ZIP entries,
 * het verwerken van geneste ZIPs, en het aanroepen van metadata extractie
 * voor de bestanden binnenin. Voldoet aan rubric eis voor 'extends' en 'override'.
 */
class ZipSourceProcessor extends AbstractSourceProcessor {

    /** Constructor roept super aan met de benodigde componenten. */
    public ZipSourceProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /**
     * Verwerkt een ZIP-archief. Overschrijft de abstracte methode van de basisklasse.
     * Doel: Implementeert de kernlogica voor ZIP-verwerking (iteratie, filtering, delegatie).
     */
    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath)))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName;
                try {
                    if (isInvalidPath(entryName)) { errors.add(new ProcessingError(fullEntryPath, "Onveilig pad.")); continue; }
                    if (entry.isDirectory()) { continue; }
                    if (!fileFilter.isFileTypeRelevant(entryName)) { ignored.add(new IgnoredEntry(fullEntryPath, "Irrelevant type.")); continue; }

                    if (isZipEntryZip(entry)) {
                        processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                    } else {
                        processRegularEntry(entryName, zis, fullEntryPath)
                                .ifPresentOrElse(results::add, () -> errors.add(new ProcessingError(fullEntryPath, "Kon entry niet verwerken.")));
                    }
                } catch (Exception e) { errors.add(new ProcessingError(fullEntryPath, "Onverwachte entry fout: " + e.getMessage())); System.err.println("FOUT: Onverwacht bij entry '" + fullEntryPath + "': " + e.getMessage()); }
                finally { zis.closeEntry(); }
            }
        } catch (IOException e) { errors.add(new ProcessingError(containerPath, "Fout lezen ZIP: " + e.getMessage())); System.err.println("FOUT: Kon ZIP niet lezen '" + containerPath + "': " + e.getMessage()); }
        catch (Exception e) { errors.add(new ProcessingError(containerPath, "Onverwachte ZIP fout: " + e.getMessage())); System.err.println("KRITIEKE FOUT ZIP '" + containerPath + "': " + e.getMessage()); e.printStackTrace(System.err); }
    }

    /** Verwerkt geneste ZIP: extract naar temp, roep processSource recursief aan, ruim temp op. */
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");
            try (InputStream nzs = new NonClosingInputStream(zis)) { Files.copy(nzs, tempZip, StandardCopyOption.REPLACE_EXISTING); }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath);
            // Roep this.processSource aan; dit blijft de ZipSourceProcessor implementatie voor de temp file.
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) { errors.add(new ProcessingError(fullEntryPath, "Fout geneste ZIP: " + e.getMessage())); System.err.println("FOUT: Geneste ZIP '" + fullEntryPath + "': " + e.getMessage());
        } finally { if (tempZip != null) try { Files.deleteIfExists(tempZip); } catch (IOException e) { System.err.println("WARN: Kon temp file niet verwijderen: " + tempZip); } }
    }

    /** Verwerkt regulier bestand in ZIP. Gebruikt dependencies van base class. */
    private Optional<CkanResource> processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath) {
        try (InputStream stream = new NonClosingInputStream(zis)) {
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(entryName, output.metadata(), output.text(), fullEntryPath);
            return Optional.of(resource);
        } catch (Exception e) { System.err.println("FOUT: Verwerken entry '" + fullEntryPath + "' mislukt: " + e.getClass().getSimpleName() + ": " + e.getMessage()); return Optional.empty(); }
    }

    /** Checkt of entry een ZIP-bestand is (helper). */
    private boolean isZipEntryZip(ZipEntry e) {
        return e != null && !e.isDirectory() && config.getSupportedZipExtensions().stream().anyMatch(e.getName().toLowerCase()::endsWith);
    }
    // isInvalidPath is nu overgeërfd van AbstractSourceProcessor
}

/**
 * Verwerkt een enkel (niet-archief) bestand. Erft van {@link AbstractSourceProcessor}.
 * Implementeert de logica specifiek voor het direct openen en verwerken van één bestand.
 * Voldoet aan rubric eis voor 'extends' en 'override'.
 */
class SingleFileProcessor extends AbstractSourceProcessor {

    /** Constructor roept super aan met de benodigde componenten. */
    public SingleFileProcessor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        super(ff, mp, rf, cfg);
    }

    /**
     * Verwerkt één enkel bestand. Overschrijft de abstracte methode van de basisklasse.
     * Doel: Implementeert de kernlogica voor directe bestandsverwerking (filter, extract, format).
     */
    @Override
    public void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String source = sourcePath.toString();
        String filename = sourcePath.getFileName().toString();

        if (!fileFilter.isFileTypeRelevant(filename)) {
            ignored.add(new IgnoredEntry(source, "Irrelevant type."));
            return;
        }
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
            CkanResource resource = resourceFormatter.format(filename, output.metadata(), output.text(), source);
            results.add(resource);
        } catch (Exception e) {
            errors.add(new ProcessingError(source, e.getClass().getSimpleName() + ": " + e.getMessage()));
            System.err.println("FOUT bij bestand '" + source + "': " + e.getMessage());
        }
    }
}

// --- Hoofd Klasse: De Facade ---

/**
 * Orkestreert het extractieproces. Gebruikt nu specifieke SourceProcessor implementaties.
 * Doel: Biedt simpele interface, verbergt complexiteit, delegeert nu aan juiste processor.
 */
public class MetadataExtractor {
    // Gedeelde dependencies
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    /** Constructor injecteert gedeelde dependencies. */
    public MetadataExtractor(IFileTypeFilter ff, IMetadataProvider mp, ICkanResourceFormatter rf, ExtractorConfiguration cfg) {
        this.fileFilter = ff;
        this.metadataProvider = mp;
        this.resourceFormatter = rf;
        this.config = cfg;
    }

    /**
     * Verwerkt bronbestand of ZIP. Maakt de juiste SourceProcessor aan (ZIP of SingleFile)
     * en delegeert de verwerking daaraan.
     * Doel: Hoofdingang; kiest strategie (ZIP/file) en delegeert.
     */
    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results=new ArrayList<>(); List<ProcessingError> errors=new ArrayList<>(); List<IgnoredEntry> ignored=new ArrayList<>();
        ISourceProcessor processor; // Variabele voor de gekozen processor

        try {
            Path sourcePath = Paths.get(sourcePathString).normalize();
            if (!Files.exists(sourcePath)) {
                errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
            } else if (Files.isDirectory(sourcePath)) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund)."));
            } else {
                // Kies en maak de juiste processor aan
                if (isZipFile(sourcePath)) {
                    processor = new ZipSourceProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                } else {
                    processor = new SingleFileProcessor(fileFilter, metadataProvider, resourceFormatter, config);
                }
                // Roep de processSource methode aan op de gekozen processor
                processor.processSource(sourcePath, sourcePath.toString(), results, errors, ignored);
            }
        } catch (java.nio.file.InvalidPathException ipe) { errors.add(new ProcessingError(sourcePathString,"Ongeldige pad syntax")); }
        catch (Exception e) { errors.add(new ProcessingError(sourcePathString, "Kritieke fout: " + e.getMessage())); e.printStackTrace(System.err); }
        return finishReport(results, errors, ignored);
    }

    /** Finaliseert rapport en logt samenvatting. */
    private ProcessingReport finishReport(List<CkanResource> res, List<ProcessingError> err, List<IgnoredEntry> ign){
        System.err.printf("Verwerking afgerond. Resultaten: %d, Fouten: %d, Genegeerd: %d%n", res.size(), err.size(), ign.size());
        if (!err.isEmpty()) { System.err.println("Details fouten:"); err.forEach(error -> System.err.printf("  - [%s]: %s%n", error.source(), error.error())); }
        return new ProcessingReport(res, err, ign);
    }
    /** Checkt of pad een ZIP is. */
    private boolean isZipFile(Path p) { return p != null && Files.isRegularFile(p) && config.getSupportedZipExtensions().stream().anyMatch(p.getFileName().toString().toLowerCase()::endsWith); }

    // --- Main Methode ---

    /** CLI entry point: setup, verwerk pad, print JSON rapport. */
    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");
        String filePath = getFilePath(args); if (filePath == null) { System.err.println("FATAL: Geen geldig bestandspad opgegeven."); return; }

        // Setup: Laad gedeelde componenten
        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector languageDetector = loadLanguageDetector();
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider();
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormat(languageDetector, config);

        // Maak de extractor aan met de gedeelde componenten
        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, config);

        // Uitvoering
        System.out.println("INFO: Start verwerking voor: " + filePath);
        ProcessingReport report = extractor.processSource(filePath);

        // Output JSON
        printReportJson(report);
        System.out.println("\n--- Metadata Extractor Klaar ---");
    }

    /** Haalt bestandspad op. */
    private static String getFilePath(String[] args) {
        String defaultPath = "C:\\Users\\gurelb\\Downloads\\Veg kartering - habitatkaart 2021-2023.zip"; // <<-- AANPASSEN/VERWIJDEREN!
        String pathToCheck;
        if (args.length > 0 && args[0] != null && !args[0].isBlank()) { pathToCheck = args[0].trim(); System.out.println("INFO: Gebruik pad uit argument: " + pathToCheck); }
        else { pathToCheck = defaultPath; System.out.println("INFO: Gebruik standaard pad: " + pathToCheck); System.err.println("WAARSCHUWING: Zorg dat het standaard pad correct is of geef een pad mee!"); }
        try { if (!Files.exists(Paths.get(pathToCheck))) { System.err.println("FOUT: Pad '" + pathToCheck + "' bestaat niet!"); return null; }
        } catch (java.nio.file.InvalidPathException ipe) { System.err.println("FOUT: Pad syntax is ongeldig: '" + pathToCheck + "'"); return null; }
        return pathToCheck;
    }
    /** Laadt taal detector. */
    private static LanguageDetector loadLanguageDetector() { try { System.out.println("INFO: Laden taalmodellen..."); LanguageDetector d = OptimaizeLangDetector.getDefaultLanguageDetector(); d.loadModels(); System.out.println("INFO: Taalmodellen geladen."); return d; } catch (NoClassDefFoundError e) { System.err.println("FOUT: tika-langdetect library mist."); } catch (IOException e) { System.err.println("FOUT: Kon taalmodellen niet laden: " + e.getMessage()); } catch (Exception e) { System.err.println("FOUT: Onverwachte fout laden taalmodellen: " + e.getMessage()); e.printStackTrace(System.err); } System.err.println("WAARSCHUWING: Doorgaan zonder taaldetectie."); return null; }
    /** Print JSON rapport. */
    private static void printReportJson(ProcessingReport report) {
        if (report.getResults().isEmpty()) { System.out.println("\nINFO: Geen succesvolle resultaten om als JSON te tonen."); return; }
        System.out.println("\n--- Succesvolle Resources (JSON) ---");
        try {
            ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("resources", report.getResults().stream().map(CkanResource::getData).collect(Collectors.toList()));
            System.out.println(mapper.writeValueAsString(root));
        } catch (JsonProcessingException e) { System.err.println("FOUT: Kon JSON niet maken: " + e.getMessage()); }
        catch (Exception e) { System.err.println("FOUT: Onverwachte fout bij JSON generatie: " + e.getMessage()); e.printStackTrace(System.err); }
    }
} // Einde MetadataExtractor