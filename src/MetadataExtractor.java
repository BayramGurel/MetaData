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

// --- Configuratie Klasse ---
/**
 * Bevat alle configuratie-instellingen bij elkaar.
 * Dit maakt het makkelijk om later instellingen aan te passen.
 * 'final' betekent dat deze klasse niet uitgebreid kan worden (geen subklassen).
 */
final class ExtractorConfiguration {
    // final velden: moeten een waarde krijgen in de constructor en kunnen daarna niet meer veranderen.
    private final int maxTextSampleLength;
    private final int maxExtractedTextLength;
    private final int maxAutoDescriptionLength;
    private final int minDescMetadataLength;
    private final int maxDescMetadataLength;
    private final Set<String> supportedZipExtensions;
    private final Set<String> ignoredExtensions;
    private final List<String> ignoredPrefixes;
    private final List<String> ignoredFilenames;
    private final DateTimeFormatter iso8601Formatter;
    private final String placeholderUri;
    private final Pattern titlePrefixPattern;

    /**
     * Constructor: maakt een nieuw configuratie-object met standaardwaarden.
     */
    public ExtractorConfiguration() {
        this.maxTextSampleLength = 10000; // Voor taaldetectie
        this.maxExtractedTextLength = 5 * 1024 * 1024; // Limiteer tekstextractie tot 5MB
        this.maxAutoDescriptionLength = 250; // Max lengte voor auto-gegenereerde beschrijving
        this.minDescMetadataLength = 10; // Minimale lengte voor bruikbare metadata beschrijving
        this.maxDescMetadataLength = 1000; // Maximale lengte
        this.supportedZipExtensions = Set.of(".zip");
        this.ignoredExtensions = Set.of(
                ".ds_store", "thumbs.db", ".tmp", ".bak", ".lock", ".freelist", ".gdbindexes",
                ".gdbtablx", ".atx", ".spx", ".horizon", ".cdx", ".fpt"
        );
        this.ignoredPrefixes = List.of("~", "._");
        this.ignoredFilenames = List.of("gdb");
        this.iso8601Formatter = DateTimeFormatter.ISO_INSTANT; // Standaard datum/tijd formaat
        this.placeholderUri = "urn:placeholder:vervang-mij";
        this.titlePrefixPattern = Pattern.compile( // Patroon om standaard prefixes van titels te verwijderen
                "^(Microsoft Word - |Microsoft Excel - |PowerPoint Presentation - |Adobe Acrobat - )",
                Pattern.CASE_INSENSITIVE);
    }

    // Getters: publieke methoden om de (private final) configuratiewaarden op te vragen.
    public int getMaxTextSampleLength() { return maxTextSampleLength; }
    public int getMaxExtractedTextLength() { return maxExtractedTextLength; }
    public int getMaxAutoDescriptionLength() { return maxAutoDescriptionLength; }
    public int getMinDescMetadataLength() { return minDescMetadataLength; }
    public int getMaxDescMetadataLength() { return maxDescMetadataLength; }
    public Set<String> getSupportedZipExtensions() { return supportedZipExtensions; }
    public Set<String> getIgnoredExtensions() { return ignoredExtensions; }
    public List<String> getIgnoredPrefixes() { return ignoredPrefixes; }
    public List<String> getIgnoredFilenames() { return ignoredFilenames; }
    public DateTimeFormatter getIso8601Formatter() { return iso8601Formatter; }
    public String getPlaceholderUri() { return placeholderUri; }
    public Pattern getTitlePrefixPattern() { return titlePrefixPattern; }
}


// --- Data Objecten voor Resultaten ---
// Deze klassen (ook wel DTOs - Data Transfer Objects genoemd) bevatten alleen data.
// Ze maken het makkelijk om resultaten gestructureerd terug te geven.

/**
 * Bevat de metadata voor één succesvol verwerkt bestand (resource).
 * De data wordt opgeslagen in een Map, wat flexibel is.
 */
class CkanResource {
    private final Map<String, Object> data; // 'private final': interne data, onveranderlijk na creatie

    public CkanResource(Map<String, Object> data) {
        // Maakt een onveranderlijke kopie om te zorgen dat de data niet per ongeluk
        // van buitenaf gewijzigd kan worden na creatie.
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    /** Geeft een *kopie* van de data terug (veilig voor externe gebruikers). */
    public Map<String, Object> getData() {
        return new HashMap<>(data);
    }
}

/** Bevat informatie over een fout tijdens de verwerking. */
class ProcessingError {
    private final String source; // Waar ging het mis? (bv. bestandsnaam)
    private final String error;  // Wat was de foutmelding?

    public ProcessingError(String source, String error) {
        this.source = source;
        this.error = error;
    }
    public String getSource() { return source; }
    public String getError() { return error; }
}

/** Bevat informatie over een bestand dat overgeslagen is. */
class IgnoredEntry {
    private final String source; // Welk bestand?
    private final String reason; // Waarom overgeslagen?

    public IgnoredEntry(String source, String reason) {
        this.source = source;
        this.reason = reason;
    }
    public String getSource() { return source; }
    public String getReason() { return reason; }
}

/**
 * Verzamelt alle resultaten van een verwerkingsrun.
 * Bevat lijsten van succesvolle resultaten, fouten en overgeslagen bestanden.
 */
class ProcessingReport {
    // Lijsten zijn 'final', maar de inhoud kan (via de constructor) toegevoegd worden.
    // Ze worden onveranderlijk gemaakt in de constructor.
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Maak onveranderlijke kopieën van de lijsten.
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }
    // Getters om de lijsten (onveranderlijk) op te vragen.
    public List<CkanResource> getResults() { return results; }
    public List<ProcessingError> getErrors() { return errors; }
    public List<IgnoredEntry> getIgnored() { return ignored; }
}


// --- Interfaces: Contracten voor Componenten ---
// Interfaces definiëren WAT een component moet kunnen, maar niet HOE.
// Dit maakt het systeem flexibel: je kunt later andere implementaties maken.

/** Contract voor het filteren van bestandstypen. */
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

/** Contract voor het extraheren van metadata. */
interface IMetadataProvider {
    /** Inner class: een klasse gedefinieerd binnen een andere klasse/interface. */
    class ExtractionOutput {
        final Metadata metadata; // Tika's metadata object
        final String text;       // Geëxtraheerde tekst
        ExtractionOutput(Metadata m, String t) { this.metadata = m; this.text = t; }
    }
    /** Methode die metadata en tekst uit een input stream haalt. */
    ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException;
}

/** Contract voor het formatteren van metadata naar een CkanResource. */
interface ICkanResourceFormatter {
    CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier);
}

/** Contract voor het verwerken van een bron (bestand of ZIP). */
interface ISourceProcessor {
    void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored);
}

// --- Utility Inner Class (Nuttig voor ZIP verwerking) ---
/**
 * Een speciaal soort InputStream die voorkomt dat de 'close()' methode
 * de onderliggende stream (hier de ZipInputStream) sluit.
 * Dit is nodig om door een ZIP bestand te kunnen lezen zonder het te vroeg te sluiten.
 * 'extends FilterInputStream' = dit is een voorbeeld van overerving. NonClosingInputStream
 * erft functionaliteit van FilterInputStream.
 */
class NonClosingInputStream extends FilterInputStream {
    protected NonClosingInputStream(InputStream in) { super(in); } // Roept constructor van superklasse aan
    /**
     * @Override: Deze methode overschrijft de close() methode van de superklasse.
     * In plaats van de stream te sluiten, doet deze methode niets.
     */
    @Override public void close() { /* Doe niets */ }
}

// --- Implementaties: Concrete Uitvoering van de Contracten ---

/**
 * Implementatie van IFileTypeFilter: filtert op basis van configuratie.
 */
class DefaultFileTypeFilter implements IFileTypeFilter {
    private final ExtractorConfiguration config; // Houdt de configuratie vast

    /** Constructor ontvangt de configuratie (Dependency Injection). */
    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    @Override
    public boolean isFileTypeRelevant(String entryName) {
        if (entryName == null || entryName.isEmpty()) return false;

        String filename = getFilenameFromEntry(entryName); // Gebruik private helper
        if (filename.isEmpty()) return false;
        String lowerFilename = filename.toLowerCase();

        // Check prefixes (bv. "~" of "._") uit config
        // Gebruik een stream en lambda expressie voor een korte check.
        // stream(): zet de lijst om in een stroom van elementen.
        // anyMatch(): controleert of *minstens één* element voldoet aan de voorwaarde.
        // lowerFilename::startsWith: een method reference, checkt of de filename start met de prefix.
        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false;
        }

        // Check genegeerde bestandsnamen (bv. "gdb") uit config (case-insensitive)
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false;
        }

        // Check genegeerde extensies (bv. ".tmp") uit config
        int lastDot = lowerFilename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < lowerFilename.length() - 1) {
            String extension = lowerFilename.substring(lastDot);
            if (config.getIgnoredExtensions().contains(extension)) {
                return false;
            }
        }
        // Als geen enkele 'ignore' regel van toepassing was, is het bestand relevant.
        return true;
    }

    // --- Private Helper Methode ---
    // 'private static': alleen binnen deze klasse bruikbaar, geen object nodig.
    /** Haalt de bestandsnaam uit een volledig pad (bv. "map/bestand.txt" -> "bestand.txt"). */
    private static String getFilenameFromEntry(String entryName) {
        if (entryName == null) return "";
        String normalizedName = entryName.replace('\\', '/'); // Zorg voor consistente slashes
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }
}

/**
 * Implementatie van IMetadataProvider: gebruikt Apache Tika.
 */
class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser; // Houdt de Tika parser vast

    /** Standaard constructor: gebruikt Tika's AutoDetectParser. */
    public TikaMetadataProvider() { this(new AutoDetectParser()); }
    /** Constructor om een specifieke Tika parser te gebruiken. */
    public TikaMetadataProvider(Parser parser) { this.tikaParser = Objects.requireNonNull(parser); }

    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength) throws IOException, TikaException, SAXException {
        // BodyContentHandler vangt de tekst op, gelimiteerd door maxTextLength.
        BodyContentHandler handler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata(); // Tika object om metadata op te slaan
        ParseContext context = new ParseContext(); // Context voor de parser
        try {
            // Roep Tika aan om de stream te parsen
            tikaParser.parse(inputStream, handler, metadata, context);
        } catch (Exception e) {
            // Vang algemene fouten tijdens Tika parsing
            System.err.println("Fout tijdens Tika parsing: " + e.getMessage());
            // Gooi de specifieke exception door als het een bekende Tika/SAX/IO fout is
            if (e instanceof TikaException) throw (TikaException) e;
            if (e instanceof SAXException) throw (SAXException) e;
            if (e instanceof IOException) throw (IOException) e;
            // Anders, wrap het in een TikaException
            throw new TikaException("Onverwachte Tika parsing fout", e);
        }
        // Geef het resultaat terug met de gevonden metadata en tekst
        return new ExtractionOutput(metadata, handler.toString());
    }
}

/**
 * Implementatie van ICkanResourceFormatter: formatteert data voor CKAN.
 */
class DefaultCkanResourceFormatter implements ICkanResourceFormatter {
    // Dependencies die deze klasse nodig heeft
    private final LanguageDetector languageDetector; // Voor taaldetectie (kan null zijn)
    private final ExtractorConfiguration config; // Voor configuratie-instellingen

    /** Constructor ontvangt dependencies (Configuratie en geladen Taaldetector). */
    public DefaultCkanResourceFormatter(LanguageDetector loadedLanguageDetector, ExtractorConfiguration config) {
        if (loadedLanguageDetector == null) {
            System.err.println("Waarschuwing: Geen LanguageDetector beschikbaar, taaldetectie uitgeschakeld.");
        }
        this.languageDetector = loadedLanguageDetector;
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        Map<String, Object> resourceData = new HashMap<>(); // Gebruik een Map om de resource data op te bouwen
        List<Map<String, String>> extras = new ArrayList<>(); // Lijst voor extra CKAN velden
        String filename = getFilenameFromEntry(entryName); // Gebruik helper methode

        // --- Vul de resourceData Map ---
        resourceData.put("__comment_mandatory__", "package_id en url MOETEN extern worden aangeleverd.");
        resourceData.put("package_id", "PLACEHOLDER_PACKAGE_ID"); // Moet later ingevuld worden
        resourceData.put("url", "PLACEHOLDER_URL"); // Moet later ingevuld worden

        // Bepaal de naam: probeer opgeschoonde titel, anders originele titel, anders bestandsnaam
        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle, config.getTitlePrefixPattern());
        String resourceName = Optional.ofNullable(cleanedTitle) // Optional helpt omgaan met null waarden
                .or(() -> Optional.ofNullable(originalTitle)) // Als cleanedTitle null is, probeer originalTitle
                .orElse(filename); // Als beide null zijn, gebruik bestandsnaam
        resourceData.put("name", resourceName);

        // Genereer beschrijving
        String description = generateDescription(metadata, text, filename);
        resourceData.put("description", description);

        // Bepaal formaat (bv. PDF, DOCX)
        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType));

        // Verwerk datums (indien aanwezig) naar ISO8601 formaat
        parseToInstant(metadata.get(TikaCoreProperties.CREATED)) // Parse datum string naar Instant
                .flatMap(instant -> formatInstantToIso8601(instant, config.getIso8601Formatter())) // Formatteer Instant naar String
                .ifPresent(d -> resourceData.put("created", d)); // Voeg toe aan map als succesvol
        parseToInstant(metadata.get(TikaCoreProperties.MODIFIED))
                .flatMap(instant -> formatInstantToIso8601(instant, config.getIso8601Formatter()))
                .ifPresent(d -> resourceData.put("last_modified", d));

        // --- Vul de 'extras' lijst ---
        addExtra(extras, "source_identifier", sourceIdentifier);
        addExtra(extras, "original_entry_name", filename);
        addExtra(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));
        addExtra(extras, "mime_type", contentType);
        //... (voeg eventueel meer extra's toe zoals voorheen)

        // Taaldetectie
        detectLanguage(text, config.getMaxTextSampleLength())
                .filter(LanguageResult::isReasonablyCertain) // Alleen als detectie redelijk zeker is
                .ifPresentOrElse( // Als er een zeker resultaat is...
                        langResult -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri(langResult.getLanguage())),
                        () -> addExtra(extras, "language_uri", mapLanguageCodeToNalUri("und")) // Anders: onbepaald (und)
                );
        resourceData.put("extras", extras); // Voeg de lijst met extra's toe aan de hoofdmap

        // --- Voeg Dataset Hints toe (optioneel) ---
        Map<String, Object> datasetHints = new HashMap<>();
        datasetHints.put("potential_dataset_title_suggestion", resourceName);
        String[] subjects = metadata.getValues(DublinCore.SUBJECT);
        if (subjects != null && subjects.length > 0) {
            // Gebruik stream om tags te verzamelen, te filteren en uniek te maken
            List<String> potentialTags = Arrays.stream(subjects)
                    .filter(s -> s != null && !s.trim().isEmpty())
                    .map(String::trim)
                    .distinct()
                    .collect(Collectors.toList());
            if (!potentialTags.isEmpty()) {
                datasetHints.put("potential_dataset_tags", potentialTags);
            }
        }
        resourceData.put("dataset_hints", datasetHints);

        // Maak en retourneer het CkanResource object
        return new CkanResource(resourceData);
    }

    // --- Private Helper Methoden (voorheen in Utils klasse) ---
    // Deze methoden helpen de 'format' methode, maar zijn alleen hier nodig.

    private static String getFilenameFromEntry(String entryName) { // Zelfde als in DefaultFileTypeFilter
        if (entryName == null) return "";
        String normalizedName = entryName.replace('\\', '/');
        int lastSlash = normalizedName.lastIndexOf('/');
        return (lastSlash >= 0) ? normalizedName.substring(lastSlash + 1) : normalizedName;
    }

    private static String cleanTitle(String title, Pattern titlePrefixPattern) {
        if (title == null || title.trim().isEmpty()) return null;
        String cleaned = titlePrefixPattern.matcher(title).replaceFirst("");
        cleaned = cleaned.replace('_', ' ');
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private static void addExtra(List<Map<String, String>> extras, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            extras.add(Map.of("key", key, "value", value.trim()));
        }
    }

    private static String getMetadataValue(Metadata metadata, org.apache.tika.metadata.Property property) {
        String value = metadata.get(property);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }
    private static String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }

    private static Optional<String> formatInstantToIso8601(Instant instant, DateTimeFormatter formatter) {
        // Optional.ofNullable: maakt een Optional, leeg als 'instant' null is.
        // .map: voert de formattering alleen uit als 'instant' niet null is.
        return Optional.ofNullable(instant).map(formatter::format);
    }

    private static Optional<Instant> parseToInstant(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return Optional.empty(); // Lege Optional als input leeg is
        }
        List<DateTimeFormatter> formatters = Arrays.asList( // Lijst van formaten om te proberen
                DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_ZONED_DATE_TIME,
                DateTimeFormatter.ISO_INSTANT, DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                DateTimeFormatter.ISO_LOCAL_DATE);

        for (DateTimeFormatter formatter : formatters) {
            try { // Probeer te parsen
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) return Optional.of(OffsetDateTime.parse(dateTimeString, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME) return Optional.of(ZonedDateTime.parse(dateTimeString, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_INSTANT) return Optional.of(Instant.parse(dateTimeString));
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME) return Optional.of(java.time.LocalDateTime.parse(dateTimeString, formatter).atZone(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE) return Optional.of(java.time.LocalDate.parse(dateTimeString, formatter).atStartOfDay(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException ignored) { /* Probeer volgende formaat */ }
        }
        System.err.println("Waarschuwing: Kon datum/tijd niet parsen: " + dateTimeString);
        return Optional.empty(); // Lege Optional als niets werkte
    }

    private static String mapContentTypeToSimpleFormat(String contentType) {
        return Optional.ofNullable(contentType).filter(c -> !c.isBlank())
                .map(c -> c.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) { // Gebruik switch expression voor leesbaarheid
                    case "application/pdf" -> "PDF";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "DOCX";
                    case "application/msword" -> "DOC";
                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> "XLSX";
                    case "application/vnd.ms-excel" -> "XLS";
                    // ... (voeg andere mappings toe zoals voorheen) ...
                    case "text/plain" -> "TXT";
                    case "text/csv" -> "CSV";
                    case "image/jpeg", "image/jpg" -> "JPEG";
                    case "image/png" -> "PNG";
                    case "application/zip" -> "ZIP";
                    case "application/xml", "text/xml" -> "XML";
                    case "application/shp", "application/x-shapefile", "application/vnd.shp" -> "SHP";
                    case "application/x-dbf", "application/vnd.dbf" -> "DBF";
                    default -> { // Fallback: probeer deel na laatste '/'
                        int lastSlash = lowerType.lastIndexOf('/');
                        yield (lastSlash != -1 && lastSlash < lowerType.length() - 1)
                                ? lowerType.substring(lastSlash + 1).toUpperCase().replaceAll("[^A-Z0-9]", "")
                                : lowerType.toUpperCase().replaceAll("[^A-Z0-9]", "");
                    }
                })
                .orElse("Unknown"); // Als input null of leeg was
    }
    // --- Andere Mapping Functies (IANA URI, EU URI, Taal) ---
    // Deze kunnen hier ook als private static methoden staan, of
    // je kunt besluiten ze weg te laten voor maximale eenvoud als ze niet strikt nodig zijn.
    // Voor nu laat ik ze weg om de klasse korter te maken. Voeg ze toe indien nodig.
    private static String mapLanguageCodeToNalUri(String langCode) {
        final String EU_LANG_BASE = "http://publications.europa.eu/resource/authority/language/";
        if (langCode == null || langCode.isBlank() || langCode.equalsIgnoreCase("und")) {
            return EU_LANG_BASE + "UND";
        }
        String normCode = langCode.toLowerCase().split("-")[0];
        return EU_LANG_BASE + switch (normCode) {
            case "nl" -> "NLD"; case "en" -> "ENG"; case "de" -> "DEU";
            case "fr" -> "FRA"; case "es" -> "SPA"; case "it" -> "ITA";
            default -> "MUL"; // Meertalig/Anders
        };
    }

    /** Genereert een beschrijving (interne logica). */
    private String generateDescription(Metadata metadata, String text, String filename) {
        String desc = getMetadataValue(metadata, DublinCore.DESCRIPTION);
        if (!isDescriptionValid(desc)) {
            desc = getMetadataValue(metadata, TikaCoreProperties.DESCRIPTION);
            if (!isDescriptionValid(desc)) {
                desc = null;
            }
        }

        if (desc != null) {
            String cleanDesc = desc.trim().replaceAll("\\s+", " ");
            if (cleanDesc.length() > config.getMaxAutoDescriptionLength()) {
                cleanDesc = cleanDesc.substring(0, config.getMaxAutoDescriptionLength()).trim() + "...";
            }
            cleanDesc = cleanDesc.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!cleanDesc.isEmpty()) return cleanDesc;
        }

        if (text != null && !text.isBlank()) {
            String cleanedText = text.trim().replaceAll("\\s+", " ");
            if (cleanedText.length() > 10) {
                int end = Math.min(cleanedText.length(), config.getMaxAutoDescriptionLength());
                String snippet = cleanedText.substring(0, end).trim();
                snippet = snippet.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
                String suffix = (cleanedText.length() > end && snippet.length() < config.getMaxAutoDescriptionLength()) ? "..." : "";
                if (!snippet.isEmpty()) return snippet + suffix;
            }
        }
        return "Geen beschrijving automatisch beschikbaar voor: " + filename;
    }

    /** Valideert beschrijving (interne logica). */
    private boolean isDescriptionValid(String description) {
        if (description == null || description.isBlank()) return false;
        String trimmed = description.trim();
        return trimmed.length() >= config.getMinDescMetadataLength() &&
                trimmed.length() <= config.getMaxDescMetadataLength() &&
                !trimmed.contains("_x000d_");
    }

    /** Detecteert taal (interne logica). */
    private Optional<LanguageResult> detectLanguage(String text, int maxSampleLength) {
        if (this.languageDetector == null || text == null || text.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            String textSample = text.substring(0, Math.min(text.length(), maxSampleLength)).trim();
            if (textSample.isEmpty()) return Optional.empty();
            return Optional.of(this.languageDetector.detect(textSample));
        } catch (Exception e) {
            System.err.println("Waarschuwing: Fout tijdens taaldetectie: " + e.getMessage());
            return Optional.empty();
        }
    }
}

/**
 * Implementatie van ISourceProcessor: verwerkt ZIP-bestanden.
 */
class ZipSourceProcessor implements ISourceProcessor {
    // Dependencies
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    /** Constructor ontvangt dependencies. */
    public ZipSourceProcessor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ExtractorConfiguration config) {
        this.fileFilter = fileFilter;
        this.metadataProvider = metadataProvider;
        this.resourceFormatter = resourceFormatter;
        this.config = config;
    }

    @Override
    public void processSource(Path zipPath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Gebruik try-with-resources voor automatische resource afhandeling (sluiten van streams)
        try (InputStream fis = Files.newInputStream(zipPath);
             BufferedInputStream bis = new BufferedInputStream(fis); // Efficiënter lezen
             ZipInputStream zis = new ZipInputStream(bis)) { // Om ZIP entries te lezen

            ZipEntry entry;
            // Loop door alle entries in het ZIP bestand
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                String fullEntryPath = containerPath + "!/" + entryName; // Uniek pad voor deze entry

                // --- Veiligheidschecks en Filters ---
                if (isInvalidPath(entryName)) { // Check op bv. "../"
                    errors.add(new ProcessingError(fullEntryPath, "Onveilige entry naam overgeslagen."));
                    System.err.println("FOUT: Onveilige entry naam: " + fullEntryPath);
                    zis.closeEntry(); continue; // Ga naar volgende entry
                }
                if (entry.isDirectory()) {
                    zis.closeEntry(); continue; // Sla mappen over
                }
                if (!fileFilter.isFileTypeRelevant(entryName)) { // Gebruik de filter component
                    ignored.add(new IgnoredEntry(fullEntryPath, "Bestandstype genegeerd door filter"));
                    zis.closeEntry(); continue; // Sla irrelevante bestanden over
                }

                // --- Verwerking: Geneste ZIP of Normaal Bestand? ---
                if (isZipEntryZip(entry)) {
                    // Het is een ZIP binnen een ZIP: verwerk recursief
                    processNestedZip(entry, zis, fullEntryPath, results, errors, ignored);
                } else {
                    // Het is een normaal bestand: extraheer & formatteer
                    processRegularEntry(entryName, zis, fullEntryPath, results, errors);
                }
                zis.closeEntry(); // Ga naar de volgende entry in de ZIP
            }
        } catch (IOException e) {
            // Fout bij het lezen van het hoof-ZIP bestand
            errors.add(new ProcessingError(containerPath, "Fout bij lezen ZIP: " + e.getMessage()));
            System.err.println("FOUT: Kon ZIP niet lezen '" + containerPath + "': " + e.getMessage());
        } catch (Exception e) {
            // Onverwachte fout tijdens ZIP verwerking
            errors.add(new ProcessingError(containerPath, "Onverwachte ZIP fout: " + e.getMessage()));
            System.err.println("KRITIEKE FOUT bij ZIP '" + containerPath + "': " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    /** Verwerkt een geneste ZIP door deze tijdelijk op te slaan en opnieuw te processen. */
    private void processNestedZip(ZipEntry entry, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("nested_zip_", ".zip");
            // Kopieer de entry data naar het tijdelijke bestand zonder de hoofdstream te sluiten
            try (InputStream nestedZipStream = new NonClosingInputStream(zis)) {
                Files.copy(nestedZipStream, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            System.err.println("INFO: Verwerk geneste ZIP: " + fullEntryPath);
            // Roep DEZELFDE methode (processSource) aan voor het tijdelijke bestand (Recursie!)
            this.processSource(tempZip, fullEntryPath, results, errors, ignored);
        } catch (IOException e) {
            errors.add(new ProcessingError(fullEntryPath, "Kon geneste ZIP niet verwerken: " + e.getMessage()));
            System.err.println("FOUT: Verwerken geneste ZIP '" + fullEntryPath + "' mislukt: " + e.getMessage());
        } finally {
            // Ruim altijd het tijdelijke bestand op
            if (tempZip != null) {
                try { Files.deleteIfExists(tempZip); } catch (IOException e) { /* Log waarschuwing */ }
            }
        }
    }

    /** Verwerkt een normaal bestand binnen de ZIP. */
    private void processRegularEntry(String entryName, ZipInputStream zis, String fullEntryPath, List<CkanResource> results, List<ProcessingError> errors) {
        try {
            // Gebruik NonClosingInputStream om de hoofdstream open te houden
            InputStream nonClosingStream = new NonClosingInputStream(zis);
            // Stap 1: Extraheer metadata en tekst
            IMetadataProvider.ExtractionOutput output = metadataProvider.extract(nonClosingStream, config.getMaxExtractedTextLength());
            // Stap 2: Formatteer de output naar een CkanResource
            CkanResource resource = resourceFormatter.format(entryName, output.metadata, output.text, fullEntryPath);
            results.add(resource); // Voeg toe aan succesvolle resultaten
        } catch (Exception e) { // Vang alle fouten tijdens extractie/formattering
            errors.add(new ProcessingError(fullEntryPath, e.getClass().getSimpleName() + ": " + e.getMessage()));
            System.err.println("FOUT: Kon entry niet verwerken '" + fullEntryPath + "': " + e.getMessage());
            // e.printStackTrace(); // Eventueel aanzetten voor meer debug info
        }
    }

    /** Controleert of een entry een ZIP bestand is. */
    private boolean isZipEntryZip(ZipEntry entry) {
        if (entry == null || entry.isDirectory()) return false;
        String lowerCaseName = entry.getName().toLowerCase();
        return config.getSupportedZipExtensions().stream().anyMatch(lowerCaseName::endsWith);
    }

    /** Controleert of een pad ongeldige tekens bevat (simpele check). */
    private boolean isInvalidPath(String entryName) {
        // Basis check om path traversal te voorkomen
        return entryName.contains("..") || Paths.get(entryName).normalize().isAbsolute();
    }
}

// --- Hoofd Klasse: De Facade ---
/**
 * Dit is de hoofdklasse die het hele proces aanstuurt (Facade Pattern).
 * Het gebruikt de andere componenten (filter, provider, formatter, processor)
 * om het werk te doen.
 */
public class MetadataExtractor {

    // Houd referenties naar de componenten die nodig zijn
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ISourceProcessor sourceProcessor; // Specifiek voor ZIPs/bestanden
    private final ExtractorConfiguration config;

    /** Constructor waar alle benodigde componenten worden "geïnjecteerd". */
    public MetadataExtractor(IFileTypeFilter fileFilter,
                             IMetadataProvider metadataProvider,
                             ICkanResourceFormatter resourceFormatter,
                             ISourceProcessor sourceProcessor,
                             ExtractorConfiguration config) {
        // Sla de ontvangen componenten op in de velden van dit object
        this.fileFilter = Objects.requireNonNull(fileFilter);
        this.metadataProvider = Objects.requireNonNull(metadataProvider);
        this.resourceFormatter = Objects.requireNonNull(resourceFormatter);
        this.sourceProcessor = Objects.requireNonNull(sourceProcessor);
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Startpunt voor het verwerken van een bestand of ZIP.
     * @param sourcePathString Pad naar het bronbestand.
     * @return Een ProcessingReport met alle resultaten.
     */
    public ProcessingReport processSource(String sourcePathString) {
        // Maak lege lijsten om resultaten in te verzamelen
        List<CkanResource> successfulResults = new ArrayList<>();
        List<ProcessingError> processingErrors = new ArrayList<>();
        List<IgnoredEntry> ignoredEntries = new ArrayList<>();
        Path sourcePath;

        try {
            sourcePath = Paths.get(sourcePathString).normalize(); // Maak een Path object

            // --- Eerste controles ---
            if (!Files.exists(sourcePath)) {
                processingErrors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
                System.err.println("FOUT: Bron niet gevonden: " + sourcePathString);
                return new ProcessingReport(successfulResults, processingErrors, ignoredEntries); // Stop hier
            }

            // --- Bepaal type bron en delegeer ---
            if (Files.isDirectory(sourcePath)) {
                ignoredEntries.add(new IgnoredEntry(sourcePathString, "Bron is een map (niet ondersteund)."));
                System.err.println("INFO: Bron is een map, overgeslagen: " + sourcePathString);
            } else if (isZipFile(sourcePath)) {
                // Het is een ZIP: gebruik de sourceProcessor (ZipSourceProcessor)
                sourceProcessor.processSource(sourcePath, sourcePath.toString(), successfulResults, processingErrors, ignoredEntries);
            } else {
                // Het is een enkel bestand: verwerk het direct hier
                processSingleFile(sourcePath, successfulResults, processingErrors, ignoredEntries);
            }
        } catch (Exception e) {
            // Vang onverwachte fouten op het hoogste niveau
            String errorMsg = "Kritieke fout bij verwerken '" + sourcePathString + "': " + e.getMessage();
            processingErrors.add(new ProcessingError(sourcePathString, errorMsg));
            System.err.println("KRITIEKE FOUT: " + errorMsg);
            e.printStackTrace(System.err);
        }

        // Geef het verzamelde rapport terug
        return new ProcessingReport(successfulResults, processingErrors, ignoredEntries);
    }

    /** Verwerkt een enkel (niet-ZIP) bestand. */
    private void processSingleFile(Path sourcePath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        String sourcePathString = sourcePath.toString();
        String filename = sourcePath.getFileName().toString();

        // Controleer eerst of het bestandstype relevant is
        if (fileFilter.isFileTypeRelevant(filename)) {
            // Gebruik try-with-resources om de stream automatisch te sluiten
            try (InputStream stream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
                // Stap 1: Extraheer
                IMetadataProvider.ExtractionOutput output = metadataProvider.extract(stream, config.getMaxExtractedTextLength());
                // Stap 2: Formatteer
                CkanResource resource = resourceFormatter.format(filename, output.metadata, output.text, sourcePathString);
                results.add(resource);
            } catch (Exception e) { // Vang fouten tijdens lezen/extractie/formattering
                errors.add(new ProcessingError(sourcePathString, e.getClass().getSimpleName() + ": " + e.getMessage()));
                System.err.println("FOUT: Kon enkel bestand niet verwerken '" + sourcePathString + "': " + e.getMessage());
            }
        } else {
            ignored.add(new IgnoredEntry(sourcePathString, "Bestandstype genegeerd door filter."));
        }
    }

    /** Controleert of een bestand een ZIP is. */
    private boolean isZipFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) return false;
        String fileName = path.getFileName().toString().toLowerCase();
        return config.getSupportedZipExtensions().stream().anyMatch(fileName::endsWith);
    }


    // --- Main Methode: Startpunt van de Applicatie ---
    /**
     * Dit is waar het programma begint.
     * Het zet alles klaar en start de extractor.
     */
    public static void main(String[] args) {
        System.out.println("--- Metadata Extractor Start ---");

        // --- Stap 1: Bepaal welk bestand verwerkt moet worden ---
        String defaultFilePath = "C:\\Users\\gurelb\\Downloads\\Veg kartering - habitatkaart 2021-2023.zip";

        String filePath = defaultFilePath;
        if (args.length > 0) { // Kijk of een pad is meegegeven als argument
            filePath = args[0];
            System.out.println("INFO: Gebruik pad uit argument: " + filePath);
        } else {
            System.out.println("INFO: Geen argument opgegeven, gebruik standaard pad: " + filePath);
            System.err.println("WAARSCHUWING: Zorg dat het standaard pad correct is!");
        }

        // --- Stap 2: Maak de benodigde objecten (Dependencies) ---
        // Maak het configuratie object
        ExtractorConfiguration config = new ExtractorConfiguration();

        // Probeer de taaldetector te laden (kan mislukken als library mist)
        LanguageDetector loadedLanguageDetector = loadLanguageDetector(); // Roep helper methode aan

        // Maak de componenten en geef de configuratie (en detector) mee
        // Dit heet Dependency Injection: we geven de benodigde objecten aan de klassen die ze gebruiken.
        IFileTypeFilter filter = new DefaultFileTypeFilter(config);
        IMetadataProvider provider = new TikaMetadataProvider(); // Gebruikt standaard Tika parser
        ICkanResourceFormatter formatter = new DefaultCkanResourceFormatter(loadedLanguageDetector, config);
        // Voor ISourceProcessor gebruiken we de ZipSourceProcessor, die ook losse bestanden (via de facade) kan aanroepen
        ISourceProcessor sourceProcessor = new ZipSourceProcessor(filter, provider, formatter, config);

        // Maak de hoofd extractor en geef alle componenten mee
        MetadataExtractor extractor = new MetadataExtractor(filter, provider, formatter, sourceProcessor, config);

        // --- Stap 3: Start de verwerking ---
        System.out.println("INFO: Start verwerking voor: " + filePath);
        ProcessingReport report = extractor.processSource(filePath); // Roep de hoofd verwerkingsmethode aan
        System.out.println("INFO: Verwerking voltooid.");

        // --- Stap 4: Verwerk en toon de resultaten ---
        System.out.println("\n--- Resultaten ---");
        System.out.println("Aantal succesvolle resources: " + report.getResults().size());
        System.out.println("Aantal genegeerde entries: " + report.getIgnored().size());
        System.out.println("Aantal verwerkingsfouten: " + report.getErrors().size());

        // Print details van fouten (indien aanwezig)
        if (!report.getErrors().isEmpty()) {
            System.err.println("\n--- Foutdetails ---");
            report.getErrors().forEach(err -> System.err.println("  - [" + err.getSource() + "]: " + err.getError()));
        }

        // Print JSON output van de succesvolle resources
        if (!report.getResults().isEmpty()) {
            System.out.println("\n--- Succesvolle Resources (JSON) ---");
            try {
                // Gebruik Jackson ObjectMapper om de lijst van CkanResource objecten om te zetten naar JSON
                ObjectMapper jsonMapper = new ObjectMapper()
                        .enable(SerializationFeature.INDENT_OUTPUT); // Zorgt voor netjes ingesprongen JSON
                // We moeten de lijst van CkanResource's omzetten naar een lijst van hun data Maps voor serialisatie
                List<Map<String, Object>> resultsData = report.getResults().stream()
                        .map(CkanResource::getData) // Haal de Map uit elke CkanResource
                        .collect(Collectors.toList()); // Verzamel in een nieuwe lijst
                String jsonOutput = jsonMapper.writeValueAsString(Map.of( // Maak een top-level JSON object
                        "results", resultsData
                        // Je kunt hier ook errors en ignored toevoegen indien gewenst
                        // "errors", report.getErrors(),
                        // "ignored", report.getIgnored()
                ));
                System.out.println(jsonOutput);
            } catch (JsonProcessingException e) {
                System.err.println("FOUT: Kon resultaten niet naar JSON converteren: " + e.getMessage());
            }
        }

        System.out.println("\n--- Metadata Extractor Klaar ---");
    }

    /** Helper methode om de taaldetector te laden. */
    private static LanguageDetector loadLanguageDetector() {
        try {
            System.out.println("INFO: Laden van taalmodellen...");
            // Gebruikt Tika's Optimaize detector
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector();
            detector.loadModels(); // Laad de modellen
            System.out.println("INFO: Taalmodellen geladen.");
            return detector;
        } catch (NoClassDefFoundError e) {
            System.err.println("FOUT: Tika taal detectie library (tika-langdetect) niet gevonden. Taaldetectie uitgeschakeld.");
            System.err.println("Voeg 'org.apache.tika:tika-langdetect' toe aan je dependencies.");
        } catch (IOException e) {
            System.err.println("FOUT: Kon taalmodellen niet laden: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("FOUT: Onverwachte fout bij laden taalmodellen: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        // Als er een fout was, geef null terug
        System.err.println("WAARSCHUWING: Taaldetectie is uitgeschakeld.");
        return null;
    }

} // Einde van MetadataExtractor klasse