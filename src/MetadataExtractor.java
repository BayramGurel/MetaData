import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.tika.exception.TikaException;
// BELANGRIJK: Zorg dat de dependency voor tika-langdetect-optimaize correct is toegevoegd!
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.DublinCore; // Gebruikt voor SUBJECT (keywords) en standaard velden
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties; // Gebruikt voor CREATED, MODIFIED, CREATOR_TOOL
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.Instant;
import java.util.*;

/**
 * Extraheert metadata en tekst uit een document met Apache Tika,
 * voert basisverwerking uit (datums, taal, content type mapping),
 * en formatteert de output als JSON als basis voor DCAT metadata.
 *
 * Vereist externe libraries:
 * - Jackson Databind (voor JSON)
 * - Apache Tika Core, Parsers, en Langdetect (bijv. tika-langdetect-optimaize)
 */
public class MetadataExtractor {

    // Formatter voor ISO 8601 (UTC - 'Z') voor RDF/xsd:dateTime
    private static final DateTimeFormatter ISO_8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final String NIET_BESCHIKBAAR = "[Niet beschikbaar]";
    private static final String PLACEHOLDER_URI = "urn:placeholder:vervang-mij";

    // Optioneel: Formatter voor weergave van datums in Nederlands formaat
    private static final DateTimeFormatter NL_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * Hoofdmethode voor metadata-extractie.
     * @param args Command-line argumenten (niet gebruikt in deze versie).
     */
    public static void main(String[] args) {
        // Hardcoded bestandspad (zoals gevraagd)
        String filePathString = "C:\\Users\\gurelb\\Downloads\\datawarehouse\\Veg kartering - habitatkaart 2021-2023\\Veg kartering - habitatkaart 2021-2023\\PZH_WestduinparkEnWapendal_2023_HabitatRapport.pdf";

        System.out.println("Poging tot metadata-extractie van (hardcoded pad): " + filePathString);
        System.out.println("--------------------------------------------------");

        try {
            Path documentPath = Paths.get(filePathString);

            // Controleer bestandstoegang
            if (!Files.exists(documentPath) || !Files.isReadable(documentPath)) {
                System.err.println("Fout: Bestand niet gevonden of niet leesbaar op pad: " + filePathString);
                return;
            }

            // --- Metadata en Tekst Extractie ---
            ExtractionResult extractionResult = extraheerMetadataEnTekst(documentPath);
            Metadata metadata = extractionResult.getMetadata();
            String textContent = extractionResult.getTextContent();

            // --- Basis Feedback naar Console (Optioneel) ---
            System.out.println("Metadata Extractie Succesvol (zie JSON hieronder voor details).");
            System.out.println("Titel (Tika):      " + getMetadataValue(metadata, DublinCore.TITLE, NIET_BESCHIKBAAR) + " <-- Controleer/Vervang!");
            System.out.println("Auteur (Tika):     " + getMetadataValue(metadata, DublinCore.CREATOR, NIET_BESCHIKBAAR) + " <-- Controleer/Vervang (mogelijk username)!");
            System.out.println("Aanmaakdatum (NL): " + formatteerDatumTijd(metadata.get(TikaCoreProperties.CREATED), NL_DATE_TIME_FORMATTER, NIET_BESCHIKBAAR));

            // --- Voorbereiden Gestructureerde Data voor JSON Output ---
            Map<String, Object> jsonData = new LinkedHashMap<>(); // Behoudt volgorde voor leesbaarheid
            jsonData.put("analyse_bronBestand", filePathString); // Pad van het geanalyseerde bestand

            // Basis Tika velden (ruwe waarden waar relevant)
            jsonData.put("tika_title", getMetadataValue(metadata, DublinCore.TITLE, null));
            jsonData.put("tika_creator", getMetadataValue(metadata, DublinCore.CREATOR, null));
            jsonData.put("tika_creator_tool", getMetadataValue(metadata, TikaCoreProperties.CREATOR_TOOL, null));
            jsonData.put("tika_producer", getMetadataValue(metadata, "producer", null)); // PDF Producer
            jsonData.put("tika_content_type", getMetadataValue(metadata, Metadata.CONTENT_TYPE, null));

            // Datums (Ruwe waarde + ISO 8601 voor RDF)
            String createdDateRaw = metadata.get(TikaCoreProperties.CREATED);
            String modifiedDateRaw = metadata.get(TikaCoreProperties.MODIFIED);
            jsonData.put("tika_created_raw", createdDateRaw);
            jsonData.put("suggested_dct_issued", getIso8601DateTime(createdDateRaw, null)); // Voor dct:issued
            jsonData.put("tika_modified_raw", modifiedDateRaw);
            jsonData.put("suggested_dct_modified", getIso8601DateTime(modifiedDateRaw, null)); // Voor dct:modified

            // Content Type Mapping voor DCAT URI's
            String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE, "");
            jsonData.put("suggested_dcat_mediaType_uri", mapContentTypeToIanaUri(contentType));
            jsonData.put("suggested_dct_format_uri", mapContentTypeToEuFileTypeUri(contentType));

            // Taaldetectie (Suggestie voor dct:language)
            LanguageResult detectedLanguage = detectLanguage(textContent);
            if (detectedLanguage != null && detectedLanguage.isReasonablyCertain()) {
                String langCode = detectedLanguage.getLanguage();
                jsonData.put("detected_language_code", langCode);
                jsonData.put("detected_language_certainty", detectedLanguage.getRawScore());
                jsonData.put("suggested_dct_language_uri", mapLanguageCodeToNalUri(langCode));
            } else {
                jsonData.put("detected_language_code", null);
                jsonData.put("detected_language_certainty", null);
                jsonData.put("suggested_dct_language_uri", mapLanguageCodeToNalUri("und")); // Undetermined
            }

            // Keyword/Subject Extractie (Suggestie voor dcat:keyword)
            String[] subjects = metadata.getValues(DublinCore.SUBJECT); // Gebruik dc:subject
            if (subjects != null && subjects.length > 0) {
                List<String> subjectList = Arrays.asList(subjects);
                jsonData.put("tika_subject_or_keywords", subjectList);
                jsonData.put("suggested_dcat_keyword", subjectList); // Kan direct gebruikt worden voor dcat:keyword
            } else {
                jsonData.put("tika_subject_or_keywords", Collections.emptyList());
                jsonData.put("suggested_dcat_keyword", Collections.emptyList());
            }

            // Optioneel: Andere interessante Tika velden toevoegen aan JSON?
            jsonData.put("tika_pdf_version", getMetadataValue(metadata,"pdf:PDFVersion", null));
            jsonData.put("tika_page_count", getMetadataValue(metadata,"xmpTPg:NPages", null));


            // --- Genereer Pretty Printed JSON Output ---
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // Maakt JSON leesbaarder
            String jsonOutput = objectMapper.writeValueAsString(jsonData);

            System.out.println("\n--- Gestructureerde Metadata Output (JSON) ---");
            System.out.println(jsonOutput);
            System.out.println("--------------------------------------------------");
            System.out.println("INFO: Gebruik deze JSON als basis voor het handmatig invullen van het DCAT RDF/XML bestand.");
            System.out.println("      Controleer en corrigeer waarden zoals titel en auteur.");
            System.out.println("      Vul verplichte velden zoals beschrijving, licentie, uitgever, contactpunt, etc. aan.");


            // --- Foutafhandeling (Correcte Volgorde) ---
        } catch (InvalidPathException ipe) { // Specifiek pad probleem
            System.err.println("Fout: Het opgegeven (hardcoded) bestandspad is ongeldig: " + filePathString);
            ipe.printStackTrace();
        } catch (JsonProcessingException e) { // Specifiek JSON probleem (subklasse van IOException)
            System.err.println("Fout bij het genereren van JSON output:");
            e.printStackTrace();
        } catch (IOException e) { // Algemene I/O problemen (bestand lezen, etc.)
            System.err.println("Fout bij het lezen van het bestand of I/O-probleem tijdens parsen:");
            e.printStackTrace();
        } catch (TikaException e) { // Specifiek Tika probleem
            System.err.println("Fout tijdens het Tika parsing proces:");
            e.printStackTrace();
        } catch (SAXException e) { // Specifiek XML parsing probleem
            System.err.println("Fout tijdens SAX parsing (vaak gerelateerd aan XML-gebaseerde formaten):");
            e.printStackTrace();
        } catch (Exception e) { // Vang alle andere onverwachte exceptions (incl. ClassNotFound voor langdetect)
            System.err.println("Er is een onverwachte fout opgetreden (controleer of alle Tika dependencies aanwezig zijn, incl. langdetect):");
            e.printStackTrace();
        }
    }

    /**
     * Extraheert metadata EN tekstinhoud uit het document.
     * Beperkt de hoeveelheid tekst om geheugengebruik te limiteren.
     *
     * @param documentPath Het pad naar het document.
     * @return Een ExtractionResult object met Metadata en tekstinhoud.
     * @throws IOException   Als er een I/O-fout optreedt.
     * @throws TikaException Als er een Tika-specifieke fout optreedt.
     * @throws SAXException  Als er een SAX-parseringsfout optreedt.
     */
    public static ExtractionResult extraheerMetadataEnTekst(Path documentPath) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        // Limiteer de hoeveelheid tekst die in het geheugen wordt gehouden (hier 10MB)
        // Verhoog indien nodig voor taaldetectie op zeer grote documenten, maar wees bewust van geheugengebruik.
        // -1 is ongelimiteerd.
        BodyContentHandler handler = new BodyContentHandler(10 * 1024 * 1024);
        Metadata metadata = new Metadata();
        ParseContext context = new ParseContext();

        try (InputStream stream = Files.newInputStream(documentPath)) {
            parser.parse(stream, handler, metadata, context);
        }
        // Geef zowel metadata als de (mogelijk gelimiteerde) tekst terug
        return new ExtractionResult(metadata, handler.toString());
    }

    // --- Helper Methoden ---

    /**
     * Haalt metadata waarde op; retourneert null indien niet gevonden of leeg (geschikt voor JSON).
     */
    private static String getMetadataValue(Metadata metadata, Property property, String fallbackValue) {
        String waarde = metadata.get(property);
        return (waarde != null && !waarde.trim().isEmpty()) ? waarde.trim() : fallbackValue;
    }

    /**
     * Overload voor String keys; retourneert null indien niet gevonden of leeg (geschikt voor JSON).
     */
    private static String getMetadataValue(Metadata metadata, String key, String fallbackValue) {
        String waarde = metadata.get(key);
        return (waarde != null && !waarde.trim().isEmpty()) ? waarde.trim() : fallbackValue;
    }

    /**
     * Formatteert een datum/tijd string voor weergave (Nederlands formaat).
     * Deze methode is optioneel als de primaire output JSON is.
     */
    private static String formatteerDatumTijd(String datumTijdString, DateTimeFormatter formatter, String standaardWaarde) {
        if (datumTijdString == null || datumTijdString.trim().isEmpty()) {
            return standaardWaarde;
        }
        try {
            return formatter.format(parseBestDateTime(datumTijdString));
        } catch (DateTimeParseException e) {
            System.err.println("Waarschuwing (Weergave): Kon datum/tijd niet parsen: " + datumTijdString);
            return datumTijdString; // Geef origineel terug
        } catch (Exception e) {
            System.err.println("Fout (Weergave): Bij formatteren datum/tijd: " + datumTijdString);
            return datumTijdString; // Geef origineel terug
        }
    }

    /**
     * Formatteert een datum/tijd string naar ISO 8601 (UTC) formaat, geschikt voor xsd:dateTime.
     * Retourneert fallbackValue (bijv. null) bij fouten.
     */
    private static String getIso8601DateTime(String datumTijdString, String fallbackValue) {
        if (datumTijdString == null || datumTijdString.trim().isEmpty()) {
            return fallbackValue;
        }
        try {
            TemporalAccessor temporal = parseBestDateTime(datumTijdString);
            // Converteer naar Instant (UTC) voor ISO 8601 'Z' formaat
            Instant instant;
            if (temporal instanceof Instant) {
                instant = (Instant) temporal;
            } else if (temporal instanceof ZonedDateTime) {
                instant = ((ZonedDateTime) temporal).toInstant();
            } else if (temporal instanceof OffsetDateTime) {
                instant = ((OffsetDateTime) temporal).toInstant();
            } else {
                // Probeer te interpreteren als LocalDateTime in systeem default zone, minder ideaal
                try {
                    instant = ZonedDateTime.of(java.time.LocalDateTime.from(temporal), ZoneId.systemDefault()).toInstant();
                } catch (Exception timeEx) {
                    // Als conversie van het type niet lukt (bv. alleen LocalDate), gebruik epoch start
                    System.err.println("Waarschuwing (ISO): Kon datum type niet direct naar Instant converteren, gebruik epoch: " + datumTijdString);
                    instant = Instant.EPOCH; // Veilige fallback, maar betekent waarschijnlijk dat de input onvolledig was
                }
            }
            return ISO_8601_FORMATTER.format(instant);
        } catch (DateTimeParseException e) {
            System.err.println("Waarschuwing (ISO): Kon datum/tijd niet parsen: " + datumTijdString);
            return fallbackValue; // Geef fallback terug bij parsefout
        } catch (Exception e) {
            System.err.println("Fout (ISO): Bij formatteren datum/tijd: " + datumTijdString);
            e.printStackTrace(); // Log de volledige fout voor debugging
            return fallbackValue; // Geef fallback terug
        }
    }

    /**
     * Probeert een datum/tijd string te parsen met verschillende gangbare (ISO) formaten.
     * Gooit DateTimeParseException als geen enkel formaat past.
     */
    private static TemporalAccessor parseBestDateTime(String datumTijdString) throws DateTimeParseException {
        // Prioriteer formaten met tijdzone informatie
        try { return OffsetDateTime.parse(datumTijdString, DateTimeFormatter.ISO_OFFSET_DATE_TIME); } catch (DateTimeParseException e1) {}
        try { return ZonedDateTime.parse(datumTijdString, DateTimeFormatter.ISO_ZONED_DATE_TIME); } catch (DateTimeParseException e2) {}
        try { return Instant.parse(datumTijdString); } catch (DateTimeParseException e3) {}
        // Probeer formaten zonder expliciete tijdzone
        try { return java.time.LocalDateTime.parse(datumTijdString, DateTimeFormatter.ISO_LOCAL_DATE_TIME); } catch (DateTimeParseException e4) {}
        try { return java.time.LocalDate.parse(datumTijdString, DateTimeFormatter.ISO_LOCAL_DATE); } catch (DateTimeParseException e5) {} // Geeft LocalDate terug

        // Als niets werkt, gooi een duidelijke exception
        throw new DateTimeParseException("Kon datum/tijd string niet parsen met bekende ISO-achtige formaten", datumTijdString, 0);
    }


    /**
     * Mapt een content type string naar een IANA media type URI.
     */
    private static String mapContentTypeToIanaUri(String contentType) {
        if (contentType == null || contentType.isBlank()) return PLACEHOLDER_URI;
        String lowerType = contentType.toLowerCase().split(";")[0].trim(); // Neem alleen deel voor ';'
        return switch (lowerType) {
            case "application/pdf" -> "https://www.iana.org/assignments/media-types/application/pdf";
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.wordprocessingml.document";
            case "application/msword" -> "https://www.iana.org/assignments/media-types/application/msword";
            case "text/plain" -> "https://www.iana.org/assignments/media-types/text/plain";
            case "text/csv" -> "https://www.iana.org/assignments/media-types/text/csv";
            case "image/jpeg", "image/jpg" -> "https://www.iana.org/assignments/media-types/image/jpeg";
            case "image/png" -> "https://www.iana.org/assignments/media-types/image/png";
            case "image/gif" -> "https://www.iana.org/assignments/media-types/image/gif";
            // Voeg hier meer mappings toe indien nodig
            default -> PLACEHOLDER_URI + "-iana-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
        };
    }

    /**
     * Mapt een content type string naar een EU File Type URI.
     */
    private static String mapContentTypeToEuFileTypeUri(String contentType) {
        if (contentType == null || contentType.isBlank()) return PLACEHOLDER_URI;
        String lowerType = contentType.toLowerCase().split(";")[0].trim(); // Neem alleen deel voor ';'
        return switch (lowerType) {
            case "application/pdf" -> "http://publications.europa.eu/resource/authority/file-type/PDF";
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "http://publications.europa.eu/resource/authority/file-type/DOCX";
            case "application/msword" -> "http://publications.europa.eu/resource/authority/file-type/DOC";
            case "text/plain" -> "http://publications.europa.eu/resource/authority/file-type/TXT";
            case "text/csv" -> "http://publications.europa.eu/resource/authority/file-type/CSV";
            case "image/jpeg", "image/jpg" -> "http://publications.europa.eu/resource/authority/file-type/JPEG";
            case "image/png" -> "http://publications.europa.eu/resource/authority/file-type/PNG";
            case "image/gif" -> "http://publications.europa.eu/resource/authority/file-type/GIF";
            // Voeg hier meer mappings toe indien nodig
            default -> PLACEHOLDER_URI + "-eu-filetype-" + lowerType.replaceAll("[^a-zA-Z0-9]", "-");
        };
    }

    /**
     * Mapt een ISO 639-1 taalcode (bijv. "nl") naar een EU NAL URI (Named Authority List).
     */
    private static String mapLanguageCodeToNalUri(String langCode) {
        if (langCode == null || langCode.isBlank()) return "http://publications.europa.eu/resource/authority/language/UND"; // Undetermined
        return switch (langCode.toLowerCase()) {
            case "nl" -> "http://publications.europa.eu/resource/authority/language/NLD";
            case "en" -> "http://publications.europa.eu/resource/authority/language/ENG";
            case "de" -> "http://publications.europa.eu/resource/authority/language/DEU";
            case "fr" -> "http://publications.europa.eu/resource/authority/language/FRA";
            // Voeg meer mappings toe indien nodig (bijv. basis van ISO 639-1 naar ISO 639-3)
            default -> "http://publications.europa.eu/resource/authority/language/MUL"; // Multiple Languages / Other
        };
    }


    /**
     * Detecteert de taal van de opgegeven tekst met Tika.
     * Vereist dat een Tika language detection module (zoals tika-langdetect-optimaize)
     * op de classpath staat.
     *
     * @param text De tekst waarvan de taal gedetecteerd moet worden.
     * @return Een LanguageResult object of null bij fouten of onvoldoende zekerheid.
     */
    private static LanguageResult detectLanguage(String text) {
        if (text == null || text.trim().isEmpty()) {
            System.err.println("Waarschuwing (Taaldetectie): Geen tekst beschikbaar voor analyse.");
            return null;
        }
        try {
            // Probeer Optimaize detector te laden. Kan NoClassDefFoundError geven als dependency mist.
            LanguageDetector detector = OptimaizeLangDetector.getDefaultLanguageDetector(); // Gebruik factory methode
            detector.loadModels(); // Zorg dat modellen geladen zijn (kan intern gecached worden)

            // Gebruik een sample van de tekst voor detectie
            int maxLength = 10000; // Limiet voor performance
            String textSample = text.length() <= maxLength ? text : text.substring(0, maxLength);

            LanguageResult result = detector.detect(textSample);
            // Optioneel: retourneer alleen als redelijk zeker?
            // if (result != null && result.isReasonablyCertain()) {
            //    return result;
            // } else {
            //    return null; // Of retourneer altijd, en laat de caller beslissen.
            // }
            return result;

        } catch (NoClassDefFoundError e) {
            // Duidelijke melding als de dependency mist
            System.err.println("FATAL: Tika language detection module (bv. tika-langdetect-optimaize) niet gevonden!");
            System.err.println("       Voeg de dependency toe aan je project (pom.xml/build.gradle/classpath). Taaldetectie uitgeschakeld.");
            return null;
        }
        catch (Exception e) {
            // Vang andere mogelijke fouten tijdens laden/detecteren
            System.err.println("Waarschuwing (Taaldetectie): Kon taal niet detecteren: " + e.getClass().getName() + " - " + e.getMessage());
            // e.printStackTrace(); // Uncomment voor meer debug info
            return null;
        }
    }

    /**
     * Simpele inner class om de resultaten van de Tika extractie
     * (metadata en tekst) bij elkaar te houden.
     */
    private static class ExtractionResult {
        private final Metadata metadata;
        private final String textContent;

        public ExtractionResult(Metadata metadata, String textContent) {
            this.metadata = metadata;
            this.textContent = (textContent != null) ? textContent : ""; // Zorg dat textContent nooit null is
        }

        public Metadata getMetadata() { return metadata; }
        public String getTextContent() { return textContent; }
    }
}