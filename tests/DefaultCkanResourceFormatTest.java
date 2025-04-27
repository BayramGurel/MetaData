
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultCkanResourceFormatTest {

    private ExtractorConfiguration config;
    private DefaultCkanResourceFormat formatter;

    private static final String TEST_INGANG_NAAM = "pad/naar/document.pdf";
    private static final String TEST_BESTANDSNAAM = "document.pdf";
    private static final String TEST_BRON_ID = "test_container!/pad/naar/document.pdf";
    private static final String NAL_URI_BASIS = "http://publications.europa.eu/resource/authority/language/";

    @BeforeEach
    void stelIn() {
        config = new ExtractorConfiguration();
        formatter = new DefaultCkanResourceFormat(null, config);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> verkrijgExtras(CkanResource resource) {
        Object extrasObj = resource.getData().get("extras");
        return (extrasObj instanceof Map) ? (Map<String, String>) extrasObj : null;
    }

    @Test
    void formatteerBasisMetadataEnTitelSchoonmaak() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.TITLE, "Microsoft Word - Test Titel");
        metadata.set(Metadata.CONTENT_TYPE, "application/pdf");
        metadata.set(TikaCoreProperties.CREATOR, "Test Auteur");

        CkanResource resource = formatter.format(TEST_INGANG_NAAM, metadata, "Tekst inhoud", TEST_BRON_ID);
        Map<String, Object> data = resource.getData();
        Map<String, String> extras = verkrijgExtras(resource);

        assertEquals("Test Titel", data.get("name")); // Titel opgeschoond
        assertEquals("PDF", data.get("format"));
        assertEquals("application/pdf", data.get("mimetype"));
        assertNotNull(extras);
        assertEquals(TEST_BRON_ID, extras.get("source_identifier"));
        assertEquals(TEST_INGANG_NAAM, extras.get("original_entry_name"));
        assertEquals(TEST_BESTANDSNAAM, extras.get("original_filename"));
        assertEquals("Test Auteur", extras.get("creator"));
        assertEquals(NAL_URI_BASIS + "UND", extras.get("language_uri")); // Fallback taal
    }

    @Test
    void formatteerNaamFallbackNaarBestandsnaam() {
        Metadata metadataLeeg = new Metadata(); // Geen titel
        CkanResource resource1 = formatter.format(TEST_INGANG_NAAM, metadataLeeg, "", TEST_BRON_ID);
        assertEquals(TEST_BESTANDSNAAM, resource1.getData().get("name"));

        Metadata metadataBlank = new Metadata();
        metadataBlank.set(TikaCoreProperties.TITLE, "   "); // Lege titel
        CkanResource resource2 = formatter.format(TEST_INGANG_NAAM, metadataBlank, "", TEST_BRON_ID);
        assertEquals(TEST_BESTANDSNAAM, resource2.getData().get("name"));
    }

    @Test
    void formatteerBeschrijvingFallbackNaarGeneriek() {
        Metadata metadata = new Metadata();
        CkanResource resource = formatter.format(TEST_INGANG_NAAM, metadata, "Kort.", TEST_BRON_ID);
        assertEquals("Geen beschrijving beschikbaar voor: " + TEST_BESTANDSNAAM, resource.getData().get("description"));
    }

    @Test
    void formatteerDatumsCorrect() {
        Metadata metadata = new Metadata();
        Instant aangemaakt = OffsetDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC).toInstant();
        Instant gewijzigd = OffsetDateTime.of(2023, 1, 20, 12, 30, 0, 0, ZoneOffset.ofHours(1)).toInstant();

        metadata.set(TikaCoreProperties.CREATED, aangemaakt.toString());
        metadata.set(TikaCoreProperties.MODIFIED, DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(gewijzigd.atOffset(ZoneOffset.ofHours(1))));

        CkanResource resource = formatter.format(TEST_INGANG_NAAM, metadata, "", TEST_BRON_ID);
        Map<String, Object> data = resource.getData();

        assertEquals(aangemaakt.toString(), data.get("created"));
        assertEquals(gewijzigd.toString(), data.get("last_modified"));
    }

    @Test
    void formatteerVerwerktOntbrekendeDatums() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CREATED, "ongeldig-datum-format");

        CkanResource resource = formatter.format(TEST_INGANG_NAAM, metadata, "", TEST_BRON_ID);
        Map<String, Object> data = resource.getData();

        assertNull(data.get("created"));
        assertNull(data.get("last_modified"));
    }

    @Test
    void formatteerTaalFallbackNaarUndBijNullDetector() {
        Metadata metadata = new Metadata();
        CkanResource resource = formatter.format(TEST_INGANG_NAAM, metadata, "Willekeurige tekst.", TEST_BRON_ID);
        Map<String, String> extras = verkrijgExtras(resource);

        assertNotNull(extras);
        assertEquals(NAL_URI_BASIS + "UND", extras.get("language_uri"));
    }

    @ParameterizedTest(name = "[{index}] ContentType={0} -> Format={1}, MimeType={2}")
    @CsvSource({
            "'application/vnd.openxmlformats-officedocument.wordprocessingml.document', DOCX, application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "'text/csv; charset=UTF-8',                                                 CSV,  text/csv",
            "'image/jpeg',                                                               JPEG, image/jpeg",
            "'application/shp',                                                          SHP,  application/shp"
    })
    void formatteerBekendeContentTypes(String contentType, String expectedFormat, String expectedMimeType) {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, contentType);
        CkanResource resource = formatter.format("test" + expectedFormat.toLowerCase(), metadata, "", "id");

        assertEquals(expectedFormat, resource.getData().get("format"));
        assertEquals(expectedMimeType, resource.getData().get("mimetype"));
    }

    @Test
    void formatteerOnbekendeContentTypes() {
        Metadata metadataLeeg = new Metadata();
        CkanResource resourceLeeg = formatter.format("a.xyz", metadataLeeg, "", "id");
        assertEquals("Unknown", resourceLeeg.getData().get("format"));
        assertNull(resourceLeeg.getData().get("mimetype"));

        Metadata metadataOctet = new Metadata();
        metadataOctet.set(Metadata.CONTENT_TYPE, "application/octet-stream");
        CkanResource resourceOctet = formatter.format("a.bin", metadataOctet, "", "id");
        assertTrue(resourceOctet.getData().get("format").toString().contains("OCTETSTREAM"));
        assertEquals("application/octet-stream", resourceOctet.getData().get("mimetype"));

        Metadata metadataBlank = new Metadata();
        metadataBlank.set(Metadata.CONTENT_TYPE, "");
        CkanResource resourceBlank = formatter.format("a.xyz", metadataBlank, "", "id");
        assertEquals("Unknown", resourceBlank.getData().get("format"));
        assertNull(resourceBlank.getData().get("mimetype"));
    }
}