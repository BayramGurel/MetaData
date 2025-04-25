import org.apache.tika.language.detect.LanguageDetector; // Still needed for constructor signature
// Mockito imports removed
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
// Mockito imports removed

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
// Mockito imports removed

/**
 * Unit tests for the DefaultCkanResourceFormat class.
 * Does NOT use Mockito; passes null for LanguageDetector.
 */
// @ExtendWith removed
class DefaultCkanResourceFormatTest {

    // @Mock removed
    // private LanguageDetector mockLanguageDetector;

    private ExtractorConfiguration config;
    private DefaultCkanResourceFormat formatter; // Only one formatter instance needed now

    private static final String TEST_ENTRY_NAME = "path/to/document.pdf";
    private static final String TEST_FILENAME = "document.pdf";
    private static final String TEST_SOURCE_ID = "test_container!/path/to/document.pdf";
    private static final String TEST_TEXT_NL = "Dit is Nederlandse tekst voor taaldetectie.";
    private static final String TEST_TEXT_EN = "This is English text for language detection.";
    private static final String NAL_URI_BASE = "http://publications.europa.eu/resource/authority/language/";

    @BeforeEach
    void setUp() {
        config = new ExtractorConfiguration(); // Use default config

        // Create formatter with null LanguageDetector
        // The class handles null detector internally
        formatter = new DefaultCkanResourceFormat(null, config);
    }

    // Helper to get the nested 'extras' map safely
    @SuppressWarnings("unchecked")
    private Map<String, String> getExtras(CkanResource resource) {
        Object extrasObj = resource.getData().get("extras");
        if (extrasObj instanceof Map) {
            return (Map<String, String>) extrasObj;
        }
        return null;
    }

    @Test
    @DisplayName("Format basic metadata correctly")
    void format_BasicMetadata_CreatesCorrectResource() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.TITLE, "Test Title");
        metadata.set(Metadata.CONTENT_TYPE, "application/pdf");
        metadata.set(TikaCoreProperties.CREATOR, "Test Author");

        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, "Some text content", TEST_SOURCE_ID);
        Map<String, Object> data = resource.getData();
        Map<String, String> extras = getExtras(resource);

        assertEquals("Test Title", data.get("name"));
        assertEquals("PDF", data.get("format"));
        assertEquals("application/pdf", data.get("mimetype"));
        assertNotNull(extras);
        assertEquals(TEST_SOURCE_ID, extras.get("source_identifier"));
        assertEquals(TEST_ENTRY_NAME, extras.get("original_entry_name"));
        assertEquals(TEST_FILENAME, extras.get("original_filename"));
        assertEquals("Test Author", extras.get("creator"));
        // Check language fallback when detector is null
        assertEquals(NAL_URI_BASE + "UND", extras.get("language_uri"));
    }

    @Test
    @DisplayName("Format cleans title prefix")
    void format_TitleCleaning_RemovesPrefix() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.TITLE, "Microsoft Word - Important Document");

        CkanResource resource = formatter.format("doc.docx", metadata, "", "id1");
        assertEquals("Important Document", resource.getData().get("name"));
    }

    @Test
    @DisplayName("Format uses filename as name if title is missing or blank")
    void format_TitleFallback_UsesFilename() {
        Metadata metadata = new Metadata(); // No title
        CkanResource resource1 = formatter.format(TEST_ENTRY_NAME, metadata, "", TEST_SOURCE_ID);
        assertEquals(TEST_FILENAME, resource1.getData().get("name"));

        metadata.set(TikaCoreProperties.TITLE, "   "); // Blank title
        CkanResource resource2 = formatter.format(TEST_ENTRY_NAME, metadata, "", TEST_SOURCE_ID);
        assertEquals(TEST_FILENAME, resource2.getData().get("name"));
    }

    @Test
    @DisplayName("Format uses generic description as final fallback")
    void format_DescriptionFallback_UsesGeneric() {
        Metadata metadata = new Metadata(); // No description
        String shortText = "Short."; // Text too short to make snippet

        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, shortText, TEST_SOURCE_ID);
        assertEquals("Geen beschrijving beschikbaar voor: " + TEST_FILENAME, resource.getData().get("description"));
    }

    @Test
    @DisplayName("Format populates created and modified dates correctly")
    void format_Dates_PopulatesCreatedAndModified() {
        Metadata metadata = new Metadata();
        Instant createdInstant = OffsetDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC).toInstant();
        Instant modifiedInstant = OffsetDateTime.of(2023, 1, 20, 12, 30, 0, 0, ZoneOffset.ofHours(1)).toInstant(); // UTC+1

        metadata.set(TikaCoreProperties.CREATED, createdInstant.toString()); // ISO_INSTANT format
        metadata.set(TikaCoreProperties.MODIFIED, DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(modifiedInstant.atOffset(ZoneOffset.ofHours(1)))); // ISO_OFFSET_DATE_TIME format

        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, "", TEST_SOURCE_ID);
        Map<String, Object> data = resource.getData();

        assertEquals(createdInstant.toString(), data.get("created"));
        assertEquals(modifiedInstant.toString(), data.get("last_modified"));
    }

    @Test
    @DisplayName("Format handles missing or unparseable dates")
    void format_Dates_HandlesMissingDates() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CREATED, "invalid-date-format");
        // No modified date

        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, "", TEST_SOURCE_ID);
        Map<String, Object> data = resource.getData();

        assertNull(data.get("created"));
        assertNull(data.get("last_modified"));
    }

    // --- Language Detection Tests (Now only testing fallback with null detector) ---

    @Test
    @DisplayName("Format adds UND language URI when LanguageDetector is null")
    void format_LanguageDetection_HandlesNullDetector() {
        Metadata metadata = new Metadata();
        // Use the formatter created with a null detector in setUp()
        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, TEST_TEXT_EN, TEST_SOURCE_ID);
        Map<String, String> extras = getExtras(resource);

        assertNotNull(extras);
        assertEquals(NAL_URI_BASE + "UND", extras.get("language_uri"), "Should default to UND when detector is null");
    }

    @Test
    @DisplayName("Format adds UND language URI even with text when LanguageDetector is null")
    void format_LanguageDetection_AddsUndeterminedUri_WithTextAndNullDetector() {
        Metadata metadata = new Metadata();
        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, TEST_TEXT_NL, TEST_SOURCE_ID);
        Map<String, String> extras = getExtras(resource);

        assertNotNull(extras);
        assertEquals(NAL_URI_BASE + "UND", extras.get("language_uri"), "Should default to UND with text when detector is null");
    }

    @Test
    @DisplayName("Format adds UND language URI with empty text when LanguageDetector is null")
    void format_LanguageDetection_AddsUndeterminedUri_EmptyTextAndNullDetector() {
        Metadata metadata = new Metadata();
        CkanResource resource = formatter.format(TEST_ENTRY_NAME, metadata, "", TEST_SOURCE_ID);
        Map<String, String> extras = getExtras(resource);

        assertNotNull(extras);
        assertEquals(NAL_URI_BASE + "UND", extras.get("language_uri"), "Should default to UND with empty text when detector is null");
    }


    // --- Content Type Mapping Tests ---

    @Test
    @DisplayName("Format maps content types correctly")
    void format_ContentTypeMapping_MapsCorrectly() {
        Metadata metadata = new Metadata();

        metadata.set(Metadata.CONTENT_TYPE, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        assertEquals("DOCX", formatter.format("a.docx", metadata, "", "id").getData().get("format"));

        metadata.set(Metadata.CONTENT_TYPE, "text/csv; charset=UTF-8");
        assertEquals("CSV", formatter.format("a.csv", metadata, "", "id").getData().get("format"));
        assertEquals("text/csv", formatter.format("a.csv", metadata, "", "id").getData().get("mimetype"));


        metadata.set(Metadata.CONTENT_TYPE, "image/jpeg");
        assertEquals("JPEG", formatter.format("a.jpg", metadata, "", "id").getData().get("format"));
    }

    @Test
    @DisplayName("Format handles unknown or missing content type")
    void format_ContentTypeMapping_HandlesUnknown() {
        Metadata metadata = new Metadata(); // No content type
        assertEquals("Unknown", formatter.format("a.xyz", metadata, "", "id").getData().get("format"));
        assertNull(formatter.format("a.xyz", metadata, "", "id").getData().get("mimetype"));

        metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream"); // Generic binary
        // Fallback logic might generate OCTETSTREAM or similar based on the last part
        assertTrue(formatter.format("a.bin", metadata, "", "id").getData().get("format").toString().contains("OCTETSTREAM"));
        assertEquals("application/octet-stream", formatter.format("a.bin", metadata, "", "id").getData().get("mimetype"));

        metadata.set(Metadata.CONTENT_TYPE, ""); // Blank content type
        assertEquals("Unknown", formatter.format("a.xyz", metadata, "", "id").getData().get("format"));
        assertNull(formatter.format("a.xyz", metadata, "", "id").getData().get("mimetype"));
    }
}
