import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
// import org.mockito.Mockito; // Keep if needed later

import org.apache.tika.metadata.Metadata; // Needed for dummy IMetadataProvider
import java.io.InputStream; // Needed for dummy IMetadataProvider
import java.nio.file.Path;
import java.util.List; // Required for dummy implementation & TestProcessor

import static org.junit.jupiter.api.Assertions.*;


/**
 * Unit tests for AbstractSourceProcessor helper methods, using simple individual tests.
 */
class AbstractSourceProcessorTest {

    // --- Test Implementation ---
    // Minimal concrete subclass to instantiate and test the non-static isInvalidPath method.
    private static class TestProcessor extends AbstractSourceProcessor {
        // Constructor now uses dummy implementations instead of null
        protected TestProcessor() {
            super(
                    // Dummy IFileTypeFilter
                    (entryName) -> true, // Default to relevant for dummy
                    // Dummy IMetadataProvider
                    (inputStream, maxTextLength) -> new IMetadataProvider.ExtractionOutput(new Metadata(), ""), // Return empty dummy data
                    // Dummy ICkanResourceFormatter
                    (entryName, metadata, text, sourceIdentifier) -> null, // Return null CkanResource for dummy
                    // Default ExtractorConfiguration
                    new ExtractorConfiguration()
            );
        }

        // Dummy implementation - not called in these tests
        @Override
        public void processSource(Path sourcePath, String containerPath, List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
            // No-op
        }
    }

    // Single instance is sufficient for testing isInvalidPath
    private final TestProcessor testProcessor = new TestProcessor();

    // --- Tests for getFilenameFromEntry (static method) ---

    @Test
    @DisplayName("getFilenameFromEntry: Null input")
    void getFilenameFromEntry_Null() {
        assertEquals("", AbstractSourceProcessor.getFilenameFromEntry(null));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Empty input")
    void getFilenameFromEntry_Empty() {
        assertEquals("", AbstractSourceProcessor.getFilenameFromEntry(""));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Blank input")
    void getFilenameFromEntry_Blank() {
        assertEquals("", AbstractSourceProcessor.getFilenameFromEntry("   "));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Simple filename")
    void getFilenameFromEntry_Simple() {
        assertEquals("file.txt", AbstractSourceProcessor.getFilenameFromEntry("file.txt"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Path with forward slashes")
    void getFilenameFromEntry_ForwardSlashes() {
        assertEquals("file.txt", AbstractSourceProcessor.getFilenameFromEntry("path/to/file.txt"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Path with backslashes")
    void getFilenameFromEntry_Backslashes() {
        assertEquals("file.txt", AbstractSourceProcessor.getFilenameFromEntry("path\\to\\file.txt"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Absolute Unix path")
    void getFilenameFromEntry_AbsoluteUnix() {
        assertEquals("file.txt", AbstractSourceProcessor.getFilenameFromEntry("/absolute/path/to/file.txt"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Absolute Windows path")
    void getFilenameFromEntry_AbsoluteWindows() {
        assertEquals("file.txt", AbstractSourceProcessor.getFilenameFromEntry("C:\\absolute\\path\\to\\file.txt"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Directory path ending with slash")
    void getFilenameFromEntry_DirectoryPath() {
        assertEquals("", AbstractSourceProcessor.getFilenameFromEntry("path/to/directory/"));
    }

    @Test
    @DisplayName("getFilenameFromEntry: Filename with spaces")
    void getFilenameFromEntry_WithSpaces() {
        assertEquals("file with spaces.txt", AbstractSourceProcessor.getFilenameFromEntry("path/to/file with spaces.txt"));
    }


    // --- Tests for isInvalidPath (instance method) ---

    @Test
    @DisplayName("isInvalidPath: Null input")
    void isInvalidPath_Null() {
        assertFalse(testProcessor.isInvalidPath(null));
    }

    @Test
    @DisplayName("isInvalidPath: Valid relative path")
    void isInvalidPath_ValidRelative() {
        assertFalse(testProcessor.isInvalidPath("path/to/file.txt"));
    }

    @Test
    @DisplayName("isInvalidPath: Valid simple filename")
    void isInvalidPath_ValidSimple() {
        assertFalse(testProcessor.isInvalidPath("file.txt"));
    }

    @Test
    @DisplayName("isInvalidPath: Contains '..'")
    void isInvalidPath_ContainsDotDot() {
        assertTrue(testProcessor.isInvalidPath("../path/file.txt"));
        assertTrue(testProcessor.isInvalidPath("path/../file.txt"));
    }

    @Test
    @DisplayName("isInvalidPath: Absolute Windows path")
    void isInvalidPath_AbsoluteWindows() {
        assertTrue(testProcessor.isInvalidPath("C:\\absolute\\path\\file.txt"));
    }

    @Test
    @DisplayName("isInvalidPath: Relative path starting with slash (OS Dependent)")
    void isInvalidPath_RelativePathStartingWithSlash_OSDependent() {
        String path = "/relative/path";
        boolean expected = !System.getProperty("os.name").toLowerCase().contains("win"); // True on Unix-like
        assertEquals(expected, testProcessor.isInvalidPath(path), "Test behavior depends on OS");
    }

    @Test
    @DisplayName("isInvalidPath: Invalid syntax (null byte)")
    void isInvalidPath_InvalidSyntaxNullByte() {
        assertTrue(testProcessor.isInvalidPath("path/to/file\0.txt"));
    }

    @Test
    @DisplayName("isInvalidPath: Invalid syntax (colon)")
    void isInvalidPath_InvalidSyntaxColon() {
        // Relies on Paths.get() throwing InvalidPathException
        assertTrue(testProcessor.isInvalidPath("path/to/fi:le.txt"));
    }
}
