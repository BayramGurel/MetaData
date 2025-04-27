import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.tika.metadata.Metadata;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class AbstractSourceProcessorTest {

    private static class TestProcessor extends AbstractSourceProcessor {
        protected TestProcessor() {
            super(
                    (entryName) -> true,
                    (inputStream, maxTextLength) -> new IMetadataProvider.ExtractionOutput(new Metadata(), ""),
                    (entryName, metadata, text, sourceIdentifier) -> null,
                    new ExtractorConfiguration()
            );
        }
        @Override
        public void processSource(Path sp, String cp, List<CkanResource> r, List<ProcessingError> e, List<IgnoredEntry> i) { }
    }

    private final TestProcessor testProcessor = new TestProcessor();


    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    @DisplayName("getFilenameFromEntry: Should return empty for null, empty, or blank input")
    void getFilenameFromEntry_ReturnsEmptyForNullEmptyBlank(String input) {
        assertEquals("", AbstractSourceProcessor.getFilenameFromEntry(input));
    }

    @ParameterizedTest(name = "Input: \"{0}\" -> Expected: \"{1}\"")
    @CsvSource({
            "'file.txt',                   'file.txt'",
            "'path/to/file.txt',           'file.txt'",
            "'path\\to\\file.txt',          'file.txt'",
            "'/absolute/path/to/file.txt', 'file.txt'",
            "'C:\\absolute/path/to/file.txt','file.txt'",
            "'path/to/directory/',         ''",
            "'a/b/c /file spaced.t',       'file spaced.t'"
    })
    @DisplayName("getFilenameFromEntry: Extracts filename correctly from various paths")
    void getFilenameFromEntry_ExtractsName(String input, String expected) {
        assertEquals(expected, AbstractSourceProcessor.getFilenameFromEntry(input));
    }


    @Test
    @DisplayName("isInvalidPath: Should return false for null input")
    void isInvalidPath_NullInput() {
        assertFalse(testProcessor.isInvalidPath(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"path/to/file.txt", "file.txt", "a", "."})
    @DisplayName("isInvalidPath: Should return false for valid relative paths/filenames")
    void isInvalidPath_ValidCases(String path) {
        assertFalse(testProcessor.isInvalidPath(path));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "../path/file.txt",
            "path/../file.txt",
            "C:\\absolute\\path",
            "C:/absolute/path",
            "path/to/file\0.txt",
            "path/fi:le.txt"
    })
    @DisplayName("isInvalidPath: Should return true for explicitly invalid/unsafe paths")
    void isInvalidPath_InvalidCases(String path) {
        assertTrue(testProcessor.isInvalidPath(path));
    }

    @Test
    @DisplayName("isInvalidPath: Should return true for absolute Unix paths (/...) on non-Windows OS")
    void isInvalidPath_AbsoluteUnixPath_OSDependent() {
        String path = "/absolute/path";
        boolean expectedToBeInvalid = !System.getProperty("os.name").toLowerCase().contains("win");
        assertEquals(expectedToBeInvalid, testProcessor.isInvalidPath(path),
                "Test behavior for path starting with '/' depends on OS (should be invalid on Unix-like)");
    }
}