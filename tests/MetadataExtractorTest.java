import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.tika.metadata.Metadata;

import static org.junit.jupiter.api.Assertions.*;

class MetadataExtractorTest {

    private IFileTypeFilter fileFilter;
    private IMetadataProvider metadataProvider;
    private ICkanResourceFormatter resourceFormatter;
    private ExtractorConfiguration config;
    private MetadataExtractor metadataExtractor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        config = new ExtractorConfiguration();

        fileFilter = new IFileTypeFilter() {
            @Override
            public boolean isFileTypeRelevant(String mimeTypeOrFileName) {
                return true;
            }
        };

        metadataProvider = new IMetadataProvider() {
            @Override
            public ExtractionOutput extract(InputStream stream, int limit) throws Exception {
                if (stream != null) stream.close();
                return new ExtractionOutput(new Metadata(), "TestString");
            }
        };

        resourceFormatter = new ICkanResourceFormatter() {
            @Override
            public CkanResource format(String id, Metadata tikaMetadata, String sourcePath, String containerPath) {
                Map<String, Object> dataMap = Map.of(
                        "sourcePath", sourcePath,
                        "containerPath", containerPath,
                        "id_in", id,
                        "tika_keys_in", tikaMetadata != null ? String.join(",", tikaMetadata.names()) : "none",
                        "formatted_by_test", true);
                return new CkanResource(dataMap);
            }
        };

        metadataExtractor = new MetadataExtractor(
                fileFilter,
                metadataProvider,
                resourceFormatter,
                config
        );
    }

    @Test
    void processSource_whenPathDoesNotExist_shouldReturnError() {
        String nonExistentPath = tempDir.resolve("non_existent_file.txt").toString();
        ProcessingReport report = metadataExtractor.processSource(nonExistentPath);
        assertNotNull(report);
        assertTrue(report.getResults().isEmpty());
        assertEquals(1, report.getErrors().size());
        assertTrue(report.getIgnored().isEmpty());
        assertEquals(nonExistentPath, report.getErrors().get(0).source());
        assertTrue(report.getErrors().get(0).error().contains("Bron niet gevonden"));
    }

    @Test
    void processSource_whenPathIsDirectory_shouldReturnIgnored() throws IOException {
        Path directoryPath = tempDir.resolve("my_test_dir");
        Files.createDirectory(directoryPath);
        String directoryPathString = directoryPath.toString();
        ProcessingReport report = metadataExtractor.processSource(directoryPathString);
        assertNotNull(report);
        assertTrue(report.getResults().isEmpty());
        assertTrue(report.getErrors().isEmpty());
        assertEquals(1, report.getIgnored().size());
        assertEquals(directoryPathString, report.getIgnored().get(0).source());
        assertTrue(report.getIgnored().get(0).reason().contains("Bron is map"));
    }

    @Test
    void processSource_whenPathIsInvalidSyntax_shouldReturnError() {
        String invalidPathString = "invalid\\path*?<>/|\0";
        ProcessingReport report = metadataExtractor.processSource(invalidPathString);
        assertNotNull(report);
        assertTrue(report.getResults().isEmpty());
        assertEquals(1, report.getErrors().size());
        assertTrue(report.getIgnored().isEmpty());
        assertEquals(invalidPathString, report.getErrors().get(0).source());
        assertTrue(report.getErrors().get(0).error().contains("Ongeldige pad syntax"));
    }

    @Test
    void processSource_whenPathIsRegularFile_shouldDelegate() throws IOException {
        Path filePath = tempDir.resolve("document.txt");
        Files.createFile(filePath);
        String filePathString = filePath.toString();
        ProcessingReport report = metadataExtractor.processSource(filePathString);
        assertNotNull(report);
        assertTrue(report.getErrors().isEmpty(), "No errors expected during delegation");
        assertTrue(report.getIgnored().isEmpty(), "No ignored entries expected");
        assertFalse(report.getResults().isEmpty(), "Expected results from successful processing flow");
        assertEquals(1, report.getResults().size());
    }

    @Test
    void processSource_whenPathIsZipFile_shouldDelegate() throws IOException {
        Path zipFilePath = tempDir.resolve("my_archive.zip");
        Files.createFile(zipFilePath);
        String zipFilePathString = zipFilePath.toString();
        ProcessingReport report = metadataExtractor.processSource(zipFilePathString);
        assertNotNull(report);
        assertTrue(report.getErrors().isEmpty(), "No errors expected during delegation");
        assertTrue(report.getIgnored().isEmpty(), "No ignored entries expected");
    }

    @Test
    void processSource_whenPathIsFileWithUnregisteredArchiveExtension_shouldTreatAsRegularFile() throws IOException {
        Path otherFilePath = tempDir.resolve("presentation.pptx");
        Files.createFile(otherFilePath);
        String otherFilePathString = otherFilePath.toString();
        ProcessingReport report = metadataExtractor.processSource(otherFilePathString);
        assertNotNull(report);
        assertTrue(report.getErrors().isEmpty());
        assertTrue(report.getIgnored().isEmpty());
        assertFalse(report.getResults().isEmpty(), "Expected results from successful processing flow");
    }

}