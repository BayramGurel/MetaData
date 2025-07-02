import java.io.IOException;
import java.io.InputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

/**
 * Interface for extracting metadata and text from input streams.
 */
interface IMetadataProvider {
    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;

    record ExtractionOutput(Metadata metadata, String text) { }
}
