import java.io.IOException;
import java.io.InputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

interface IMetadataProvider {
    /**
     * Extract metadata and text from the provided stream.
     *
     * @param inputStream   the content to parse
     * @param maxTextLength maximum length of text to extract
     * @return extracted metadata and text
     */
    ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception;

    /** Simple holder for the parser output. */
    public static record ExtractionOutput(Metadata metadata, String text) { }
}
