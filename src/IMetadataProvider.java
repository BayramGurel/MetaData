import java.io.IOException;
import java.io.InputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

interface IMetadataProvider {
    ExtractionOutput extract(InputStream var1, int var2) throws IOException, TikaException, SAXException, Exception;

    public static record ExtractionOutput(Metadata metadata, String text) {
    }
}
