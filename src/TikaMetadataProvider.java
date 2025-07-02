import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser;

    public TikaMetadataProvider() {
        this(new AutoDetectParser());
    }

    public TikaMetadataProvider(Parser parser) {
        this.tikaParser = (Parser)Objects.requireNonNull(parser, "Tika Parser mag niet null zijn");
    }

    @Override
    public IMetadataProvider.ExtractionOutput extract(InputStream inputStream,
                                                      int maxTextLength)
            throws IOException, TikaException, SAXException, Exception {
        BodyContentHandler contentHandler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();

        try {
            this.tikaParser.parse(inputStream, contentHandler, metadata, parseContext);
            return new IMetadataProvider.ExtractionOutput(metadata, contentHandler.toString());
        } catch (Exception e) {
            System.err.println("Fout tijdens Tika parsing: " + e.getMessage());
            throw e;
        }
    }
}
