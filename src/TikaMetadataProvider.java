import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

// IMetadataProvider implementation using Apache Tika for metadata and text extraction.
public class TikaMetadataProvider implements IMetadataProvider {
    private final Parser tikaParser; // The Apache Tika parser instance.

    // Default constructor: uses Tika's AutoDetectParser.
    public TikaMetadataProvider() {
        this(new AutoDetectParser());
    }

    // Constructor allowing injection of a specific Tika Parser.
    public TikaMetadataProvider(Parser parser) {
        this.tikaParser = Objects.requireNonNull(parser, "Tika Parser mag niet null zijn");
    }

    // Extracts metadata and text from an InputStream using the configured Tika parser.
    // Text extraction is limited by maxTextLength.
    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception { // Method signature from IMetadataProvider

        // BodyContentHandler helps limit the amount of text buffered in memory.
        BodyContentHandler contentHandler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata(); // Tika's metadata container.
        ParseContext parseContext = new ParseContext(); // Context for the parsing operation.

        try {
            // Execute Tika parsing.
            tikaParser.parse(inputStream, contentHandler, metadata, parseContext);
            // Return results packaged in ExtractionOutput.
            return new ExtractionOutput(metadata, contentHandler.toString());
        } catch (Exception e) {
            // Log the error and rethrow it, allowing the caller to handle.
            System.err.println("Fout tijdens Tika parsing: " + e.getMessage());
            throw e; // Preserves original exception for upstream handling.
        }
    }
}