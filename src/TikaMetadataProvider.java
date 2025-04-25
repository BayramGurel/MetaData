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

/**
 * Tika-based implementation of {@link IMetadataProvider}.
 * Uses {@link IMetadataProvider.ExtractionOutput}.
 */
public class TikaMetadataProvider implements IMetadataProvider {

    // Note: The ExtractionOutput record is defined within IMetadataProvider interface

    private final Parser tikaParser;

    /** Default constructor using AutoDetectParser. */
    public TikaMetadataProvider() {
        this(new AutoDetectParser());
    }

    /** Constructor for injecting a specific Tika Parser. */
    public TikaMetadataProvider(Parser parser) {
        this.tikaParser = Objects.requireNonNull(parser, "Tika Parser cannot be null");
    }

    /** Extracts metadata and text using Tika. */
    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception {

        // Use BodyContentHandler with text length limit
        BodyContentHandler contentHandler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext(); // Context for parsing configuration

        try {
            // Perform Tika parsing
            tikaParser.parse(inputStream, contentHandler, metadata, parseContext);
            // Return result using the correct ExtractionOutput type from the interface
            return new ExtractionOutput(metadata, contentHandler.toString());
        } catch (Exception e) {
            // Log error and rethrow to allow reporting
            System.err.println("Error during Tika parsing: " + e.getMessage());
            throw e;
        }
        // InputStream is NOT closed here; responsibility of the caller
    }
}
