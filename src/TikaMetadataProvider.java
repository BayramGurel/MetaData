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
 * Tika-gebaseerde implementatie van {@link IMetadataProvider}.
 * Gebruikt {@link IMetadataProvider.ExtractionOutput}.
 */
public class TikaMetadataProvider implements IMetadataProvider {

    // Noot: Het ExtractionOutput record is gedefinieerd binnen IMetadataProvider

    private final Parser tikaParser;

    /** Standaard constructor met AutoDetectParser. */
    public TikaMetadataProvider() {
        this(new AutoDetectParser());
    }

    /** Constructor voor injectie van specifieke Tika Parser. */
    public TikaMetadataProvider(Parser parser) {
        this.tikaParser = Objects.requireNonNull(parser, "Tika Parser mag niet null zijn");
    }

    /** Extraheert metadata en tekst met Tika. */
    @Override
    public ExtractionOutput extract(InputStream inputStream, int maxTextLength)
            throws IOException, TikaException, SAXException, Exception {

        // Gebruik BodyContentHandler met tekstlimiet
        BodyContentHandler contentHandler = new BodyContentHandler(maxTextLength);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext(); // Context voor parse configuratie

        try {
            // Voer Tika parsing uit
            tikaParser.parse(inputStream, contentHandler, metadata, parseContext);
            // Retourneer resultaat met correcte ExtractionOutput type
            return new ExtractionOutput(metadata, contentHandler.toString());
        } catch (Exception e) {
            // Log fout en gooi opnieuw op voor rapportage
            System.err.println("Fout tijdens Tika parsing: " + e.getMessage());
            throw e;
        }
        // InputStream wordt hier NIET gesloten; verantwoordelijkheid van aanroeper
    }
}
