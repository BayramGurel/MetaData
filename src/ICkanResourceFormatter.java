import org.apache.tika.metadata.Metadata;

/**
 * Formatter contract for converting extracted metadata and text into a CKAN resource.
 */
public interface ICkanResourceFormatter {
    CkanResource format(
            String entryName,
            Metadata metadata,
            String text,
            String sourceIdentifier
    );
}
