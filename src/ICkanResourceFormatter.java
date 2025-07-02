import org.apache.tika.metadata.Metadata;

/**
 * Contract for formatting metadata into a CKAN resource.
 */
interface ICkanResourceFormatter {
    CkanResource format(String entryName,
                        Metadata metadata,
                        String text,
                        String sourceIdentifier);
}
