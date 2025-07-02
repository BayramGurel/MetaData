import org.apache.tika.metadata.Metadata;

/** Formats extracted metadata into a CKAN resource representation. */
interface ICkanResourceFormatter {
    /**
     * @param entryName       the original entry name or filename
     * @param metadata        metadata as produced by the extractor
     * @param text            extracted textual content
     * @param sourceIdentifier identifier used for error reporting
     * @return a CKAN resource ready for serialization
     */
    CkanResource format(String entryName,
                        Metadata metadata,
                        String text,
                        String sourceIdentifier);
}
