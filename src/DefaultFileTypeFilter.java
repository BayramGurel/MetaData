import java.util.Objects;

/**
 * Standaard implementatie van {@link IFileTypeFilter}.
 * Filtert bestanden/entries op basis van configuratie.
 */
public class DefaultFileTypeFilter implements IFileTypeFilter {

    private final ExtractorConfiguration config;

    /** Constructor, injecteert configuratie. */
    public DefaultFileTypeFilter(ExtractorConfiguration config) {
        this.config = Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    /** Bepaalt of een bestand/entry relevant is voor verwerking. */
    @Override
    public boolean isFileTypeRelevant(String entryName) {
        // Basis validiteit check
        if (entryName == null || entryName.isBlank()) {
            return false;
        }

        // Extraheer bestandsnaam (deel na laatste slash)
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName);
        if (filename.isEmpty()) {
            return false; // Waarschijnlijk een map entry eindigend op '/'
        }

        String lowerFilename = filename.toLowerCase(); // Voor case-insensitive checks

        // Check tegen negeerlijsten uit configuratie
        if (config.getIgnoredPrefixes().stream().anyMatch(lowerFilename::startsWith)) {
            return false; // Genegeerde prefix gevonden
        }
        if (config.getIgnoredFilenames().stream().anyMatch(lowerFilename::equalsIgnoreCase)) {
            return false; // Genegeerde bestandsnaam gevonden
        }

        // Check genegeerde extensies (indien extensie bestaat)
        int lastDotIndex = lowerFilename.lastIndexOf('.');
        // Zeker stellen dat punt bestaat, niet eerste teken is, en tekens erna heeft
        if (lastDotIndex > 0 && lastDotIndex < lowerFilename.length() - 1) {
            String extension = lowerFilename.substring(lastDotIndex); // Inclusief de punt
            if (config.getIgnoredExtensions().contains(extension)) {
                return false; // Genegeerde extensie gevonden
            }
        }

        // Geen negeer-regel gevonden -> bestand is relevant
        return true;
    }
}
