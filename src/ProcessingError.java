import java.util.Objects;

/**
 * Record capturing a processing error for a source.
 */
record ProcessingError(String source, String error) {
    public ProcessingError {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(error, "Foutmelding mag niet null zijn");
        if (source.isBlank()) {
            throw new IllegalArgumentException("Bron mag niet leeg zijn");
        } else if (error.isBlank()) {
            throw new IllegalArgumentException("Foutmelding mag niet leeg zijn");
        }
    }
}
