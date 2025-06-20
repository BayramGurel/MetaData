import java.util.Objects;

record IgnoredEntry(String source, String reason) {
    public IgnoredEntry {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(reason, "Reden mag niet null zijn");
        if (source.isBlank()) {
            throw new IllegalArgumentException("Bron mag niet leeg zijn");
        } else if (reason.isBlank()) {
            throw new IllegalArgumentException("Reden mag niet leeg zijn");
        }
    }
}
