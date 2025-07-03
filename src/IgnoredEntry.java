import java.util.Objects;

/**
 * Describes an entry that was ignored during processing, along with the reason.
 *
 * @param source the source or path of the ignored entry; must not be null or blank
 * @param reason the reason why the entry was ignored; must not be null or blank
 */
public record IgnoredEntry(String source, String reason) {
    public IgnoredEntry {
        Objects.requireNonNull(source, "Source must not be null");
        Objects.requireNonNull(reason, "Reason must not be null");
        if (source.isBlank()) {
            throw new IllegalArgumentException("Source must not be blank");
        }
        if (reason.isBlank()) {
            throw new IllegalArgumentException("Reason must not be blank");
        }
    }
}
