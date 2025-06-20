import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

// Represents an error encountered during processing, referencing a source and an error message.
record ProcessingError(String source, String error) {
    // Compact constructor to validate that source and error are non-null and non-blank.
    public ProcessingError {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(error, "Foutmelding mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (error.isBlank()) throw new IllegalArgumentException("Foutmelding mag niet leeg zijn");
    }
}

// Represents an entry that was ignored during processing, with a source and a reason for ignoring.
record IgnoredEntry(String source, String reason) {
    // Compact constructor to validate that source and reason are non-null and non-blank.
    public IgnoredEntry {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(reason, "Reden mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (reason.isBlank()) throw new IllegalArgumentException("Reden mag niet leeg zijn");
    }
}

// Holds the consolidated results of a processing operation, including successful items,
// errors encountered, and items that were ignored. This class is immutable.
public final class ProcessingReport {
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    // Constructs a ProcessingReport, creating immutable copies of the provided lists.
    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Objects.requireNonNull(results, "Lijst met resultaten mag niet null zijn");
        Objects.requireNonNull(errors, "Lijst met fouten mag niet null zijn");
        Objects.requireNonNull(ignored, "Lijst met genegeerde items mag niet null zijn");

        // Use defensive copying and unmodifiable wrappers to ensure immutability
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }

    // Returns an unmodifiable list of successfully processed CkanResources.
    public List<CkanResource> getResults() {
        return results;
    }

    // Returns an unmodifiable list of processing errors.
    public List<ProcessingError> getErrors() {
        return errors;
    }

    // Returns an unmodifiable list of ignored entries.
    public List<IgnoredEntry> getIgnored() {
        return ignored;
    }

    @Override
    public String toString() {
        // Provides a summary string with counts (Dutch terms used).
        return "ProcessingReport{" +
                "resultaten=" + results.size() +   // "results"
                ", fouten=" + errors.size() +      // "errors"
                ", genegeerd=" + ignored.size() +  // "ignored"
                '}';
    }
}
