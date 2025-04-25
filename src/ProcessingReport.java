import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

// --- Reporting DTOs ---

/**
 * Represents an error encountered during processing. Immutable record.
 * @param source Path where the error occurred.
 * @param error Error description.
 */
record ProcessingError(String source, String error) {
    public ProcessingError { // Compact constructor validation
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(error, "Error message cannot be null");
        if (source.isBlank()) throw new IllegalArgumentException("Source cannot be blank");
        if (error.isBlank()) throw new IllegalArgumentException("Error message cannot be blank");
    }
}

/**
 * Represents an item that was skipped during processing. Immutable record.
 * @param source Path of the ignored item.
 * @param reason Reason for ignoring.
 */
record IgnoredEntry(String source, String reason) {
    public IgnoredEntry { // Compact constructor validation
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(reason, "Reason cannot be null");
        if (source.isBlank()) throw new IllegalArgumentException("Source cannot be blank");
        if (reason.isBlank()) throw new IllegalArgumentException("Reason cannot be blank");
    }
}

/**
 * Bundles the results of an extraction run (successes, errors, ignored items).
 * This class is immutable after creation.
 */
public final class ProcessingReport {

    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    /**
     * Constructor for ProcessingReport. Creates defensive copies of the lists.
     * @param results List of successful CkanResource objects.
     * @param errors List of ProcessingError objects.
     * @param ignored List of IgnoredEntry objects.
     */
    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Ensure lists are not null
        Objects.requireNonNull(results, "Results list cannot be null");
        Objects.requireNonNull(errors, "Errors list cannot be null");
        Objects.requireNonNull(ignored, "Ignored list cannot be null");

        // Create unmodifiable copies to ensure immutability
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }

    /** Returns the unmodifiable list of successful results. */
    public List<CkanResource> getResults() {
        return results;
    }

    /** Returns the unmodifiable list of errors. */
    public List<ProcessingError> getErrors() {
        return errors;
    }

    /** Returns the unmodifiable list of ignored items. */
    public List<IgnoredEntry> getIgnored() {
        return ignored;
    }

    /** Provides a summary string representation. */
    @Override
    public String toString() {
        return "ProcessingReport{" +
                "results=" + results.size() +
                ", errors=" + errors.size() +
                ", ignored=" + ignored.size() +
                '}';
    }
}
