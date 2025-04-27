import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

record ProcessingError(String source, String error) {
    public ProcessingError {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(error, "Foutmelding mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (error.isBlank()) throw new IllegalArgumentException("Foutmelding mag niet leeg zijn");
    }
}

record IgnoredEntry(String source, String reason) {
    public IgnoredEntry {
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(reason, "Reden mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (reason.isBlank()) throw new IllegalArgumentException("Reden mag niet leeg zijn");
    }
}

public final class ProcessingReport {
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Objects.requireNonNull(results, "Lijst met resultaten mag niet null zijn");
        Objects.requireNonNull(errors, "Lijst met fouten mag niet null zijn");
        Objects.requireNonNull(ignored, "Lijst met genegeerde items mag niet null zijn");

        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }

    public List<CkanResource> getResults() {
        return results;
    }

    public List<ProcessingError> getErrors() {
        return errors;
    }

    public List<IgnoredEntry> getIgnored() {
        return ignored;
    }

    @Override
    public String toString() {
        return "ProcessingReport{" +
                "resultaten=" + results.size() +
                ", fouten=" + errors.size() +
                ", genegeerd=" + ignored.size() +
                '}';
    }
}