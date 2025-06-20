import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ProcessingReport {
    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        Objects.requireNonNull(results, "Lijst met resultaten mag niet null zijn");
        Objects.requireNonNull(errors, "Lijst met fouten mag niet null zijn");
        Objects.requireNonNull(ignored, "Lijst met genegeerde items mag niet null zijn");
        this.results = Collections.unmodifiableList(new ArrayList(results));
        this.errors = Collections.unmodifiableList(new ArrayList(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList(ignored));
    }

    public List<CkanResource> getResults() {
        return this.results;
    }

    public List<ProcessingError> getErrors() {
        return this.errors;
    }

    public List<IgnoredEntry> getIgnored() {
        return this.ignored;
    }

    public String toString() {
        int var10000 = this.results.size();
        return "ProcessingReport{resultaten=" + var10000 + ", fouten=" + this.errors.size() + ", genegeerd=" + this.ignored.size() + "}";
    }
}
