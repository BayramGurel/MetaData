import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

// --- Rapportage DTOs ---

/**
 * Representeert een fout tijdens verwerking. Onveranderlijk record.
 * @param source Pad waar de fout optrad.
 * @param error Foutbeschrijving.
 */
record ProcessingError(String source, String error) {
    public ProcessingError { // Compacte constructor validatie
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(error, "Foutmelding mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (error.isBlank()) throw new IllegalArgumentException("Foutmelding mag niet leeg zijn");
    }
}

/**
 * Representeert een overgeslagen item. Onveranderlijk record.
 * @param source Pad van genegeerd item.
 * @param reason Reden voor negeren.
 */
record IgnoredEntry(String source, String reason) {
    public IgnoredEntry { // Compacte constructor validatie
        Objects.requireNonNull(source, "Bron mag niet null zijn");
        Objects.requireNonNull(reason, "Reden mag niet null zijn");
        if (source.isBlank()) throw new IllegalArgumentException("Bron mag niet leeg zijn");
        if (reason.isBlank()) throw new IllegalArgumentException("Reden mag niet leeg zijn");
    }
}

/**
 * Bundelt de resultaten van een extractie-run.
 * Onveranderlijk na creatie.
 */
public final class ProcessingReport {

    private final List<CkanResource> results;
    private final List<ProcessingError> errors;
    private final List<IgnoredEntry> ignored;

    /**
     * Constructor. Maakt defensieve kopieën van de lijsten.
     * @param results Lijst van succesvolle CkanResource objecten.
     * @param errors Lijst van ProcessingError objecten.
     * @param ignored Lijst van IgnoredEntry objecten.
     */
    public ProcessingReport(List<CkanResource> results, List<ProcessingError> errors, List<IgnoredEntry> ignored) {
        // Zorg dat lijsten niet null zijn
        Objects.requireNonNull(results, "Lijst met resultaten mag niet null zijn");
        Objects.requireNonNull(errors, "Lijst met fouten mag niet null zijn");
        Objects.requireNonNull(ignored, "Lijst met genegeerde items mag niet null zijn");

        // Maak onveranderlijke kopieën
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
        this.ignored = Collections.unmodifiableList(new ArrayList<>(ignored));
    }

    /** Geeft de onveranderlijke lijst van succesvolle resultaten. */
    public List<CkanResource> getResults() {
        return results;
    }

    /** Geeft de onveranderlijke lijst van fouten. */
    public List<ProcessingError> getErrors() {
        return errors;
    }

    /** Geeft de onveranderlijke lijst van genegeerde items. */
    public List<IgnoredEntry> getIgnored() {
        return ignored;
    }

    /** Geeft een samenvattende string representatie. */
    @Override
    public String toString() {
        return "ProcessingReport{" +
                "resultaten=" + results.size() +
                ", fouten=" + errors.size() +
                ", genegeerd=" + ignored.size() +
                '}';
    }
}
