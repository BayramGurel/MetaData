import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Onveranderlijke representatie van CKAN resource metadata.
 */
public final class CkanResource {

    /** Onveranderlijke map met resource data. */
    private final Map<String, Object> data;

    /**
     * Constructor. Maakt een defensieve, onveranderlijke kopie.
     * @param data Input map met resource data (vereist).
     */
    public CkanResource(Map<String, Object> data) {
        Objects.requireNonNull(data, "Data map mag niet null zijn");

        Map<String, Object> defensiveCopy = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            // Behandel 'extras' map apart voor onveranderlijkheid
            if ("extras".equals(entry.getKey()) && entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, String> originalExtras = (Map<String, String>) entry.getValue();
                defensiveCopy.put("extras", Collections.unmodifiableMap(new HashMap<>(originalExtras)));
            } else {
                defensiveCopy.put(entry.getKey(), entry.getValue());
            }
        }
        this.data = Collections.unmodifiableMap(defensiveCopy);
    }

    /** Geeft de onveranderlijke data map terug. */
    public Map<String, Object> getData() {
        return this.data;
    }

    /** toString voor debuggen. */
    @Override
    public String toString() {
        return "CkanResource{" + "data=" + data + '}';
    }

    /** equals gebaseerd op data map inhoud. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CkanResource that = (CkanResource) o;
        return Objects.equals(data, that.data);
    }

    /** hashCode gebaseerd op data map inhoud. */
    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
