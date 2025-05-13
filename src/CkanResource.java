import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

// Represents an immutable CKAN resource; defensively copies input data.
public final class CkanResource {
    private final Map<String, Object> data;

    // Constructs CkanResource. Defensively copies initialData.
    // If "extras" entry is a Map, it's also deeply copied and made unmodifiable.
    public CkanResource(Map<String, Object> initialData) {
        Objects.requireNonNull(initialData, "Data map mag niet null zijn");

        Map<String, Object> workingCopy = new HashMap<>(initialData); // Initial shallow copy

        Object extrasValue = workingCopy.get("extras");
        if (extrasValue instanceof Map) {
            // Assumes 'extrasValue' is Map<String, String>.
            // A ClassCastException will occur if this type assumption is violated.
            @SuppressWarnings("unchecked")
            Map<String, String> originalExtras = (Map<String, String>) extrasValue;
            // Deep copy 'extras' and make it unmodifiable
            workingCopy.put("extras", Collections.unmodifiableMap(new HashMap<>(originalExtras)));
        }

        this.data = Collections.unmodifiableMap(workingCopy);
    }

    // Returns the unmodifiable internal data map.
    public Map<String, Object> getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return "CkanResource{" + "data=" + data + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CkanResource that = (CkanResource) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}