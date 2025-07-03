import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable wrapper for CKAN resource data.
 */
public final class CkanResource {
    private final Map<String, Object> data;

    /**
     * @param initialData the initial key/value map for this resource; must not be null.
     *                    If it contains an "extras" entry whose value is a Map,
     *                    that map will also be made unmodifiable.
     * @throws NullPointerException if initialData is null
     */
    public CkanResource(Map<String, Object> initialData) {
        Objects.requireNonNull(initialData, "Initial data map must not be null");
        // make a defensive copy of the top-level map
        Map<String, Object> workingCopy = new HashMap<>(initialData);

        // if there’s an "extras" entry and it’s itself a map, make it unmodifiable too
        Object extrasValue = workingCopy.get("extras");
        if (extrasValue instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, String> originalExtras = (Map<String, String>) extrasValue;
            Map<String, String> extrasCopy = new HashMap<>(originalExtras);
            workingCopy.put("extras", Collections.unmodifiableMap(extrasCopy));
        }

        // expose only an unmodifiable view
        this.data = Collections.unmodifiableMap(workingCopy);
    }

    /**
     * @return an unmodifiable view of the underlying data map
     */
    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "CkanResource{data=" + data + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CkanResource)) return false;
        CkanResource that = (CkanResource) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
