import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable wrapper for CKAN resource data.
 */
public final class CkanResource {
    private final Map<String, Object> data;

    public CkanResource(Map<String, Object> initialData) {
        Objects.requireNonNull(initialData, "Data map mag niet null zijn");
        Map<String, Object> workingCopy = new HashMap(initialData);
        Object extrasValue = workingCopy.get("extras");
        if (extrasValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, String> originalExtras = (Map<String, String>) extrasValue;
            workingCopy.put("extras", Collections.unmodifiableMap(new HashMap(originalExtras)));
        }

        this.data = Collections.unmodifiableMap(workingCopy);
    }

    public Map<String, Object> getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return "CkanResource{data=" + data + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CkanResource that = (CkanResource) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
