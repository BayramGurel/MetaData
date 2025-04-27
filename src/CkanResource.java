import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class CkanResource {
    private final Map<String, Object> data;

    public CkanResource(Map<String, Object> data) {
        Objects.requireNonNull(data, "Data map mag niet null zijn");

        Map<String, Object> defensiveCopy = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {

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