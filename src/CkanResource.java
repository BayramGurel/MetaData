import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    public String toString() {
        return "CkanResource{data=" + String.valueOf(this.data) + "}";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            CkanResource that = (CkanResource)o;
            return Objects.equals(this.data, that.data);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.data});
    }
}
