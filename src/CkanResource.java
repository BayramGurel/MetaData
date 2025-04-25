import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents metadata from a single source, formatted for CKAN compatibility.
 * This class is immutable after creation.
 */
public final class CkanResource {

    /**
     * Core resource data (CKAN fields) stored as an unmodifiable map.
     */
    private final Map<String, Object> data;

    /**
     * Constructor for CkanResource.
     * Creates a defensive copy of the input map to ensure immutability.
     * Handles nested 'extras' map specifically for immutability.
     * @param data A map containing the CKAN resource fields. Must not be null.
     */
    public CkanResource(Map<String, Object> data) {
        Objects.requireNonNull(data, "Data map cannot be null for CkanResource");

        // Create a deep, immutable copy
        Map<String, Object> defensiveCopy = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            // Ensure the 'extras' map is also immutable if present
            if ("extras".equals(entry.getKey()) && entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") // Checked with instanceof
                Map<String, String> originalExtras = (Map<String, String>) entry.getValue();
                // Copy and make unmodifiable
                defensiveCopy.put("extras", Collections.unmodifiableMap(new HashMap<>(originalExtras)));
            } else {
                // Copy other values (assuming they are immutable or standard types)
                defensiveCopy.put(entry.getKey(), entry.getValue());
            }
        }
        this.data = Collections.unmodifiableMap(defensiveCopy);
    }

    /**
     * Returns the internal (unmodifiable) data map.
     * @return An unmodifiable Map containing the resource data.
     */
    public Map<String, Object> getData() {
        return this.data; // The internal map is already unmodifiable
    }

    /** Standard toString implementation for debugging. */
    @Override
    public String toString() {
        return "CkanResource{" + "data=" + data + '}';
    }

    /** Standard equals implementation based on the data map content. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CkanResource that = (CkanResource) o;
        return Objects.equals(data, that.data); // Compare internal data maps
    }

    /** Standard hashCode implementation based on the data map content. */
    @Override
    public int hashCode() {
        return Objects.hash(data); // Hash based on internal data map
    }
}
