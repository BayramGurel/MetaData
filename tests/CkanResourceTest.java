import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the CkanResource class.
 */
class CkanResourceTest {

    private Map<String, Object> originalData;
    private Map<String, String> originalExtras;

    @BeforeEach
    void setUp() {
        // Set up initial mutable data for testing defensive copies
        originalData = new HashMap<>();
        originalData.put("name", "Test Resource");
        originalData.put("format", "TXT");

        originalExtras = new HashMap<>();
        originalExtras.put("key1", "value1");
        originalExtras.put("key2", "value2");
        originalData.put("extras", originalExtras);
    }

    @Test
    @DisplayName("Constructor should throw NullPointerException for null data")
    void constructor_NullData_ThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> {
            new CkanResource(null);
        }, "Constructor should throw NullPointerException when data map is null");
    }

    @Test
    @DisplayName("Constructor should create defensive copy of data map")
    void constructor_CreatesDefensiveCopy_DataMap() {
        CkanResource resource = new CkanResource(originalData);
        Map<String, Object> retrievedData = resource.getData();

        // Modify original map AFTER creating the resource
        originalData.put("name", "Modified Name");
        originalData.put("new_field", 123);

        // Check that the resource's data is unchanged
        assertEquals("Test Resource", retrievedData.get("name"), "Resource name should not change after modifying original map");
        assertNull(retrievedData.get("new_field"), "New field added to original map should not appear in resource's data");
        assertEquals(3, retrievedData.size(), "Resource data map size should remain unchanged"); // name, format, extras
    }

    @Test
    @DisplayName("Constructor should create defensive copy of nested 'extras' map")
    void constructor_CreatesDefensiveCopy_ExtrasMap() {
        CkanResource resource = new CkanResource(originalData);
        @SuppressWarnings("unchecked")
        Map<String, String> retrievedExtras = (Map<String, String>) resource.getData().get("extras");

        assertNotNull(retrievedExtras, "'extras' map should exist in resource data");

        // Modify original extras map AFTER creating the resource
        originalExtras.put("key1", "modifiedValue1");
        originalExtras.put("newKey", "newValue");

        // Check that the resource's extras map is unchanged
        assertEquals("value1", retrievedExtras.get("key1"), "Extras value should not change after modifying original extras map");
        assertNull(retrievedExtras.get("newKey"), "New key added to original extras map should not appear in resource's extras");
        assertEquals(2, retrievedExtras.size(), "Resource extras map size should remain unchanged"); // key1, key2
    }

    @Test
    @DisplayName("getData should return an unmodifiable map")
    void getData_ReturnsUnmodifiableMap() {
        CkanResource resource = new CkanResource(originalData);
        Map<String, Object> retrievedData = resource.getData();

        // Attempt to modify the returned map
        assertThrows(UnsupportedOperationException.class, () -> {
            retrievedData.put("another_field", "should fail");
        }, "Attempting to modify the map returned by getData() should throw UnsupportedOperationException");
    }

    @Test
    @DisplayName("getData should return an unmodifiable 'extras' map")
    void getData_ReturnsUnmodifiableExtrasMap() {
        CkanResource resource = new CkanResource(originalData);
        @SuppressWarnings("unchecked")
        Map<String, String> retrievedExtras = (Map<String, String>) resource.getData().get("extras");

        assertNotNull(retrievedExtras, "'extras' map should exist");

        // Attempt to modify the returned extras map
        assertThrows(UnsupportedOperationException.class, () -> {
            retrievedExtras.put("another_extra", "should fail");
        }, "Attempting to modify the 'extras' map returned within getData() should throw UnsupportedOperationException");
    }

    @Test
    @DisplayName("equals should return true for identical data content")
    void equals_IdenticalData_ReturnsTrue() {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("name", "Resource A");
        data1.put("format", "PDF");
        Map<String, String> extras1 = new HashMap<>();
        extras1.put("lang", "nl");
        data1.put("extras", extras1);
        CkanResource resource1 = new CkanResource(data1);

        // Create a separate but identical map
        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Resource A");
        data2.put("format", "PDF");
        Map<String, String> extras2 = new HashMap<>();
        extras2.put("lang", "nl");
        data2.put("extras", extras2);
        CkanResource resource2 = new CkanResource(data2);

        assertEquals(resource1, resource2, "equals should return true for resources with identical data");
        assertEquals(resource1.hashCode(), resource2.hashCode(), "hashCode should be the same for equal resources");
    }

    @Test
    @DisplayName("equals should return false for different data content")
    void equals_DifferentData_ReturnsFalse() {
        CkanResource resource1 = new CkanResource(originalData); // Uses data from setUp

        // Create resource with different data
        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Different Resource"); // Different name
        data2.put("format", "TXT");
        Map<String, String> extras2 = new HashMap<>();
        extras2.put("key1", "value1");
        extras2.put("key2", "value2");
        data2.put("extras", extras2);
        CkanResource resource2 = new CkanResource(data2);

        assertNotEquals(resource1, resource2, "equals should return false for resources with different data");
    }

    @Test
    @DisplayName("equals should return false for different 'extras' content")
    void equals_DifferentExtras_ReturnsFalse() {
        CkanResource resource1 = new CkanResource(originalData); // Uses data from setUp

        // Create resource with different extras
        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Test Resource");
        data2.put("format", "TXT");
        Map<String, String> extras2 = new HashMap<>();
        extras2.put("key1", "DIFFERENT_value"); // Different value in extras
        extras2.put("key2", "value2");
        data2.put("extras", extras2);
        CkanResource resource2 = new CkanResource(data2);

        assertNotEquals(resource1, resource2, "equals should return false for resources with different extras");
    }

    @Test
    @DisplayName("equals should handle null comparison")
    void equals_NullComparison_ReturnsFalse() {
        CkanResource resource = new CkanResource(originalData);
        assertNotEquals(null, resource, "equals should return false when comparing with null");
    }

    @Test
    @DisplayName("equals should handle comparison with different object type")
    void equals_DifferentType_ReturnsFalse() {
        CkanResource resource = new CkanResource(originalData);
        String otherObject = "I am a string";
        assertNotEquals(resource, otherObject, "equals should return false when comparing with a different type");
    }

    @Test
    @DisplayName("hashCode should be consistent")
    void hashCode_Consistent() {
        CkanResource resource = new CkanResource(originalData);
        int initialHashCode = resource.hashCode();
        // Call multiple times to check consistency
        assertEquals(initialHashCode, resource.hashCode(), "hashCode should be consistent across multiple calls");
        assertEquals(initialHashCode, resource.hashCode(), "hashCode should be consistent across multiple calls");
    }

    @Test
    @DisplayName("Constructor handles map without 'extras'")
    void constructor_NoExtrasMap() {
        Map<String, Object> dataNoExtras = new HashMap<>();
        dataNoExtras.put("name", "No Extras Resource");
        CkanResource resource = new CkanResource(dataNoExtras);
        assertNull(resource.getData().get("extras"), "'extras' should be null if not provided in input");
        assertEquals(1, resource.getData().size());
    }
}
