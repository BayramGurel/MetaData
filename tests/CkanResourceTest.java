import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CkanResourceTest {

    private Map<String, Object> originalData;
    private Map<String, String> originalExtras;

    @BeforeEach
    void setUp() {
        originalData = new HashMap<>();
        originalData.put("name", "Test Resource");
        originalData.put("format", "TXT");

        originalExtras = new HashMap<>();
        originalExtras.put("key1", "value1");
        originalExtras.put("key2", "value2");
        originalData.put("extras", originalExtras);
    }

    @Test
    @DisplayName("Constructor gooit NullPointerException bij null data")
    void constructor_NullData_ThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new CkanResource(null));
    }

    @Test
    @DisplayName("equals retourneert true bij identieke data inhoud")
    void equals_IdenticalData_ReturnsTrue() {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("name", "Resource A");
        data1.put("format", "PDF");
        Map<String, String> extras1 = new HashMap<>();
        extras1.put("lang", "nl");
        data1.put("extras", extras1);
        CkanResource resource1 = new CkanResource(data1);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Resource A");
        data2.put("format", "PDF");
        Map<String, String> extras2 = new HashMap<>();
        extras2.put("lang", "nl");
        data2.put("extras", extras2);
        CkanResource resource2 = new CkanResource(data2);

        assertEquals(resource1, resource2);
        assertEquals(resource1.hashCode(), resource2.hashCode());
    }

    @Test
    @DisplayName("equals retourneert false bij verschillende data inhoud")
    void equals_DifferentData_ReturnsFalse() {
        CkanResource resource1 = new CkanResource(originalData);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Different Resource");
        data2.put("format", "TXT");
        Map<String, String> extras2 = new HashMap<>(originalExtras);
        data2.put("extras", extras2);
        CkanResource resource2 = new CkanResource(data2);

        assertNotEquals(resource1, resource2);
    }

    @Test
    @DisplayName("equals verwerkt vergelijking met ander object type")
    void equals_DifferentType_ReturnsFalse() {
        CkanResource resource = new CkanResource(originalData);
        String otherObject = "I am a string";
        assertNotEquals(resource, otherObject);
    }

    @Test
    @DisplayName("Constructor verwerkt map zonder 'extras'")
    void constructor_NoExtrasMap() {
        Map<String, Object> dataNoExtras = new HashMap<>();
        dataNoExtras.put("name", "No Extras Resource");
        CkanResource resource = new CkanResource(dataNoExtras);
        assertNull(resource.getData().get("extras"));
        assertEquals(1, resource.getData().size());
    }
}