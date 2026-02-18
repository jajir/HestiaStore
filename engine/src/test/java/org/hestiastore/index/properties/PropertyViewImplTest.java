package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PropertyViewImplTest {

    @Test
    void typed_accessors_delegate_to_converters() {
        final Map<String, String> data = new HashMap<>();
        data.put("string", "value");
        data.put("int", "7");
        data.put("long", "42");
        data.put("double", "3.14");
        data.put("booleanTrue", "true");
        data.put("booleanFalse", "false");

        final PropertyView view = new PropertyViewImpl(data,
                new PropertyConverters());

        assertEquals("value", view.getString("string"));
        assertEquals(7, view.getInt("int"));
        assertEquals(42L, view.getLong("long"));
        assertEquals(3.14, view.getDouble("double"));
        assertEquals(true, view.getBoolean("booleanTrue"));
        assertEquals(false, view.getBoolean("booleanFalse"));
        assertNull(view.getString("missing"));
        assertEquals("fallback", view.getStringOrDefault("missing", "fallback"));
    }
}
