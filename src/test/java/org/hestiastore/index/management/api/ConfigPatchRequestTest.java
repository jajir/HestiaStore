package org.hestiastore.index.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ConfigPatchRequestTest {

    @Test
    void createsDefensiveCopy() {
        final Map<String, String> values = new HashMap<>();
        values.put("indexBusyTimeoutMillis", "2500");

        final ConfigPatchRequest request = new ConfigPatchRequest(values, true);
        values.put("indexBusyTimeoutMillis", "777");

        assertEquals("2500", request.values().get("indexBusyTimeoutMillis"));
    }

    @Test
    void rejectsEmptyValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConfigPatchRequest(Map.of(), false));
    }
}
