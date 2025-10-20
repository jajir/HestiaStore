package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PropertyWriterImplTest {

    @Test
    void setters_store_values_and_return_same_writer() {
        final Map<String, String> workingCopy = new HashMap<>();
        final PropertyWriter writer = new PropertyWriterImpl(workingCopy);

        final PropertyWriter returned = writer.setString("alpha", "value")
                .setInt("beta", 7).setLong("gamma", 42L)
                .setDouble("delta", 3.14).setBoolean("epsilon", true);

        assertSame(writer, returned);
        assertEquals("value", workingCopy.get("alpha"));
        assertEquals("7", workingCopy.get("beta"));
        assertEquals("42", workingCopy.get("gamma"));
        assertEquals("3.14", workingCopy.get("delta"));
        assertEquals("true", workingCopy.get("epsilon"));
    }
}
