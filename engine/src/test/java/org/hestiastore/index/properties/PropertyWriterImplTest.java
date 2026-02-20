package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PropertyWriterImplTest {

    @Test
    void setters_store_values_and_return_same_writer() {
        final Map<String, String> workingCopy = new HashMap<>();
        final PropertyWriter writer = new PropertyWriterImpl(workingCopy);

        final PropertyWriter returned = writer.setString("alpha", "value")
                .setInt("beta", 1_234_567).setLong("gamma", 9_876_543_210L)
                .setDouble("delta", 12_345.14).setBoolean("epsilon", true);

        assertSame(writer, returned);
        assertEquals("value", workingCopy.get("alpha"));
        assertEquals("1_234_567", workingCopy.get("beta"));
        assertEquals("9_876_543_210", workingCopy.get("gamma"));
        assertEquals("12_345.14", workingCopy.get("delta"));
        assertEquals("true", workingCopy.get("epsilon"));
    }
}
