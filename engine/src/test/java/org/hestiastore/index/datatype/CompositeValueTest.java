package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CompositeValueTest {

    @Test
    void constructor_makesDefensiveCopyOfProvidedElements() {
        final Object[] elements = new Object[] { "A", 1 };

        final CompositeValue value = new CompositeValue(elements);
        elements[0] = "B";

        assertEquals("A", value.get(0));
    }

    @Test
    void getElements_returnsDefensiveCopy() {
        final CompositeValue value = CompositeValue.of("A", 1);

        final Object[] elements = value.getElements();
        elements[0] = "B";

        assertEquals("A", value.get(0));
        assertEquals("A", value.getElements()[0]);
    }

    @Test
    void constructor_rejectsNullElementsArray() {
        assertThrows(IllegalArgumentException.class,
                () -> new CompositeValue((Object[]) null));
    }
}
