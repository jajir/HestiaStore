package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.bytes.ByteSequenceView;
import org.junit.jupiter.api.Test;

public class DataBlockPayloadTest {

    private static final ByteSequenceView BYTES_1 = ByteSequenceView
            .of("test data".getBytes());

    @Test
    void test_equals() {
        DataBlockPayload payload1 = DataBlockPayload.of(BYTES_1);
        DataBlockPayload payload2 = DataBlockPayload.of(BYTES_1);

        assertEquals(payload1, payload1);
        assertEquals(payload2, payload2);
        assertEquals(payload1, payload2);

        assertEquals(payload1.hashCode(), payload2.hashCode());
    }

    @Test
    void test_required_Bytes() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> DataBlockPayload.of(null));

        assertEquals("Property 'bytes' must not be null.", e.getMessage());
    }

}
