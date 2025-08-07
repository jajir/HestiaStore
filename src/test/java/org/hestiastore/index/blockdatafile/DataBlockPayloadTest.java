package org.hestiastore.index.blockdatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

public class DataBlockPayloadTest {

    private static final Bytes BYTES_1 = Bytes.of("test data".getBytes());

    @Test
    void test_equals() {
        DataBlockPayload payload1 = DataBlockPayload.of(BYTES_1);
        DataBlockPayload payload2 = DataBlockPayload.of(BYTES_1);

        assertEquals(payload1, payload1);
        assertEquals(payload2, payload2);
        assertEquals(payload1, payload2);

        assertEquals(payload1.hashCode(), payload2.hashCode());
    }

}
