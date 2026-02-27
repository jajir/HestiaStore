package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class DataBlockPayloadTest {

    @Test
    void test_equals() {
        DataBlockPayload payload1 = DataBlockPayload
                .ofSequence(ByteSequences.wrap("test data".getBytes()));
        DataBlockPayload payload2 = DataBlockPayload
                .ofSequence(ByteSequences.wrap("test data".getBytes()));

        assertEquals(payload1, payload1);
        assertEquals(payload2, payload2);
        assertEquals(payload1, payload2);

        assertEquals(payload1.hashCode(), payload2.hashCode());
    }

    @Test
    void test_required_Bytes() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> DataBlockPayload.ofSequence(null));

        assertEquals("Property 'bytes' must not be null.", e.getMessage());
    }

    @Test
    void test_of_sequence_and_length_and_copy() {
        final DataBlockPayload payload = DataBlockPayload
                .ofSequence(ByteSequences.wrap(new byte[] { 1, 2, 3 }));

        assertEquals(3, payload.length());
        final byte[] copy = payload.getBytesSequence().toByteArrayCopy();
        copy[0] = 9;
        assertEquals(1, payload.getBytesSequence().getByte(0));
    }

}
