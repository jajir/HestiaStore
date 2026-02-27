package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkPayloadTest {

    @Test
    void ofSequence_requires_non_null_bytes() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkPayload.ofSequence(null));

        assertEquals("Property 'bytes' must not be null.", e.getMessage());
    }

    @Test
    void sequence_copy_isolated_from_payload() {
        final ChunkPayload payload = ChunkPayload
                .ofSequence(ByteSequences.wrap(new byte[] { 1, 2, 3 }));

        final byte[] copy = payload.getBytesSequence().toByteArrayCopy();
        copy[0] = 9;

        assertArrayEquals(new byte[] { 1, 2, 3 },
                payload.getBytesSequence().toByteArrayCopy());
    }

    @Test
    void of_sequence_and_crc_and_equals() {
        final ByteSequence sequence = ByteSequences.wrap(new byte[] { 4, 5, 6 });
        final ChunkPayload p1 = ChunkPayload.ofSequence(sequence);
        final ChunkPayload p2 = ChunkPayload
                .ofSequence(ByteSequences.wrap(new byte[] { 4, 5, 6 }));

        assertEquals(3, p1.length());
        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        assertEquals(p2.calculateCrc(), p1.calculateCrc());
    }

}
